from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring, trim, coalesce, when
from pyspark.sql.window import Window

# --- WORKAROUND: Zero Guard for Decimal Fields ---
# Middleware converts 0.0 (DoubleType) to integer 0, which OIC API rejects as "data type mismatch".
# This replaces exact-zero with a tiny epsilon to preserve decimal type through serialization.
# Set _ZERO_GUARD_ENABLED = False to disable (pass-through as DoubleType only).
_ZERO_GUARD_ENABLED = True
_ZERO_GUARD_EPSILON = 0.001
_MAX_LEN_STR = 150

def _zero_guard(col_expr):
    """Replace zero with epsilon to prevent middleware from serializing as integer."""
    if _ZERO_GUARD_ENABLED:
        return F.when(col_expr == 0.0, F.lit(_ZERO_GUARD_EPSILON)).otherwise(col_expr).cast(T.DoubleType())
    return col_expr.cast(T.DoubleType())

def empty_array_of(elem_type: T.DataType):
    return F.from_json(F.lit('[]'), T.ArrayType(elem_type))

def _string_lenght_guard(col_expr, max_len=_MAX_LEN_STR):
    """
    Trim string and cut to max_len characters.
    """
    return F.substring(
        F.trim(col_expr),
        1,
        max_len
    ).cast(T.StringType())

# --- Helpers ---
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

def _to_iso_tz7_from_yyyymmdd(col):
    c = F.trim(F.col(col)).cast(T.StringType())
    return (
        F.when(c.isNull(), F.lit(None).cast(T.StringType()))
         .when(c == F.lit('99999999'),
               F.lit('9999-01-01T00:00:00+07:00'))
         .when(F.length(c) == 8,
               F.concat_ws("",F.substring(c,1,4), F.lit("-"),
                   F.substring(c,5,2), F.lit("-"),
                   F.substring(c,7,2), F.lit("T00:00:00+07:00"))
          ).otherwise(F.lit(None).cast(T.StringType()))
    )

def _to_iso_tz7_from_yyyy_mm_dd(col):
    s = F.trim(F.col(col)).cast(T.StringType())
    d = F.to_date(s, 'yyyy-MM-dd')
    return (
        F.when(s.isNull(), F.lit(None).cast(T.StringType()))
         .when(s == F.lit('9999-99-99'),
               F.lit('9999-01-01T00:00:00+07:00'))
         .when(d.isNotNull(), F.concat_ws('', F.date_format(d, 'yyyy-MM-dd'), F.lit('T00:00:00+07:00')))
         .otherwise(F.lit(None).cast(T.StringType()))
    )


def grp_std_policy_only_aggregation(run_params, spark_session, start_date, end_date):
    """
    Aggregates Group Standard Policy data for Policy-level submission only.
    Creates a dummy insured record with zero values as required by OIC spec.

    Args:
        run_params: Run parameters from Glue job
        spark_session: Spark session
        start_date: Start date for data filtering (yyyyMMdd format)
        end_date: End date for data filtering (yyyyMMdd format)

    Returns:
        DataFrame: Aggregated policy data ready for JSON output
    """
    # pre mapping
    _billfreq_map = F.create_map(
        *[F.lit(kv) for kv in ("00","05","01","04","02","03","04","02","12","01")]
    )

    # defined mapping
    _stat_base_map = F.create_map(
        F.lit("IF"),F.lit("0100"),
        F.lit("PE"),F.lit("0100"),
        F.lit("PN"),F.lit("9999"),
        F.lit("PR"),F.lit("9999"),
        F.lit("LA"),F.lit("0700"),
        F.lit("DC"),F.lit("0402"),
        F.lit("CA"),F.lit("0400")
    )
    print('Reading Group Policy source tables...')

    # =====================================================
    # SECTION 1: Read Source Tables
    # =====================================================

    # Policy Header - ATLLDTA_CHDRPF
    if run_params.workflow_type == 'Daily':
        df_chdrpf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')\
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date))
            )
        df_chdrpf_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf')\
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date))
            )
    elif run_params.workflow_type == 'One-Time-Inforce':
        df_chdrpf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')\
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Inforce statuses
            )
        df_chdrpf_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf')\
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Inforce statuses
            )
    elif run_params.workflow_type == 'One-Time-Non-Inforce':
        df_chdrpf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')\
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (~F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Non-Inforce statuses
            )
        df_chdrpf_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf')\
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (~F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Non-Inforce statuses
            )
    else:
        df_chdrpf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')\
            .filter(
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)
            )
        df_chdrpf_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf')\
            .filter(
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)
            )

    # Filter for Group Standard Policy types (exclude MRTA/CTL)
    # Standard Group: CHS, CR1, CR2, EB1, GCI, GHP, GHS, GHT, GLP, GLT, GPA, GTL, PAG, PAS, PA1, PA2, PA3, PHS, PH1, PH2
    # Exclude: CR3 (MRTA), CL1 (Circle Loan)
    # 2026/02/09 tarat added :  CL1 follow by mapping
    # Include: CR3 (MRTA), CL1 (Circle Loan) Added on 2026/05/20 : added 'CR3', 'MRT'
    grp_std_cnttype_list = ['CHS','CR1','CR2','EB1','GCI','GHP','GHS','GHT','GLP','GLT',
                            'GPA','GTL','PAG','PAS','PA1','PA2','PA3','PHS','PH1','PH2',
                            'CR4','CR5','CR6','CL1', 'CR3', 'MRT']


    df_chdrpf_source = df_chdrpf_source.filter(F.col('cnttype').isin(grp_std_cnttype_list)).select('chdrnum','cnttype','cownnum','occdate','statcode','datime')
    # 2026/05/07 : Union df_chdrpf_source
    df_chdrpf_ilcl_source = df_chdrpf_ilcl_source.filter(F.col('cnttype').isin(grp_std_cnttype_list)).select('chdrnum','cnttype','cownnum','occdate','statcode','datime')
    df_chdrpf_source = df_chdrpf_source.unionByName(df_chdrpf_ilcl_source).dropDuplicates()

    # Client/Customer - ATLLDTA_CLNTPF
    df_clntpf_source = spark_session.table('processing_ibs.ibs_atlldta_clntpf').select('clntnum','clttype','lsurname','cltaddr01','cltaddr02','cltaddr03','cltaddr04','cltaddr05','cltpcode','validflag')

    # Group Policy Info - ATLLDTA_GCHIPF
    df_gchipf_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf').select('chdrnum','ccdate','crdate','btdatenr','billfreq','nofmbr','agntnum')
    # 2026/05/11 : union df_gchipf_ilcl_source
    df_gchipf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf_ilcl').select('chdrnum','ccdate','crdate','btdatenr','billfreq','nofmbr','agntnum')
    df_gchipf_source = df_gchipf_source.unionByName(df_gchipf_ilcl_source).dropDuplicates()

    # New - Group Policy
    df_bnfhpf_source = spark_session.table('processing_ibs.ibs_atlldta_bnfhpf').select('chdrnum','prodtyp','planno','bencde')

    df_gmhdpf = spark_session.table('processing_ibs.ibs_atlldta_gmhdpf')
    # new 2026/04/29 union table *_ilcl 
    df_gmhdpf_ilcl = spark_session.table('processing_ibs.ibs_atlldta_gmhdpf_ilcl')
    base_cols = df_gmhdpf.columns
    
    df_ilcl_aligned = df_gmhdpf_ilcl.select(*[
        c if c in df_gmhdpf_ilcl.columns else lit(None).alias(c)
        for c in base_cols
    ])

    df_gmhdpf = df_gmhdpf.unionByName(df_ilcl_aligned).dropDuplicates()

    # Premium/Billing - ATLLDTA_GBIHPF
    df_gbihpf_source = spark_session.table('processing_ibs.ibs_atlldta_gbihpf').select('chdrnum','datime')

    # Coverage source tables
    # new 2026/04/29 union table *_ilcl 
    df_gxhipf_source = spark_session.table('processing_ibs.ibs_atlldta_gxhipf').select('chdrnum','mbrno','dpntno','planno','prodtyp','suminsu')
    df_gxhipf_ilcl= spark_session.table('processing_ibs.ibs_atlldta_gxhipf_ilcl').select('chdrnum','mbrno','dpntno','planno','prodtyp','suminsu')
    df_gxhipf_source = df_gxhipf_source.unionByName(df_gxhipf_ilcl).dropDuplicates()

    df_bnflpf_source = spark_session.table('processing_ibs.ibs_atlldta_bnflpf').select('chdrnum','planno','prodtyp','bencde','amtyear','amtday','amtvis','daysvis','effdate','tranno')
    
    # new join for pol_licence
    df_agplpf_source = spark_session.table('processing_ibs.ibs_atlldta_agplpf').select('agntnum','licence')

    # =====================================================
    # SECTION 2: Reference Tables
    # =====================================================

    print('Reading ref tables...')
    # Reference for I-SERFF mapping
    df_ref_group_pol_sub_type_lserff = spark_session.table('processing_ibs.ibs_ref_group_pol_sub_type_lserff').select('plan_code','lserff_code')

    df_ref_group_product_type_lserff = spark_session.table('processing_ibs.ibs_ref_group_product_type_lserff')

    # Reference for service_md_ref
    df_ref_policy_service_md = spark_session.table('processing_ibs.ibs_ref_policy_service_md_ref')

    # Reference for ibs_ref_location_code
    df_ref_location_code = spark_session.table('processing_ibs.ibs_ref_location_code').select('province_th',substring('code', 1, 2).alias('code')).distinct()

    df_ref_g400_product_benefit_code = spark_session.table('processing_ibs.ibs_ref_g400_product_benefit_code').select('prodtyp', 'bencde', 'longdesc1', 'longdesc2', 'oic_code')

    df_ref_group_policy_type_lserff = spark_session.table('processing_ibs.ibs_ref_group_policy_type_lserff')

    # =====================================================
    # SECTION 3: Default Values
    # =====================================================

    print('Declaring Default Params Value...')
    pol_refer_code_of_company_prefix = "POL_KAL_"
    # pol_transaction_status = "N"  # New
    pol_transaction_status = ""  # sent as null string ""
    pol_company_id = "1022"
    cust_country_code = "THA"

    # Required field null values
    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00

    # Non-required field null values
    NON_REQ_NULL_STR = None
    NON_REQ_NULL_INT = None
    NON_REQ_NULL_DEC = None

    # =====================================================
    # SECTION 4: Policy Type Mapping
    # =====================================================

    # Map cnttype to OIC pol_type
    # PT-03: Group Life Insurance
    # PT-10: Group PA (Personal Accident)
    policy_type_mapping = {
        'CHS': 'PT-03', 'CL1': 'PT-03', 'CR1': 'PT-03', 'CR2': 'PT-03',
        'CR3': 'PT-03', 'CR4': 'PT-03', 'CR5': 'PT-03', 'CR6': 'PT-03',
        'EB1': 'PT-03', 'GCI': 'PT-03', 'GHP': 'PT-03', 'GHS': 'PT-03',
        'GHT': 'PT-03', 'GLP': 'PT-03', 'GLT': 'PT-03', 'GTL': 'PT-03',
        'MRT': 'PT-03', 'PHS': 'PT-03', 'PH1': 'PT-03', 'PH2': 'PT-03',
        'GPA': 'PT-10', 'PAG': 'PT-10', 'PAS': 'PT-10',
        'PA1': 'PT-10', 'PA2': 'PT-10', 'PA3': 'PT-10'
    }


    # =====================================================
    # SECTION 5: Join Tables and Build Policy DataFrame
    # =====================================================

    print('Joining Policy tables...')

    # 1) policy + customer
    df_policy_with_owner = df_chdrpf_source.alias('pol') \
        .join(
            df_clntpf_source.alias('cust'),
            F.trim(F.col('pol.cownnum')) == F.trim(F.col('cust.clntnum')),
            'left'
        ) \
        .join(
            spark_session.table('processing_ibs.ibs_atlldta_clexpf').select('clntnum','zwhidnum').alias('cid'),
            F.trim(F.col('pol.cownnum')) == F.trim(F.col('cid.clntnum')),
            'inner'
            # 'left'
        )

    # 2) policy + policy info Latest GCHIPF: max(CCDATE)
    window_gchi = Window.partitionBy('chdrnum').orderBy(F.col('ccdate').desc())
    df_gchipf_latest = df_gchipf_source \
        .withColumn('rn', F.row_number().over(window_gchi)) \
        .filter(F.col('rn') == 1) \
        .select('chdrnum','ccdate','crdate','btdatenr', 'billfreq','nofmbr','agntnum')
    
    df_policy_with_gchi = df_policy_with_owner \
        .join(
            df_gchipf_latest.alias('gchi'),
            F.trim(F.col('pol.chdrnum')) == F.trim(F.col('gchi.chdrnum')),
            'left'
        )

    # Latest GBIHPF: max datime
    window_gbih = Window.partitionBy('chdrnum').orderBy(F.col('datime').desc())
    df_gbihpf_latest = df_gbihpf_source \
        .withColumn('row_num', F.row_number().over(window_gbih)) \
        .filter(F.col('row_num') == 1) \
        .drop('row_num')

    df_policy_base = df_policy_with_gchi \
        .join(
            df_gbihpf_latest.alias('gbih'),
            F.trim(F.col('pol.chdrnum')) == F.trim(F.col('gbih.chdrnum')),
            'left'
        )

    # new - Join gchi.nofmbr and max(planno) for atlldta_bnfhpf
    df_planno_max = df_bnfhpf_source.groupBy('chdrnum').agg(
        F.max(F.col('planno').cast('int')).alias('max_planno')
    )

    # 2026/04/23 new - join df_agplpf_source for pol_licese
    df_policy_base = df_policy_base.join(df_agplpf_source.alias('agp'),
                                     F.trim(F.col('gchi.agntnum')) == F.trim(F.col('agp.agntnum')),
                                     'left') 

    df_policy_base = df_policy_base.join(df_planno_max.alias('pln'),
                                         F.trim(F.col('pol.chdrnum')) == F.trim(F.col('pln.chdrnum')),
                                         'left')

    # add for prodtype
    df_policy_base = df_policy_base.join(df_bnfhpf_source.select('chdrnum','prodtyp','planno').distinct().alias('bnf'),
                                         (F.trim(F.col('pol.chdrnum')) == F.trim(F.col('bnf.chdrnum'))) & (F.col('pln.max_planno') == F.col('bnf.planno')),
                                         'left')
    
    # new - join ref (df_ref_group_pol_sub_type_lserff)
    df_policy_base = df_policy_base.join(
        df_ref_group_pol_sub_type_lserff.alias('ref'),
        F.trim(F.col('bnf.prodtyp')) == F.trim(F.col('ref.plan_code')),
        'left'
    )

    # 2026/04/22 : new - add join new logic
    df_policy_base = df_policy_base.join(
        df_ref_group_policy_type_lserff.alias('ref_poltype'),
        F.trim(F.col('pol.cnttype')) == F.trim(F.col('ref_poltype.cnttype')),
        'left'
    )

    # new - join ref df_ref_group_product_type_lserff
    df_policy_base = df_policy_base.join(
        df_ref_group_product_type_lserff.alias('ref_prodtype'),
        F.trim(F.col('bnf.prodtyp')) == F.trim(F.col('ref_prodtype.descitem')),
        'left'
    )

    # new - year for gchipf
    df_pol_year = df_gchipf_source.groupBy('chdrnum').agg(F.count('ccdate').alias('pol_year_cnt'))
    df_policy_base = df_policy_base.join(df_pol_year.alias('pyr'),
                                         F.trim(F.col('pol.chdrnum')) == F.trim(F.col('pyr.chdrnum')),
                                         'left')

    df_policy_base = df_policy_base.join(
        df_ref_location_code.alias('ref_location'),
        F.trim(
            F.when(
                (F.col('cust.cltaddr05').isNull()) | (F.col('cust.cltaddr05') == ''),
                F.col('cust.cltaddr04')
            ).otherwise(F.col('cust.cltaddr05'))
        ) == F.trim(F.col('ref_location.province_th')),
        'left'
    )

    # =====================================================
    # SECTION 6: Build Output Policy DataFrame
    # =====================================================

    print('Building Output Policy DataFrame...')

    # Create mapping expression for pol_type
    pol_type_expr = F.when(F.col('pol.cnttype') == 'CHS', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CL1', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CR1', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CR2', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CR3', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CR4', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CR5', 'PT-03')\
        .when(F.col('pol.cnttype') == 'CR6', 'PT-03')\
        .when(F.col('pol.cnttype') == 'EB1', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GCI', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GHP', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GHS', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GHT', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GLP', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GLT', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GTL', 'PT-03')\
        .when(F.col('pol.cnttype') == 'MRT', 'PT-03')\
        .when(F.col('pol.cnttype') == 'PHS', 'PT-03')\
        .when(F.col('pol.cnttype') == 'PH1', 'PT-03')\
        .when(F.col('pol.cnttype') == 'PH2', 'PT-03')\
        .when(F.col('pol.cnttype') == 'GPA', 'PT-10')\
        .when(F.col('pol.cnttype') == 'PAG', 'PT-10')\
        .when(F.col('pol.cnttype') == 'PAS', 'PT-10')\
        .when(F.col('pol.cnttype') == 'PA1', 'PT-10')\
        .when(F.col('pol.cnttype') == 'PA2', 'PT-10')\
        .when(F.col('pol.cnttype') == 'PA3', 'PT-10')\
        .otherwise('PT-03')

    # Create mapping expression for pol_type_desc
    pol_type_desc_expr = F.when(F.col('pol.cnttype') == 'CHS', 'Complete Health Solution')\
        .when(F.col('pol.cnttype') == 'CL1', 'Circle Loan')\
        .when(F.col('pol.cnttype') == 'CR1', 'Credit Life (Normal)')\
        .when(F.col('pol.cnttype') == 'CR2', 'Credit Life (Open-End)')\
        .when(F.col('pol.cnttype') == 'CR3', 'Credit Life (MRTA)')\
        .when(F.col('pol.cnttype') == 'CR4', 'Credit Life (GLTSP)')\
        .when(F.col('pol.cnttype') == 'CR5', 'Credit Life (Special 1)')\
        .when(F.col('pol.cnttype') == 'CR6', 'Credit Life (Special 2)')\
        .when(F.col('pol.cnttype') == 'EB1', 'Employee Benefit')\
        .when(F.col('pol.cnttype') == 'GCI', 'Group Critical Illness')\
        .when(F.col('pol.cnttype') == 'GHP', 'Group Health Package')\
        .when(F.col('pol.cnttype') == 'GHS', 'Group Complete Health Solution')\
        .when(F.col('pol.cnttype') == 'GHT', 'Group Health Tailor Made')\
        .when(F.col('pol.cnttype') == 'GLP', 'Group Leasing Policy')\
        .when(F.col('pol.cnttype') == 'GLT', 'Group Level Term Single Prem')\
        .when(F.col('pol.cnttype') == 'GTL', 'Group Term Life')\
        .when(F.col('pol.cnttype') == 'MRT', 'Mortage Reducing term assuranc')\
        .when(F.col('pol.cnttype') == 'PHS', 'Group Perfect Health Solution')\
        .when(F.col('pol.cnttype') == 'PH1', 'Group Health Package 1 (Old)')\
        .when(F.col('pol.cnttype') == 'PH2', 'Group Health Package 2 (New)')\
        .when(F.col('pol.cnttype') == 'GPA', 'Group Personal Accident')\
        .when(F.col('pol.cnttype') == 'PAG', 'Group PA (General)')\
        .when(F.col('pol.cnttype') == 'PAS', 'PA Student Package')\
        .when(F.col('pol.cnttype') == 'PA1', 'Group PA Tailor Made')\
        .when(F.col('pol.cnttype') == 'PA2', 'Group PA Package 2')\
        .when(F.col('pol.cnttype') == 'PA3', 'PA3-PA365(KTB)')\
        .otherwise('')


    # new - Policy Status Mapping 
    pol_status_base = F.create_map(
        # F.lit("IF"), F.lit("0100"),
        F.lit("PE"), F.lit("0100"),
        F.lit("PN"), F.lit("9999"),
        F.lit("PR"), F.lit("9999"),
        F.lit("LA"), F.lit("0700"),
        F.lit("DC"), F.lit("0402"),
        F.lit("CA"), F.lit("0400")
    )

    w = Window.partitionBy("pol.chdrnum")

    output_policy_df = df_policy_base\
        .withColumn('pol_refer_code_of_company',
                        F.concat_ws('',
                            F.lit(pol_refer_code_of_company_prefix),
                            F.trim(F.col('pol.chdrnum')),
                            F.lit('_'),
                            F.date_format(F.to_date(F.col('gchi.ccdate').cast('string'),'yyyyMMdd'),'yyyy')
                        ).cast(T.StringType())
                    )\
        .withColumn('pol_transaction_status', lit(pol_transaction_status).cast(T.StringType()))\
        .withColumn('pol_company_id', lit(pol_company_id).cast(T.StringType()))\
        .withColumn('pol_id', F.trim(F.col('pol.chdrnum')).cast(T.StringType()))\
        .withColumn('pol_type', pol_type_expr.cast(T.StringType()))\
        .withColumn('pol_type_gli_name', F.col('gchi.nofmbr').cast(T.IntegerType())) \
        .withColumn('pol_type_gli_plan', F.col('pln.max_planno').cast(T.IntegerType()))\
        .withColumn('pol_product_type', F.col('ref_poltype.dt_ppt_code'))\
        .withColumn('pol_product_type_other_detail', F.when(F.col('pol_product_type')=='PPT-20', _string_lenght_guard(F.col('ref_prodtype.longdesc'),max_len=49)).otherwise(F.lit(REQ_NULL_STR)).cast(T.StringType())) \
        .withColumn('pol_type_2', lit('NP').cast(T.StringType()))\
        .withColumn('pol_sub_type', F.coalesce(F.col('ref_poltype.lserff_code'), F.lit(REQ_NULL_STR)).cast(T.StringType())) \
        .withColumn('pol_name', F.col('ref_poltype.longdesc')) \
        .withColumn('pol_name_oic', F.col('ref_poltype.longdesc'))\
        .withColumn('pol_start_date', _to_iso_tz7_from_yyyymmdd('gchi.ccdate'))\
        .withColumn('pol_end_date', _to_iso_tz7_from_yyyymmdd('gchi.crdate'))\
        .withColumn('pol_claim_document', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('pol_payment_term', F.when(F.max("gchi.ccdate").over(w) != F.min("gchi.ccdate").over(w),F.concat(F.lit("กำหนดการชำระเบี้ยครั้งถัดไป วันที่ "),F.max("gchi.ccdate").over(w))).otherwise(F.lit(NON_REQ_NULL_STR)))\
        .withColumn('pol_payment_amount', _zero_guard(lit(REQ_NULL_DEC)))\
        .withColumn('pol_payment_period', F.coalesce(_billfreq_map.getItem(F.col('gchi.billfreq')), F.lit('05')).cast(T.StringType()))\
        .withColumn('pol_info_url', lit('https://www.krungthai-axa.co.th/th/employee-portal').cast(T.StringType()))\
        .withColumn('pol_claim_url', lit('https://www.krungthai-axa.co.th/th/employee-portal').cast(T.StringType()))\
        .withColumn('pol_document_urls', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('pol_year', F.col('pyr.pol_year_cnt').cast(T.IntegerType())) \
        .withColumn('pol_status_note', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('pol_status_date', _to_iso_tz7_from_yyyy_mm_dd('pol.datime'))\
        .withColumn('pol_expropriation_value', _zero_guard(lit(REQ_NULL_DEC)))\
        .withColumn('pol_immediate_refund', _zero_guard(lit(REQ_NULL_DEC)))\
        .withColumn('pol_sale_date', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('pol_transaction_date', _to_iso_tz7_from_yyyy_mm_dd('pol.datime'))\
        .withColumn('pol_approved_date', _to_iso_tz7_from_yyyymmdd('gchi.ccdate'))\
        .withColumn('pol_issue_date', _to_iso_tz7_from_yyyymmdd('gchi.ccdate'))\
        .withColumn('pol_account_date', _to_iso_tz7_from_yyyymmdd('gchi.btdatenr'))\
        .withColumn('pol_agree_date', _to_iso_tz7_from_yyyymmdd('pol.occdate'))\
        .withColumn('pol_distribution', F.lit('03').cast(T.StringType())) \
        .withColumn('pol_distribution_other_detail', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('pol_license', F.col('agp.licence').cast(T.StringType()))\
        .withColumn('pol_re_insured_contact_number', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('pol_life_detail', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .withColumn('renewal', F.when(F.col('pol_year')==1, F.lit('03')).otherwise(F.lit('01')).cast(T.StringType()))\
        .withColumn('renewal_pol_id', F.when(F.col('renewal')=='01', F.concat_ws('', F.col('pol_id'), F.lit('_'),(F.date_format(F.to_date(F.col('gchi.ccdate').cast('string'),'yyyyMMdd'),'yyyy').cast('int')+1).cast('string'))).otherwise(F.lit(None).cast(T.StringType())))

    output_policy_df = output_policy_df.withColumn(
        "pol_status",
        F.when((~F.col("pol.statcode").isin("IF", "PN", "PE", "PR")) & (F.to_date(F.col("gchi.crdate").cast("string"), "yyyyMMdd") >= F.current_date()),
            F.when(F.col("pol_year") == 1, F.lit("0100"))
             .otherwise(F.lit("0200"))
        )
        .when(
            F.col("pol.statcode") == "IF",F.when(F.col("pol_year") == 1, F.lit("0100"))
             .otherwise(F.lit("0200"))
        )
        .when(F.col("pol.statcode").isin("PN", "PE", "PR"), F.lit("0100"))
        .when(F.col("pol.statcode") == "CA", F.lit("0400"))
        .when(F.col("pol.statcode") == "LA", F.lit("0700"))
        .when(F.col("pol.statcode") == "DC", F.lit("0402"))
        .when(F.col("pol.statcode") == "QR", F.lit("9999"))
        .otherwise(F.lit("9999"))

    )

    # =====================================================
    # SECTION 7: Build Customer Struct
    # =====================================================

    print('Building Customer struct...')

    output_policy_df = output_policy_df.withColumn('customer',
        F.struct(
            # 20260212 tarat added: after discuss with K.Panisara else cust_type --> send 0
            F.when(F.col('cust.clttype')=='C', F.lit('O')).otherwise(F.lit('0')).cast(T.StringType()).alias('cust_type'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_type_other_detail'),
            F.when(F.col('cust.clttype')=='C', F.lit('01')).otherwise(F.lit(REQ_NULL_STR)).cast(T.StringType()).alias('cust_id_type'),
            F.col('cid.zwhidnum').cast(T.StringType()).alias('cust_id'),
            F.when(F.col('cust.clttype')=='C', F.trim(F.col('cust.lsurname'))).otherwise(F.lit(REQ_NULL_STR)).cast(T.StringType()).alias('cust_company_name'),
            F.lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_first_name'),
            F.lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_last_name'),
            F.lit(NON_REQ_NULL_INT).cast(T.IntegerType()).alias('cust_age'),
            # F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_birthday'),
            F.lit('1753-01-01T00:00:00+00:00').cast(T.StringType()).alias('cust_birthday'),
            F.when(F.col('cust.clttype')=='C', F.lit('99')).otherwise(F.lit(REQ_NULL_STR)).cast(T.StringType()).alias('cust_gender'),
            F.concat_ws('',F.trim(F.col('cust.cltaddr01')),F.trim(F.col('cust.cltaddr02')),F.trim(F.col('cust.cltaddr03'))).cast(T.StringType()).alias('cust_address'),
            F.lit('00').cast(T.StringType()).alias('cust_sub_district'),
            F.lit('00').cast(T.StringType()).alias('cust_district'),
            F.coalesce(F.substring(F.col('ref_location.code'), 0, 2), F.lit('00')).cast(T.StringType()).alias('cust_province'),
            # F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_zip_code'), 
            F.col('cust.cltpcode').cast(T.StringType()).alias('cust_zip_code'), 
            F.lit(cust_country_code).cast(T.StringType()).alias('cust_country_code'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_phone_number'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_mobile_number'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_email'),
            F.lit(NON_REQ_NULL_INT).cast(T.IntegerType()).alias('cust_issued_age'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_race'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_nationality'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_religion'),
            F.lit('99').cast(T.StringType()).alias('cust_occupation_level'),
            F.lit('ประกันกลุ่มของกรุงไทยแอกซ่าเก็บocc levelที่insure').cast(T.StringType()).alias('cust_occupation_level_other_detail'),
            F.lit('เนื่องจากประกันกลุ่มของกรุงไทยแอกซ่า ไม่ได้เก็บ occupation').cast(T.StringType()).alias('cust_occupation'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_job'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_position'),
            F.lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_business_type'),
            _zero_guard(F.lit(NON_REQ_NULL_DEC)).alias('cust_income_per_year'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_us_person'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_us_person_born'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_us_person_green_card'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_us_person_vat'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_us_person_183'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_permanent'),
            F.lit('00').cast(T.StringType()).alias('cust_sub_district_permanent'),
            F.lit('00').cast(T.StringType()).alias('cust_district_permanent'),
            F.lit('00').cast(T.StringType()).alias('cust_province_permanent'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_work'),
            F.lit('00').cast(T.StringType()).alias('cust_sub_district_work'),
            F.lit('00').cast(T.StringType()).alias('cust_district_work'),
            F.lit('00').cast(T.StringType()).alias('cust_province_work'),
            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_current'),
            F.lit('00').cast(T.StringType()).alias('cust_sub_district_current'),
            F.lit('00').cast(T.StringType()).alias('cust_district_current'),
            F.lit('00').cast(T.StringType()).alias('cust_province_current')
        )
    )

    # =====================================================
    # SECTION 8: Build Dummy Insured Array
    # =====================================================

    print('Building Dummy Insured array (required by OIC spec)...')

    # Create dummy insured with DUMMY_{policy_id} format
    # All numeric values set to 0
    output_policy_df = output_policy_df.withColumn('insureds',
        F.array(
            F.struct(
                lit(1).cast(T.IntegerType()).alias('insrd_cov_plan_seq'),
                lit(1).cast(T.IntegerType()).alias('insrd_seq'),
                lit('99').cast(T.StringType()).alias('insrd_type'),
                lit('99').cast(T.StringType()).alias('insrd_id_type'),
                concat(lit('DUMMY_'), F.trim(F.col('pol.chdrnum'))).cast(T.StringType()).alias('insrd_id'),
                lit('DUMMY').cast(T.StringType()).alias('insrd_first_name'),
                lit('INSURED').cast(T.StringType()).alias('insrd_last_name'),
                lit(0).cast(T.IntegerType()).alias('insrd_age'),
                lit('1999-01-01T00:00:00+07:00').cast(T.StringType()).alias('insrd_birthday'),
                lit('99').cast(T.StringType()).alias('insrd_gender'),
                lit(REQ_NULL_STR).cast(T.StringType()).alias('insrd_address'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_sub_district'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_district'),
                lit('00').cast(T.StringType()).alias('insrd_province'),
                lit('00').cast(T.StringType()).alias('insrd_zip_code'),
                lit(cust_country_code).cast(T.StringType()).alias('insrd_country_code'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_phone_number'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_mobile_number'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_email'),
                lit(0).cast(T.IntegerType()).alias('insrd_issued_age'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_race'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_nationality'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_religion'),
                lit('88').cast(T.StringType()).alias('insrd_occupation_level'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation_level_other_detail'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_job'),
                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_position'),
                lit(REQ_NULL_STR).cast(T.StringType()).alias('insrd_business_type'),
                _zero_guard(lit(0.00)).alias('insrd_income_per_year'),
                lit('01').cast(T.StringType()).alias('standard_risk'),
                lit('01').cast(T.StringType()).alias('medical'),
                F.lit(None).cast(T.ArrayType(T.StringType())).alias('insrd_pol_sub_ids'),
                empty_array_of(T.StringType()).alias("insrd_beneficiaries")
            )
        )
    )

    # =====================================================
    # SECTION 9: Build Sub-Policies Array
    # =====================================================

    print('Building Real Sub-Policies ')

    # v2 dummy - sub_policies and wait insured only update sub_policies
    w = Window.partitionBy("pol.chdrnum").orderBy(F.monotonically_increasing_id())
    sub_pol_output = (output_policy_df.select('pol.chdrnum','ref_prodtype.lserff_code','gchi.ccdate','gchi.crdate').distinct()
                        .withColumn('sub_pol_id',F.concat_ws("_", F.col("pol.chdrnum"), F.row_number().over(w)))
                        .withColumn('sub_pol_type', F.lit("A"))
                        .withColumn('sub_pol_ppt', F.col("ref_prodtype.lserff_code"))
                        .withColumn('sub_pol_start_date', _to_iso_tz7_from_yyyymmdd("gchi.ccdate"))
                        .withColumn('sub_pol_end_date', _to_iso_tz7_from_yyyymmdd("gchi.crdate"))
                        .withColumn('sub_pol_sum_insured_per_year_exclude', _zero_guard(F.lit(0.00)))
                        .withColumn('sub_pol_sum_insured_per_year_include', _zero_guard(F.lit(0.00)))
                        .withColumn('policy_cov_plan_seqs', F.array(F.struct(
                                        (F.row_number().over(w)).cast(T.IntegerType()).alias('cov_plan_seq'),
                                        F.lit('cov_plan_name_dummy').cast(T.StringType()).alias('cov_plan_name'),
                                        _zero_guard(F.lit(50000.00).cast(T.DoubleType())).alias('cov_plan_sum_insured'),
                                        _zero_guard(F.lit(0.00).cast(T.DoubleType())).alias('cov_plan_maturity_benefit'),
                                        _zero_guard(F.lit(0.00).cast(T.DoubleType())).alias('cov_plan_survival_benefit_min'),
                                        _zero_guard(F.lit(0.00).cast(T.DoubleType())).alias('cov_plan_survival_benefit_max'),
                                        F.array(F.struct(
                                            F.lit('CR0002').cast(T.StringType()).alias('cov_plan_cov_code'),
                                            F.lit('cov_plan_cov_detail_dummy').cast(T.StringType()).alias('cov_plan_cov_detail'),
                                            _zero_guard(F.lit(50000.00).cast(T.DoubleType())).alias('cov_plan_cov_amt'),
                                            _zero_guard(F.lit(0.00).cast(T.DoubleType())).alias('cov_plan_cov_sum_insured_per_coverage_per_year'),
                                            _zero_guard(F.lit(0.00).cast(T.DoubleType())).alias('cov_plan_cov_sum_insured_per_coverage_per_time'),
                                            _zero_guard(F.lit(0.00).cast(T.DoubleType())).alias('cov_plan_cov_co_payment'),
                                            _zero_guard(F.lit(NON_REQ_NULL_DEC)).cast(T.DoubleType()).alias('cov_plan_cov_deductible_amount'),
                                            F.lit(NON_REQ_NULL_STR).alias('cov_plan_cov_deductible_detail'),
                                            F.lit(NON_REQ_NULL_INT).cast(T.IntegerType()).alias('cov_plan_cov_number_of_day'),
                                            F.lit(1).alias('cov_plan_cov_cover_year'),
                                            F.lit(1).alias('cov_plan_cov_payment_year'),
                                            F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_plan_cov_claim_status'),
                                        )).alias('coverages_under_plans')
                                    )))
                        .withColumn('coverages', F.array([]))
                        .groupBy("chdrnum")
                        .agg(
                            F.collect_list(
                                F.struct(
                                    F.col("sub_pol_id").alias("sub_pol_id"),
                                    F.col("sub_pol_type").alias("sub_pol_type"),
                                    F.col("sub_pol_ppt").alias("sub_pol_ppt"),
                                    F.col("sub_pol_start_date").alias("sub_pol_start_date"),
                                    F.col("sub_pol_end_date").alias("sub_pol_end_date"),
                                    F.col("sub_pol_sum_insured_per_year_exclude").alias("sub_pol_sum_insured_per_year_exclude"),
                                    F.col("sub_pol_sum_insured_per_year_include").alias("sub_pol_sum_insured_per_year_include"),
                                    F.col("policy_cov_plan_seqs"),
                                    F.col("coverages").alias("coverages"),
                                )
                            ).alias("sub_policies")
                        )
                     )
    
    output_policy_df = output_policy_df.join(sub_pol_output.select("chdrnum","sub_policies"), on="chdrnum", how="left")\
                             .withColumn("sub_policies", F.col("sub_policies"))

    # =====================================================
    # SECTION 10: Build Empty Arrays for other fields
    # =====================================================

    print('Building empty arrays for beneficiaries, endorsements, loans, etc...')

    # cast investments schema
    policy_fund_codes_schema = T.ArrayType(
        T.StructType([
            T.StructField("inv_fund_code", T.StringType()),
            T.StructField("inv_fund_unit", T.DoubleType()),
            T.StructField("inv_fund_nav", T.DoubleType()),
            T.StructField("inv_fund_thai_name", T.StringType()),
            T.StructField("inv_fund_eng_name", T.StringType()),
            T.StructField("inv_fund_type", T.StringType()),
            T.StructField("inv_fund_amc", T.StringType()),
            T.StructField("inv_fund_data_status", T.StringType())
        ])
    )
    investment_struct = T.StructType([
        T.StructField("inv_transaction_date", T.StringType()),
        T.StructField("inv_transaction_type", T.StringType()),
        T.StructField("policy_fund_codes", policy_fund_codes_schema)
    ])
    investments_array_schema = T.ArrayType(investment_struct)

    
    output_policy_df = (output_policy_df
        .withColumn('beneficiaries', 
            F.array(F.struct(
                F.lit(0).cast(T.IntegerType()).alias('beneficiary_seq_no'),
                F.lit('บริษัทไม่มีการเก็บ insured beneficiary ในระบบ').cast(T.StringType()).alias('beneficiary_name'),
                F.lit('99').cast(T.StringType()).alias('beneficiary_relation'),
                F.lit('ไม่มีเก็บในระบบ').cast(T.StringType()).alias('beneficiary_relation_other_detail'),
               _zero_guard(F.lit(100.001).cast(T.DoubleType())).alias('benefit_ratio'),
            ))
        )
        .withColumn('endorsements', empty_array_of(T.StringType()))
        .withColumn('loans', empty_array_of(T.StringType()))
        .withColumn('loan_payments', empty_array_of(T.StringType()))
        .withColumn('investments', empty_array_of(T.StringType()))
        # .withColumn('investments', F.lit(None).cast(investments_array_schema))
    )

    # =====================================================
    # SECTION 11: Select Final Columns
    # =====================================================

    print('Selecting final columns...')

    final_columns = [
        'pol_refer_code_of_company', 'pol_transaction_status', 'pol_company_id', 'pol_id',
        'pol_type', 'pol_type_gli_name', 'pol_type_gli_plan', 'pol_product_type',
        'pol_product_type_other_detail', 'pol_type_2', 'pol_sub_type', 'pol_name',
        'pol_name_oic', 'pol_start_date', 'pol_end_date', 'pol_claim_document',
        'pol_payment_term', 'pol_payment_amount', 'pol_payment_period', 'pol_info_url',
        'pol_claim_url', 'pol_document_urls', 'pol_year', 'pol_status', 'pol_status_note',
        'pol_status_date', 'pol_expropriation_value', 'pol_immediate_refund', 'pol_sale_date',
        'pol_transaction_date', 'pol_approved_date', 'pol_issue_date', 'pol_account_date',
        'pol_agree_date', 'pol_distribution', 'pol_distribution_other_detail', 'pol_license',
        'pol_re_insured_contact_number', 'pol_life_detail',
        'renewal', 'renewal_pol_id',
        'customer', 'insureds', 'sub_policies', 'beneficiaries',
        'endorsements', 'loans', 'loan_payments', 'investments'
    ]

    output_policy_df = output_policy_df.select(final_columns).dropDuplicates(['pol_id', 'pol_refer_code_of_company'])

    print(f'Group Standard Policy Only aggregation completed. Record count: {output_policy_df.count()}')

    return output_policy_df