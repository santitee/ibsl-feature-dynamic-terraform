from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import substring

# Zero Guard for Decimal Fields
_ZERO_GUARD_ENABLED = True
_ZERO_GUARD_EPSILON = 0.001
_MAX_LEN_STR = 150

def _zero_guard(col_expr):
    if _ZERO_GUARD_ENABLED:
        return F.when(col_expr == 0.0, F.lit(_ZERO_GUARD_EPSILON)).otherwise(col_expr).cast(T.DoubleType())
    return col_expr.cast(T.DoubleType())

def _zero_guard_decimal(col_expr, precision=18, scale=2):
    dec = T.DecimalType(precision, scale)
    if _ZERO_GUARD_ENABLED:
        return F.when(col_expr.cast(dec) == F.lit(0).cast(dec),
                      F.lit(_ZERO_GUARD_EPSILON).cast(dec)).otherwise(col_expr.cast(dec))
    return col_expr.cast(dec)

def _string_lenght_guard(col_expr, max_len=_MAX_LEN_STR):
    """
    Trim string and cut to max_len characters.
    """
    return F.substring(
        F.trim(col_expr),
        1,
        max_len
    ).cast(T.StringType())


def _to_iso_tz7_from_yyyymmdd(col):
    c = F.col(col).cast(T.StringType())
    return F.when(
        (c.isNotNull()) & (F.length(c) == 8),
        F.concat_ws(
            "",
            F.substring(c, 1, 4), F.lit("-"),
            F.substring(c, 5, 2), F.lit("-"),
            F.substring(c, 7, 2), F.lit("T00:00:00+07:00")
        )
    ).otherwise(F.lit(None).cast(T.StringType()))

def _to_iso_tz7_from_yyyy_mm_dd(col):
    d = F.to_date(F.col(col).cast(T.StringType()), 'yyyy-MM-dd')
    return F.when(
        d.isNotNull(),
        F.concat_ws('', F.date_format(d, 'yyyy-MM-dd'), F.lit('T00:00:00+07:00'))
    ).otherwise(F.lit(None).cast(T.StringType()))


def grp_std_insured_only_aggregation(run_params, spark_session, start_date, end_date):

    spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    print('Reading Group Policy Insured source tables...')

    # =====================================================
    # SECTION 1: Read Source Tables
    # =====================================================
    
    # Policy Header - ATLLDTA_CHDRPF
    if run_params.workflow_type == 'Daily':
        df_chdrpf_ilcl_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)))
        )
        df_chdrpf_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)))
        )
    elif run_params.workflow_type == 'One-Time-Inforce':
        df_chdrpf_ilcl_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Inforce statuses
            )
        )
        df_chdrpf_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Inforce statuses
            )
        )
    elif run_params.workflow_type == 'One-Time-Non-Inforce':
        df_chdrpf_ilcl_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (~F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Non-Inforce statuses
            )
        )
        df_chdrpf_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                 (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)) &
                (~F.col('statcode').isin('IF', 'PN', 'PE','PR'))  # Non-Inforce statuses
            )
        )
    else:
        df_chdrpf_ilcl_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)
            )
        )
        df_chdrpf_source = (spark_session.table('processing_ibs.ibs_atlldta_chdrpf')
            .select('chdrnum','cnttype','cownnum','occdate','currto','statcode','datime')
            .filter(
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)
            )
        )
    
    # 2026/05/20 : added 'CR3', 'MRT'
    grp_std_cnttype_list = [
        'CHS','CR1','CR2','EB1','GCI','GHP','GHS','GHT','GLP','GLT',
        'GPA','GTL','PAG','PAS','PA1','PA2','PA3','PHS','PH1','PH2',
        'CR4','CR5','CR6', 'CR3', 'MRT'
    ]
    df_chdrpf_source = df_chdrpf_source.filter(F.col('cnttype').isin(grp_std_cnttype_list))
    # 2026/05/07 : Union df_chdrpf_source
    df_chdrpf_ilcl_source = df_chdrpf_ilcl_source.filter(F.col('cnttype').isin(grp_std_cnttype_list))
    df_chdrpf_source = df_chdrpf_source.unionByName(df_chdrpf_ilcl_source).dropDuplicates()
    print('chdrpf count :',df_chdrpf_source.count())

    # new 2026/04/29 union table *_ilcl 
    df_gmhdpf_source = (spark_session.table('processing_ibs.ibs_atlldta_gmhdpf')
            .select('chdrnum','mbrno','dpntno','clntnum','age','occpclas','datime','dtetrm','dteatt')
        )
    df_gmhdpf_ilcl = spark_session.table('processing_ibs.ibs_atlldta_gmhdpf_ilcl').select('chdrnum','mbrno','dpntno','clntnum','age','occpclas','datime','dtetrm','dteatt')
    df_gmhdpf_source = df_gmhdpf_source.unionByName(df_gmhdpf_ilcl).dropDuplicates()
    # 2026-06-08 : add filter by date for member
    df_gmhdpf_source = df_gmhdpf_source.filter(
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") >= start_date) &
                (F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date)
            )

    df_clntpf_source = (
        spark_session.table('processing_ibs.ibs_atlldta_clntpf')
        .select('clntnum','validflag','clttype','natlty','lgivname','lsurname',
                'secuityno','cltsex','cltdob',
                'cltaddr01','cltaddr02','cltaddr03','cltaddr04','cltaddr05','cltpcode','ctrycode',
                'cltphone01','cltphone02')
    )

    df_gchipf_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf').select('chdrnum','ccdate','crdate','btdatenr', 'billfreq','nofmbr','agntnum')
    # 2026/05/11 : union df_gchipf_ilcl_source
    df_gchipf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf_ilcl').select('chdrnum','ccdate','crdate','btdatenr','billfreq','nofmbr','agntnum')
    df_gchipf_source = df_gchipf_source.unionByName(df_gchipf_ilcl_source).dropDuplicates()

    df_gbihpf_source = spark_session.table('processing_ibs.ibs_atlldta_gbihpf').select('chdrnum','datime')

    # new 2026/04/29 union table *_ilcl 
    df_gxhipf_source = (
        spark_session.table('processing_ibs.ibs_atlldta_gxhipf')
        .select('chdrnum','mbrno','dpntno','planno','prodtyp','suminsu','effdate')
    )
    df_gxhipf_ilcl= spark_session.table('processing_ibs.ibs_atlldta_gxhipf_ilcl').select('chdrnum','mbrno','dpntno','planno','prodtyp','suminsu','effdate')
    df_gxhipf_source = df_gxhipf_source.unionByName(df_gxhipf_ilcl).dropDuplicates()

    df_ref_group_product_type_lserff = spark_session.table('processing_ibs.ibs_ref_group_product_type_lserff')

    df_ref_location_code = spark_session.table('processing_ibs.ibs_ref_location_code').select('province_th',substring('code', 1, 2).alias('code')).distinct()
    
    df_bnfhpf_source = spark_session.table('processing_ibs.ibs_atlldta_bnfhpf')

    # 2026-06-02: add visyear for cov_plan_cov_number_of_day fallback
    df_bnflpf_source = (
        spark_session.table('processing_ibs.ibs_atlldta_bnflpf')
        .select('chdrnum','planno','prodtyp','bencde','amtyear','amtday','amtvis','daysvis','visyear','effdate','tranno')
        .distinct()
    )

    df_ref_g400_product_benefit_code = spark_session.table('processing_ibs.ibs_ref_g400_product_benefit_code').select('prodtyp', 'bencde', 'longdesc1', 'longdesc2', 'oic_code')
    
    df_ref_g400_oic_benefit_code_definition = spark_session.table('processing_ibs.ibs_ref_g400_claim_oic_benefit_code_definition').select('oic_code_bnf', 'oic_des_th')

    # add new itemsmt
    df_ref_g400_atlldta_itemsmt = spark_session.table("processing_ibs.ibs_ref_g400_atlldta_itemsmt")

    df_ref_g400_atlldta_itemsmt = df_ref_g400_atlldta_itemsmt.filter(~(F.col("itemtabl").rlike('^"?ITEMTABL"?$') & F.col("itemitem").rlike('^"?ITEMITEM"?$') & F.col("genarea").rlike('^"?GENAREA"?$'))).filter(F.col("itemtabl")=='TR9RM')


    # =====================================================
    # SECTION 2: Default Values
    # =====================================================
    print('Declaring Default Params Value...')
    pol_refer_code_of_company_prefix = "POL_KAL_"
    # 2026/04/08 : pol_transaction_status force A for insured only
    pol_transaction_status = "A" 
    pol_company_id = "1022"
    cust_country_code = "THA"

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00

    NON_REQ_NULL_STR = None
    NON_REQ_NULL_INT = None
    NON_REQ_NULL_DEC = None

    # =====================================================
    # SECTION 3: GCHIPF Aggregates (latest ccdate/crdate per policy)
    # =====================================================
    print('Building Output DataFrame...')
    window_gchi = Window.partitionBy('chdrnum').orderBy(F.col('ccdate').desc())
    df_gchipf_latest = df_gchipf_source \
        .withColumn('rn', F.row_number().over(window_gchi)) \
        .filter(F.col('rn') == 1) \
        .select('chdrnum','ccdate','crdate','btdatenr', 'billfreq','nofmbr','agntnum')

    # =====================================================
    # SECTION 4: Latest member record per (policy, member, dependent)
    # =====================================================
    
    print('Joining Member tables with Policy and Client...')
    window_member = Window.partitionBy('chdrnum','mbrno').orderBy(F.col('dpntno').desc())
    df_gmhdpf_latest = (
        df_gmhdpf_source
        .withColumn('row_num', F.row_number().over(window_member))
        .filter(F.col('row_num') == 1)
        .drop('row_num')
    )

    df_member_with_policy = (
        df_gmhdpf_latest.alias('mbr')
        .join(df_chdrpf_source.alias('pol'),
              F.trim(F.col('mbr.chdrnum')) == F.trim(F.col('pol.chdrnum')),
              'inner')
    )

    df_member_with_client = (
        df_member_with_policy
        .join(df_clntpf_source.alias('cust'),
              (F.trim(F.col('mbr.clntnum')) == F.trim(F.col('cust.clntnum'))) & (F.col('cust.validflag')=='1'),
              'left')
    )

    df_member_with_client = df_member_with_client.join(
        df_ref_location_code.alias('ref_location'),
        F.trim(
                F.when(
                    (F.col('cust.cltaddr05').isNull()) | (F.col('cust.cltaddr05') == ''),
                    F.col('cust.cltaddr04')
                ).otherwise(F.col('cust.cltaddr05'))
            ) == F.trim(F.col('ref_location.province_th')),
            'left'
    )

    df_policy_with_gchi = df_member_with_client \
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

    df_member_with_client = df_policy_with_gchi \
            .join(
                df_gbihpf_latest.alias('gbih'),
                F.trim(F.col('pol.chdrnum')) == F.trim(F.col('gbih.chdrnum')),
                'left'
            )
    
    df_planno_max = df_bnfhpf_source.groupBy('chdrnum').agg(
            F.max(F.col('planno').cast('int')).alias('max_planno')
    )

    df_member_with_client = df_member_with_client.join(df_planno_max.alias('pln'),
                                             F.trim(F.col('pol.chdrnum')) == F.trim(F.col('pln.chdrnum')),
                                             'left')

    # =====================================================
    # SECTION 5: Set up policy
    # =====================================================
    
    w = Window.partitionBy("pol.chdrnum")

    output_policy_df = (df_member_with_client
        .withColumn('pol_refer_code_of_company',
                        F.concat_ws('',
                            F.lit(pol_refer_code_of_company_prefix),
                            F.trim(F.col('pol.chdrnum')),
                            F.lit('_'),
                            F.date_format(F.to_date(F.col('gchi.ccdate').cast('string'),'yyyyMMdd'),'yyyy')
                        ).cast(T.StringType())
                    )
        .withColumn('pol_transaction_status', F.lit('A').cast(T.StringType()))\
        .withColumn('pol_company_id', F.lit(pol_company_id).cast(T.StringType()))\
        .withColumn('pol_id', F.trim(F.col('pol.chdrnum')).cast(T.StringType()))\
        .withColumn('pol_type', F.lit(None).cast(T.StringType()))
        .withColumn('pol_type_gli_name', F.lit(None).cast(T.IntegerType()))
        .withColumn('pol_type_gli_plan', F.lit(None).cast(T.IntegerType()))
        .withColumn('pol_product_type', F.lit(None).cast(T.StringType()))
        .withColumn('pol_product_type_other_detail', F.lit("").cast(T.StringType()))
        .withColumn('pol_type_2', F.lit(None).cast(T.StringType()))
        .withColumn('pol_sub_type', F.lit(None).cast(T.StringType()))
        .withColumn('pol_name', F.lit(None).cast(T.StringType()))
        .withColumn('pol_name_oic', F.lit(None).cast(T.StringType()))
        .withColumn('pol_start_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_end_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_claim_document', F.lit(None).cast(T.StringType()))
        .withColumn('pol_payment_term', F.lit(None).cast(T.StringType()))
        .withColumn('pol_payment_amount', F.lit(None).cast(T.DecimalType(18,2)))
        .withColumn('pol_payment_period', F.lit(None).cast(T.StringType()))
        .withColumn('pol_info_url', F.lit(None).cast(T.StringType()))
        .withColumn('pol_claim_url', F.lit(None).cast(T.StringType()))
        .withColumn('pol_document_urls', F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn('pol_year', F.lit(None).cast(T.IntegerType()))
        .withColumn('pol_status', F.lit(None).cast(T.StringType()))
        .withColumn('pol_status_note', F.lit(None).cast(T.StringType()))
        .withColumn('pol_status_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_expropriation_value', F.lit(None).cast(T.DecimalType(18,2)))
        .withColumn('pol_immediate_refund', F.lit(None).cast(T.DecimalType(18,2)))
        .withColumn('pol_sale_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_transaction_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_approved_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_issue_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_account_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_agree_date', F.lit(None).cast(T.StringType()))
        .withColumn('pol_distribution', F.lit(None).cast(T.StringType()))
        .withColumn('pol_distribution_other_detail', F.lit(None).cast(T.StringType()))
        .withColumn('pol_license', F.lit(None).cast(T.StringType()))
        .withColumn('pol_re_insured_contact_number', F.lit(None).cast(T.StringType()))
        .withColumn('pol_life_detail', F.lit(None).cast(T.StringType()))
        .withColumn('renewal', F.lit(None).cast(T.StringType()))
        .withColumn('renewal_pol_id', F.lit(None).cast(T.StringType()))
        .withColumn('customer', F.lit(None).cast(T.StructType([
              T.StructField('cust_type', T.StringType()),
              T.StructField('cust_type_other_detail', T.StringType()),
              T.StructField('cust_id_type', T.StringType()),
              T.StructField('cust_id', T.StringType()),
              T.StructField('cust_company_name', T.StringType()),
              T.StructField('cust_first_name', T.StringType()),
              T.StructField('cust_last_name', T.StringType())
        ])))
        .withColumn('beneficiaries', F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn('endorsements', F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn('loans', F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn('loan_payments', F.lit(None).cast(T.ArrayType(T.StringType())))
        .withColumn('investments', F.lit(None).cast(T.ArrayType(T.StringType())))
        )


    # =====================================================
    # SECTION 6: Build insured struct (per MEMBER)
    # =====================================================
    print('Building insureds (per member)...')
    gender_expr = (
        F.when(F.col('cust.clttype')=='C', '99')
         .when(F.trim(F.col('cust.cltsex')) == 'M', '01')
         .when(F.trim(F.col('cust.cltsex')) == 'F', '02')
         .otherwise('99')
    )

    sub_pol_id_expr = F.concat(F.trim(F.col('pol.chdrnum')), F.lit('_'), F.lpad(F.trim(F.col('mbr.mbrno')), 5, '0'))

    insrd_id_type_expr = (
        F.when((F.col('cust.clttype')=='P') & (F.col('cust.natlty')=='THA'), '01')
         .when((F.col('cust.clttype')=='P') & (F.col('cust.natlty')!='THA'), '02')
         .when(F.col('cust.clttype')=='C', '01')
         .otherwise('99')
    )
    insrd_id_expr = F.coalesce(
        F.trim(F.col('cust.secuityno')),
        F.concat(F.trim(F.col('cust.lgivname')), F.lit(' '), F.trim(F.col('cust.lsurname')))
    )

    # insured_seq 
    df_insured_per_member = (
        df_member_with_client
        .withColumn(
            'insrd_seq',
            F.row_number().over(
                Window.partitionBy(F.col('pol.chdrnum')).orderBy(F.col('mbr.mbrno'))
            )
        )
        .withColumn(
            'insured_struct',
            F.struct(
                F.lit(1).cast(T.IntegerType()).alias('insrd_cov_plan_seq'), # not require
                F.col('insrd_seq').cast(T.IntegerType()).alias('insrd_seq'),
                # F.lit('P').cast(T.StringType()).alias('insrd_type'),
                F.when((F.col('cust.clttype')=='P') & (F.col('cust.natlty')=='THA'), F.lit('P'))
                 .when((F.col('cust.clttype')=='P') & (F.col('cust.natlty') !='THA'), F.lit('F'))
                 .otherwise(F.lit('99')).cast(T.StringType()).alias('insrd_type'),
                insrd_id_type_expr.alias('insrd_id_type'),
                insrd_id_expr.cast(T.StringType()).alias('insrd_id'),
                F.when(F.col('cust.clttype')=='P', F.trim(F.col('cust.lgivname'))).otherwise(F.lit(REQ_NULL_STR)).cast(T.StringType()).alias('insrd_first_name'),
                F.when(F.col('cust.clttype')=='P', F.trim(F.col('cust.lsurname'))).otherwise(F.lit(REQ_NULL_STR)).cast(T.StringType()).alias('insrd_last_name'),
                F.coalesce(
                    F.col('mbr.age'),
                    F.when(
                        (F.trim(F.col('cust.cltdob')).isNotNull()) & (F.trim(F.col('cust.cltdob')) != '0'),
                        F.floor((F.date_format(F.current_date(), 'yyyyMMdd').cast(T.IntegerType()) - F.trim(F.col('cust.cltdob')).cast(T.IntegerType())) / 10000)
                    ).otherwise(F.lit(0))
                ).cast(T.IntegerType()).alias('insrd_age'),
                F.coalesce(_to_iso_tz7_from_yyyymmdd('cust.cltdob'), F.lit('9999-01-01T00:00:00+07:00')).cast(T.StringType()).alias('insrd_birthday'),
                gender_expr.cast(T.StringType()).alias('insrd_gender'),
                F.concat_ws(' ', F.col('cust.cltaddr01'), F.col('cust.cltaddr02'), F.col('cust.cltaddr03')).alias('insrd_address'),
                F.lit(None).alias('insrd_sub_district'),
                F.lit(None).alias('insrd_district'),
                F.coalesce(F.substring(F.col('ref_location.code'), 0, 2), F.lit(REQ_NULL_STR)).cast(T.StringType()).alias('insrd_province'),
                F.lit(None).cast(T.StringType()).alias('insrd_zip_code'),
                F.coalesce(F.col('cust.ctrycode'), F.lit(cust_country_code)).cast(T.StringType()).alias('insrd_country_code'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_phone_number'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_mobile_number'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_email'),
                F.coalesce(
                    F.col('mbr.age'),
                    F.when(
                        (F.trim(F.col('cust.cltdob')).isNotNull()) & (F.trim(F.col('cust.cltdob')) != '0'),
                        F.floor((F.date_format(F.current_date(), 'yyyyMMdd').cast(T.IntegerType()) - F.trim(F.col('cust.cltdob')).cast(T.IntegerType())) / 10000)
                    ).otherwise(F.lit(0))
                ).cast(T.IntegerType()).alias('insrd_issued_age'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_race'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_nationality'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_religion'),
                F.coalesce(F.when(F.trim(F.col('mbr.occpclas')) == '', F.lit('99')).otherwise(F.col('mbr.occpclas')), F.lit('99')).cast(T.StringType()).alias('insrd_occupation_level'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation_level_other_detail'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_job'),
                F.lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_position'),
                F.lit('บริษัทไม่มีการเก็บ business type ในระบบ').cast(T.StringType()).alias('insrd_business_type'),
                F.lit(None).alias('insrd_income_per_year'),
                F.lit('03').cast(T.StringType()).alias('standard_risk'),
                F.lit('02').cast(T.StringType()).alias('medical'),
                F.array(sub_pol_id_expr).alias('insrd_pol_sub_ids'),
                F.array().cast(T.ArrayType(
                    T.StructType([
                        T.StructField('insrd_beneficiary_seq_no', T.IntegerType()),
                        T.StructField('insrd_beneficiary_name', T.StringType()),
                        T.StructField('insrd_beneficiary_relation', T.StringType()),
                        T.StructField('insrd_beneficiary_relation_other_detail', T.StringType()),
                        T.StructField('insrd_benefit_ratio', T.DecimalType(18,2))
                    ])
                )).alias('insrd_beneficiaries')
            )
        )
        # .withColumn('dedup_key', F.concat_ws('|', F.col('mbr.chdrnum'), F.col('mbr.mbrno'), F.col('mbr.dpntno')))
        .withColumn('dedup_key', F.concat_ws('|', F.col('mbr.chdrnum'), F.col('mbr.mbrno')))
    )

    df_insured_per_member = (
        df_insured_per_member
        .select(
            F.col('pol.chdrnum').alias('chdrnum'),
            F.col('mbr.mbrno').alias('mbrno'),
            F.col('gchi.ccdate').alias('max_ccdate'),
            F.col('gchi.crdate').alias('max_crdate'),
            F.col('insured_struct'),
            F.col('insrd_seq')
        ).distinct()
    )

    # =====================================================
    # SECTION 7 new : Build sub_policy per MEMBER 
    # =====================================================

    # build bnfhpf_sumins_percent
    df_bnfhpf_sumins_percent = df_bnfhpf_source.select('prodtyp','planno','bencde', 'bnftlst', F.concat(F.trim(F.col('bnftlst')), F.trim(F.col('bencde'))).alias('itemitem') ).distinct()
    df_bnfhpf_sumins_percent = df_bnfhpf_sumins_percent.join(df_ref_g400_atlldta_itemsmt,'itemitem', 'left' )
    df_bnfhpf_sumins_percent = df_bnfhpf_sumins_percent.select('prodtyp','planno','bencde', (F.col('genarea')/100000).alias('percent_sum') )

    # 2026/05/06 : change bnfhpf main - build Policy Coverage Plan Coverage 
    cov_prod_output = (df_bnfhpf_source.select('chdrnum','prodtyp','planno','bencde').distinct().alias('bnh')
                            .join(df_bnflpf_source.distinct().alias('bnf'),
                                                     (F.trim(F.col('bnh.chdrnum')) == F.trim(F.col('bnf.chdrnum')))&
                                                     (F.trim(F.col('bnh.planno')) == F.trim(F.col('bnf.planno')))&
                                                     (F.trim(F.col('bnh.prodtyp')) == F.trim(F.col('bnf.prodtyp')))&
                                                     (F.trim(F.col('bnh.bencde')) == F.trim(F.col('bnf.bencde'))),
                                                     'left')
                                               .join(df_ref_g400_product_benefit_code.distinct().alias('ref_prodben'), 
                                                     (F.trim(F.col('bnh.prodtyp')) == F.trim(F.col('ref_prodben.prodtyp')))&
                                                     (F.trim(F.col('bnh.bencde')) == F.trim(F.col('ref_prodben.bencde'))),
                                                     'inner')
                                               .join(df_ref_g400_oic_benefit_code_definition.distinct().alias('ref_bendef'),
                                                     (F.trim(F.col('ref_prodben.oic_code')) == F.trim(F.col('ref_bendef.oic_code_bnf'))),
                                                     'inner')
                                               .groupBy('bnh.chdrnum','bnh.prodtyp','bnh.bencde','amtyear','amtday','amtvis','daysvis','visyear','effdate','tranno','longdesc1','longdesc2','oic_code','oic_des_th')
                                               .agg(F.max(F.col('bnh.planno').cast('int')).alias('max_planno'))
                                             )

    # gxh_sum_sumin = (df_gxhipf_source.distinct().alias('gxh')
    #                    .join(df_gmhdpf_latest.select('chdrnum','mbrno','dpntno').distinct().alias('gmh'),
    #                         (F.trim(F.col('gxh.chdrnum')) == F.trim(F.col('gmh.chdrnum'))) &
    #                         (F.trim(F.col('gxh.mbrno')) == F.trim(F.col('gmh.mbrno'))) &
    #                         (F.trim(F.col('gxh.dpntno')) == F.trim(F.col('gmh.dpntno'))),
    #                          'inner'
    #                        )
    #                    .select('gxh.chdrnum','gxh.planno','prodtyp','suminsu').distinct().groupBy('gxh.chdrnum','gxh.planno','prodtyp').agg(F.sum(F.col('suminsu')).alias('sum_suminsu'))
    #                  )

    # 2026-06-08:  change suminsu logic 'gchdate.ccdate >= gxh.effdate' to (F.col('gxh.effdate') <= F.lit(end_date))
    gxh_sum_sumin = (df_gxhipf_source.distinct().alias('gxh')
                       .join(df_gmhdpf_latest.select('chdrnum','mbrno','dpntno').distinct().alias('gmh'),
                            (F.trim(F.col('gxh.chdrnum')) == F.trim(F.col('gmh.chdrnum'))) &
                            (F.trim(F.col('gxh.mbrno')) == F.trim(F.col('gmh.mbrno'))) &
                            (F.trim(F.col('gxh.dpntno')) == F.trim(F.col('gmh.dpntno'))),
                             'inner'
                           )
                        .join(df_gchipf_latest.select('chdrnum','ccdate').distinct().alias('gchdate'),
                             (F.trim(F.col('gxh.chdrnum')) == F.trim(F.col('gchdate.chdrnum'))),
                             'left'
                           )
                      .select('gxh.chdrnum','gxh.mbrno','gxh.planno','gxh.effdate','prodtyp', F.col('suminsu').alias('sum_suminsu'))
                      .where((F.col('gxh.effdate') <= F.lit(end_date))).groupBy('gxh.chdrnum','gxh.planno','prodtyp','gxh.mbrno')
                      .agg(F.max(F.struct(F.col('gxh.effdate'),F.col('sum_suminsu'))).alias('tmp'))
                      .select('gxh.chdrnum','gxh.planno','prodtyp','gxh.mbrno',F.col('tmp.effdate').alias('max_effdate'),F.col('tmp.sum_suminsu')))

    cov_prod_output = (cov_prod_output.distinct()
                       .join(gxh_sum_sumin.alias('gxh_sum'),
                             (F.trim(F.col('bnh.chdrnum')) == F.trim(F.col('gxh_sum.chdrnum')))&
                             (F.trim(F.col('max_planno')) == F.trim(F.col('gxh_sum.planno').cast('int')))&
                             (F.trim(F.col('bnh.prodtyp')) == F.trim(F.col('gxh_sum.prodtyp'))), 'inner'
                            )
                       .join(df_bnfhpf_sumins_percent.alias('bsc'), 
                              (F.trim(F.col('bnh.prodtyp')) == F.trim(F.col('bsc.prodtyp')))&
                              (F.trim(F.col('bnh.bencde')) == F.trim(F.col('bsc.bencde')))&
                              (F.trim(F.col('max_planno')) == F.trim(F.col('bsc.planno')).cast('int') ), 'left'
                            )
                       .withColumn('cov_plan_cov_code', F.col("oic_code"))
                       .withColumn('cov_plan_cov_detail', F.when(F.col("bnh.bencde").isNotNull(), _string_lenght_guard(F.col("oic_des_th"))).otherwise(F.lit("")))
                       # 2026-06-02: cov_plan_cov_amt don't cal with percent
                       .withColumn('cov_plan_cov_amt', _zero_guard(F.coalesce( F.col("sum_suminsu").cast(T.DoubleType()) , F.lit(0.00))))
                       #.withColumn('cov_plan_cov_amt', _zero_guard(F.coalesce( F.col("sum_suminsu").cast(T.DoubleType()) * F.coalesce(F.col("percent_sum").cast(T.DoubleType()), F.lit(1.0)) , F.lit(0.00))))
                       # 2026-06-02: per_year else -> cov_plan_cov_amt (major), was 0.00
                       # .withColumn('cov_plan_cov_sum_insured_per_coverage_per_year', _zero_guard(F.coalesce(F.col("bnf.amtyear").cast(T.DoubleType()), F.lit(0.00))))   # OLD
                       .withColumn('cov_plan_cov_sum_insured_per_coverage_per_year', _zero_guard(F.coalesce(F.col("bnf.amtyear").cast(T.DoubleType()), F.col("cov_plan_cov_amt").cast(T.DoubleType()))))
                       # 2026-06-02: per_time case3 (null) else -> cov_plan_cov_amt (major), was REQ_NULL_DEC
                       # .withColumn('cov_plan_cov_sum_insured_per_coverage_per_time', _zero_guard(F.when(F.col("bnf.amtday") > 0, F.col("bnf.amtday")).when(F.col("bnf.amtday") == 0, F.col("bnf.amtvis")).otherwise(F.lit(REQ_NULL_DEC)).cast(T.DoubleType())))   # OLD
                       .withColumn('cov_plan_cov_sum_insured_per_coverage_per_time', _zero_guard(F.when(F.col("bnf.amtday") > 0, F.col("bnf.amtday").cast(T.DoubleType())).when(F.col("bnf.amtday") == 0, F.col("bnf.amtvis").cast(T.DoubleType())).otherwise(F.col("cov_plan_cov_amt").cast(T.DoubleType()))))
                       .withColumn('cov_plan_cov_co_payment', _zero_guard(F.lit(NON_REQ_NULL_DEC)))
                       .withColumn('cov_plan_cov_deductible_amount', _zero_guard(F.lit(NON_REQ_NULL_DEC)))
                       .withColumn('cov_plan_cov_deductible_detail', F.lit(NON_REQ_NULL_STR) )
                       # 2026-06-02: number_of_day = daysvis; if null -> visyear; else 0
                       # .withColumn('cov_plan_cov_number_of_day', F.coalesce(F.col("bnf.daysvis"), F.lit(NON_REQ_NULL_INT)).cast(T.IntegerType()))   # OLD
                       .withColumn('cov_plan_cov_number_of_day', F.coalesce(F.col("bnf.daysvis"), F.col("bnf.visyear"), F.lit(0)).cast(T.IntegerType()))
                       .withColumn('cov_plan_cov_cover_year', F.lit(1))
                       .withColumn('cov_plan_cov_payment_year', F.lit(1))
                       .withColumn('cov_plan_cov_claim_status', F.lit(NON_REQ_NULL_STR).cast(T.StringType()))
                       .groupBy("bnh.chdrnum", "max_planno", "bnh.prodtyp", "gxh_sum.mbrno")
                       .agg(
                           F.array_distinct(
                           F.collect_list(
                               F.struct(
                                   F.col("cov_plan_cov_code").alias("cov_plan_cov_code"),
                                   F.col("cov_plan_cov_detail").alias("cov_plan_cov_detail"),
                                   F.col("cov_plan_cov_amt").alias("cov_plan_cov_amt"),
                                   F.col("cov_plan_cov_sum_insured_per_coverage_per_year").alias("cov_plan_cov_sum_insured_per_coverage_per_year"),
                                   F.col("cov_plan_cov_sum_insured_per_coverage_per_time").alias("cov_plan_cov_sum_insured_per_coverage_per_time"),
                                   F.col("cov_plan_cov_co_payment").alias("cov_plan_cov_co_payment"),
                                   F.col("cov_plan_cov_deductible_amount").alias("cov_plan_cov_deductible_amount"),
                                   F.col("cov_plan_cov_deductible_detail").alias("cov_plan_cov_deductible_detail"),
                                   F.col("cov_plan_cov_number_of_day").alias("cov_plan_cov_number_of_day"),
                                   F.col("cov_plan_cov_cover_year").alias("cov_plan_cov_cover_year"),
                                   F.col("cov_plan_cov_payment_year").alias("cov_plan_cov_payment_year"),
                                   F.col("cov_plan_cov_claim_status").alias("cov_plan_cov_claim_status")
                               )
                           )).alias("coverages_under_plans")
                       )
                      )
    
    #build Policy Coverage Plan Sequence
    cov_plan_output = (df_gxhipf_source.distinct().alias('gxh')
                       .join(df_gmhdpf_source.select('chdrnum','mbrno','dpntno').distinct().alias('gmh'),
                            (F.trim(F.col('gxh.chdrnum')) == F.trim(F.col('gmh.chdrnum'))) &
                            (F.trim(F.col('gxh.mbrno')) == F.trim(F.col('gmh.mbrno'))) &
                            (F.trim(F.col('gxh.dpntno')) == F.trim(F.col('gmh.dpntno'))),
                             'inner'
                           )
                       .join(df_gchipf_latest.select('chdrnum','ccdate').distinct().alias('gchdate'),
                                 (F.trim(F.col('gxh.chdrnum')) == F.trim(F.col('gchdate.chdrnum'))),
                                 'left'
                               )
                       .select('gxh.chdrnum','gxh.mbrno','gxh.planno','gxh.effdate','prodtyp','suminsu')
                       .where((F.col('gxh.effdate') <= F.lit(end_date))).groupBy('gxh.chdrnum','gxh.planno','prodtyp','gxh.mbrno')
                       .agg(F.max(F.struct(F.col('gxh.effdate'),F.col('suminsu'))).alias('tmp'))
                       .select('gxh.chdrnum','gxh.planno','prodtyp','gxh.mbrno',F.col('tmp.effdate').alias('max_effdate'),F.col('tmp.suminsu'))
                       .join(df_ref_group_product_type_lserff.select('descitem','longdesc').distinct().alias('ref_prodtype'), F.trim(F.col('prodtyp')) == F.trim(F.col('ref_prodtype.descitem')),'inner')
                     )
    cov_plan_output = (cov_plan_output.alias('cov_1')
                       .join(cov_prod_output.alias('cov_2'),
                             (F.trim(F.col('cov_1.chdrnum')) == F.trim(F.col('cov_2.chdrnum'))) &
                             (F.trim(F.col('cov_1.mbrno')) == F.trim(F.col('cov_2.mbrno'))) &
                             (F.trim(F.col('cov_1.planno').cast('int')) == F.trim(F.col('cov_2.max_planno'))) &
                             (F.trim(F.col('cov_1.prodtyp')) == F.trim(F.col('cov_2.prodtyp'))),
                              'inner'
                            )
                       # .withColumn('sub_pol_id',F.concat_ws("_", F.col("cov_1.chdrnum"), F.lpad(F.col("mbrno").cast("string"), 5, "0")))
                       .withColumn('sub_pol_id_seq', 
                                  F.row_number().over(Window.partitionBy(F.col('cov_1.chdrnum'),F.col('cov_1.mbrno')).orderBy(F.col('cov_1.prodtyp')))
                                 )
                       .withColumn('sub_pol_id',F.concat_ws("_", F.col("cov_1.chdrnum"), F.lpad(F.col("cov_1.mbrno").cast("string"), 5, "0"), F.col('sub_pol_id_seq'))
                       )
                       .withColumn('cov_plan_seq', 
                                   F.row_number().over(
                                    Window.partitionBy(F.col('cov_1.chdrnum'),F.col('cov_1.mbrno')).orderBy(F.col('cov_1.prodtyp'))
                                    )
                                  )
                       .withColumn('cov_plan_name', F.col("longdesc"))
                       .withColumn('cov_plan_sum_insured', _zero_guard(F.col("suminsu").cast(T.DoubleType())))
                       .withColumn('cov_plan_maturity_benefit', _zero_guard(F.lit(0.00)))
                       .withColumn('cov_plan_survival_benefit_min', _zero_guard(F.lit(0.00)))
                       .withColumn('cov_plan_survival_benefit_max', _zero_guard(F.lit(0.00)))
                       .withColumn('coverages_under_plans', F.col("coverages_under_plans"))
                       .groupBy("cov_1.chdrnum", "cov_1.mbrno", "cov_1.planno", "cov_1.prodtyp", "sub_pol_id")
                       .agg(
                           F.collect_list(
                               F.struct(
                                   F.col("cov_plan_seq").alias("cov_plan_seq"),
                                   F.col("cov_plan_name").alias("cov_plan_name"),
                                   F.col("cov_plan_sum_insured").alias("cov_plan_sum_insured"),
                                   F.col("cov_plan_maturity_benefit").alias("cov_plan_maturity_benefit"),
                                   F.col("cov_plan_survival_benefit_min").alias("cov_plan_survival_benefit_min"),
                                   F.col("cov_plan_survival_benefit_max").alias("cov_plan_survival_benefit_max"),
                                   F.col("coverages_under_plans").alias("coverages_under_plans"),
                               )
                           ).alias("policy_cov_plan_seqs")
                       )
                    )

    # add for prodtype
    df_member_with_client_prodtype = df_member_with_client.join(df_bnfhpf_source.select('chdrnum','prodtyp','planno').distinct().alias('bnf'),
                                             (F.trim(F.col('pol.chdrnum')) == F.trim(F.col('bnf.chdrnum'))) & (F.col('pln.max_planno') == F.col('bnf.planno')),
                                             'left')

    df_member_with_client_prodtype = df_member_with_client_prodtype.join(
            df_ref_group_product_type_lserff.alias('ref_prodtype'),
            F.trim(F.col('bnf.prodtyp')) == F.trim(F.col('ref_prodtype.descitem')),
            'left'
        )

    # #build new sub pol
    sub_pol_output = (df_member_with_client_prodtype.select('pol.chdrnum','ref_prodtype.lserff_code','gchi.ccdate','gchi.crdate','prodtyp','planno').distinct()
                          .join(df_gmhdpf_source.select('chdrnum','mbrno').distinct().alias('gmh'),
                                (F.trim(F.col('pol.chdrnum')) == F.trim(F.col('gmh.chdrnum')))
                               )
                         )

    sub_pol_output = (sub_pol_output.select('pol.chdrnum','mbrno','ref_prodtype.lserff_code','gchi.ccdate','gchi.crdate','prodtyp','planno').distinct().alias('sub_pol')
                      .join(cov_plan_output.alias('cov_1'),
                             # (F.trim(F.col('sub_pol.sub_pol_id')) == F.trim(F.col('cov_1.sub_pol_id')))
                              (F.trim(F.col('sub_pol.chdrnum')) == F.trim(F.col('cov_1.chdrnum')))
                             &(F.trim(F.col('sub_pol.mbrno')) == F.trim(F.col('cov_1.mbrno')))
                             &(F.trim(F.col('sub_pol.prodtyp')) == F.trim(F.col('cov_1.prodtyp')))
                             &(F.col('sub_pol.planno') == (F.col('cov_1.planno')))
                            ,'inner'
                            )
                        .withColumn('sub_pol_id_seq', 
                                    F.row_number().over(Window.partitionBy(F.col('sub_pol.chdrnum'),F.col('sub_pol.mbrno')).orderBy(F.col('cov_1.prodtyp')))
                                   )
                         .withColumn('sub_pol_id',F.concat_ws("_", F.col("sub_pol.chdrnum"), F.lpad(F.col("sub_pol.mbrno").cast("string"), 5, "0"), F.col('sub_pol_id_seq'))
                         )
                        .withColumn('sub_pol_type', F.lit("A"))
                        .withColumn('sub_pol_ppt', F.col("lserff_code"))
                        .withColumn('sub_pol_start_date', _to_iso_tz7_from_yyyymmdd("sub_pol.ccdate"))
                        .withColumn('sub_pol_end_date', _to_iso_tz7_from_yyyymmdd("sub_pol.crdate"))
                        .withColumn('sub_pol_sum_insured_per_year_exclude', _zero_guard(F.lit(0.00)))
                        .withColumn('sub_pol_sum_insured_per_year_include', _zero_guard(F.lit(0.00)))
                        .withColumn('policy_cov_plan_seqs', F.col("policy_cov_plan_seqs"))
                        .withColumn('coverages', F.array([]))
                        .groupBy("sub_pol.chdrnum", "sub_pol.mbrno")
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

    middle_columns = [
          'pol.chdrnum','mbr.mbrno','pol_refer_code_of_company', 'pol_transaction_status', 'pol_company_id', 'pol_id',
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
          'customer'
      ]
    output_policy_df = output_policy_df.select(middle_columns).distinct()

    output_policy_df = output_policy_df.join(sub_pol_output.select("chdrnum","mbrno","sub_policies"), 
                                            (F.trim(F.col('pol.chdrnum')) == F.trim(F.col('sub_pol.chdrnum'))) & 
                                            (F.trim(F.col('mbr.mbrno')) == F.trim(F.col('sub_pol.mbrno'))), how="inner").withColumn("sub_policies", F.col("sub_policies"))

    # =====================================================
    # SECTION 8: Build Final Payload per MEMBER
    # =====================================================
    print('Building output payload (per member)...')

    output_policy_df = output_policy_df.join(
        df_insured_per_member.alias('ins'),
        (F.col('pol.chdrnum') == F.col('ins.chdrnum')) &
        (F.col('mbr.mbrno')   == F.col('ins.mbrno')),
        'inner'
    )

    # create insrd_pol_sub_ids_expr
    insrd_pol_sub_ids_expr = F.when(
            F.col('sub_policies').isNotNull(),
            F.expr("""
              flatten(
                transform(
                  sub_policies,
                  sp -> transform(
                         sp.policy_cov_plan_seqs,
                         p -> concat(sp.sub_pol_id, '|', p.cov_plan_seq)
                  )
                )
              )
            """)
        ).otherwise(F.array())

    output_policy_df = (
            output_policy_df
            .withColumn('insured_struct_updated',
                F.col('ins.insured_struct').withField('insrd_pol_sub_ids', insrd_pol_sub_ids_expr)
            )
            .withColumn('insureds', F.array(F.col('insured_struct_updated')))
            .withColumn('sub_policies', F.col('sub_policies'))
            .withColumn('beneficiaries', F.lit(None).cast(T.ArrayType(T.StringType())))
            .withColumn('endorsements', F.lit(None).cast(T.ArrayType(T.StringType())))
            .withColumn('loans', F.lit(None).cast(T.ArrayType(T.StringType())))
            .withColumn('loan_payments', F.lit(None).cast(T.ArrayType(T.StringType())))
            .withColumn('investments', F.lit(None).cast(T.ArrayType(T.StringType())))
        )

    # Final select / dedup member
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

    output_policy_df = (output_policy_df.select(final_columns))

    count_val = output_policy_df.count()
    print(f'Group Standard Insured only payloads completed. Record count: {count_val}')

    return output_policy_df