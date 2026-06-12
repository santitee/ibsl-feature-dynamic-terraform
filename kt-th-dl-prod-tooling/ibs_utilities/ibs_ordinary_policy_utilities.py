'''
job name     : IBS Ordinary Policy Utilities module.
Created by   : Natchanon Thepysuwan
Created date : 8 Aug 2025
version      : 4.0
Modified by  : Santi Teeragul
Last update  : 23 Dec 2025
'''

from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.window import *


def write_start_date_ref_table(
    spark_session,
    source_table: str = "processing_ibs.ibs_lfkludta_lfppml",
    s3_path: str = "s3://kt-th-dl-prod-ibs/data/spec4/aggregation/start_date_ref_table",
    workflow_run_date: str = None,  # YYYYMMDD; if None, use Asia/Bangkok local date
    header: bool = False,
    mode: str = "append",
    coalesce_to_one: bool = True,
):
    # 1) Load source table
    try:
        df = spark_session.table(source_table)
    except Exception as e:
        raise RuntimeError(f"Failed to read table '{source_table}': {e}")

    # Validate required columns
    required_cols = {"pdoi", "pdot"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Source table missing required columns: {missing}")

    # 2) Aggregate max values
    row = df.agg(
        F.max(F.col("pdoi")).alias("max_pdoi"),
        F.max(F.col("pdot")).alias("max_pdot")
    ).collect()[0]

    max_pdoi = row["max_pdoi"]
    max_pdot = row["max_pdot"]

    print(f"[INFO] max_pdoi={max_pdoi}, max_pdot={max_pdot}")

    # 3) Determine policy transaction date with null safety
    if max_pdoi is None and max_pdot is None:
        raise ValueError("Both max pdoi and max pdot are NULL; cannot determine policy transaction date.")
    if max_pdoi is None:
        policy_transaction_date = max_pdot
    elif max_pdot is None:
        policy_transaction_date = max_pdoi
    else:
        # Your original logic is effectively min(max_pdoi, max_pdot)
        policy_transaction_date = min(max_pdoi, max_pdot)

    print(f"[INFO] policy_transaction_date={policy_transaction_date}")

    # 4) Determine workflow_run_date (YYYYMMDD)
    if workflow_run_date is None:
        current_date_str = datetime.now() + timedelta(hours=7)
        current_date_str = current_date_str.strftime("%Y%m%d")
    else:
        if not (isinstance(workflow_run_date, str) and len(workflow_run_date) == 8 and workflow_run_date.isdigit()):
            raise ValueError("workflow_run_date must be a YYYYMMDD string.")
        current_date_str = workflow_run_date

    # Ensure policy_transaction_date is string for consistent CSV output
    if hasattr(policy_transaction_date, "strftime"):
        policy_transaction_date_str = policy_transaction_date.strftime("%Y-%m-%d")
    else:
        policy_transaction_date_str = str(policy_transaction_date)

    # 5) Create single-row DataFrame
    df_out = spark_session.createDataFrame(
        [(current_date_str, policy_transaction_date_str)],
        schema=["workflow_run_date", "policy_transaction_date"]
    )

    print("[INFO] Row to write:")
    df_out.show(truncate=False)

    # 6) Write CSV to S3
    to_write = df_out.coalesce(1) if coalesce_to_one else df_out
    try:
        to_write.write.mode(mode).option("header", str(header).lower()).csv(s3_path)
    except Exception as e:
        raise RuntimeError(f"Failed to write CSV to '{s3_path}': {e}")

    print(f"[INFO] Wrote CSV to {s3_path} (mode={mode}, header={header}, coalesce_to_one={coalesce_to_one})")
    print(f"Write start_date_ref_table successfully!!!")
    return df_out

def ordinary_policy_aggregation(run_params, spark_session, start_date, end_date):
    print('Reading data source transactional tables...')
    if(run_params.workflow_type == 'Daily') :
        print('Reading data source transactional tables...')
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')\
                .filter(((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                        ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date)))
        

    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')\
                .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                        ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) &
                        (F.col('pstu').isin('1','2','5','6','B','F'))\
                    )
    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')\
                .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                        ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) &
                        (~F.col('pstu').isin('1','2','5','6','B','F'))\
                    )

    df_lfkludta_lfpappus_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpappus')
    df_lfkludta_lfpappus_source = df_lfkludta_lfpappus_source\
            .join(df_lfkludta_lfppml_source, df_lfkludta_lfpappus_source['uspno']==df_lfkludta_lfppml_source['pno'], 'left') \
            .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                    ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) \
                )\
            .select(df_lfkludta_lfpappus_source['*'])

    df_tblcustomer_namemasterbypno_source = spark_session.table(
        'processing_ibs.ibs_tblcustomer_namemasterbypno')
    df_tblcustomer_namemasterbypno_source = df_tblcustomer_namemasterbypno_source\
            .join(df_lfkludta_lfppml_source, df_tblcustomer_namemasterbypno_source['pno']==df_lfkludta_lfppml_source['pno'], 'leftsemi')\
            .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                    ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) \
                )    

    df_tblcustomer_namemasterbypno_owner_source = spark_session.table(
        'processing_ibs.ibs_tblcustomer_namemasterbypno_owner')
    df_tblcustomer_namemasterbypno_owner_source = df_tblcustomer_namemasterbypno_owner_source\
            .join(df_lfkludta_lfppml_source, df_tblcustomer_namemasterbypno_owner_source['pno']==df_lfkludta_lfppml_source['pno'], 'leftsemi')\
            .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                    ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) \
                )

    df_lfkludta_lfppolus_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfppolus')
    df_lfkludta_lfppolus_source = df_lfkludta_lfppolus_source\
            .join(df_lfkludta_lfppml_source, df_lfkludta_lfppolus_source['uspno']==df_lfkludta_lfppml_source['pno'], 'left') \
            .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                    ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) \
                )\
            .select(df_lfkludta_lfppolus_source['*'])

    df_lfkludta_lfpclm_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpclm')\
        .select('clpno', 'clstu', 'cllter', 'cltype')\
        .filter((F.col('cldot') >= start_date) & (F.col('cldot') < end_date))

    df_ref_major_claim_status = spark_session.table(
            'processing_ibs.ibs_ref_policy_claim_status_major_claim')

    df_ref_claim_coverage = spark_session.table(
            'processing_ibs.ibs_ref_policy_claim_coverage')
        
    df_ref_coverage_md = spark_session.table(
    "processing_ibs.ibs_ref_coverage_md")

    df_ref_major_claim_status = df_ref_major_claim_status.withColumnRenamed('clstu', 'ref_clstu') \
                                                            .withColumnRenamed('description', 'ref_claim_description') \
                                                            .withColumnRenamed('oic_code', 'oic_code_claimsts')

    df_ref_claim_coverage = df_ref_claim_coverage.withColumnRenamed('cltype', 'ref_cltype') \
                                                    .withColumnRenamed('cl_type_description', 'ref_cl_type_description') \
                                                    .withColumnRenamed('oic_code', 'oic_code_claimcoverage')\
                                                    
    df_ref_claim_coverage = df_ref_claim_coverage.join(df_ref_coverage_md, df_ref_claim_coverage.oic_code_claimcoverage == df_ref_coverage_md.oic_cov_code, 'left')  
    df_lfkludta_lfpclm_source = df_lfkludta_lfpclm_source \
                                    .join(df_ref_major_claim_status,
                                        df_lfkludta_lfpclm_source.clstu == df_ref_major_claim_status.ref_clstu, 'left') \
                                    .withColumn("cltype_join_key", F.when(F.col("cltype").like("HP%"), "HP%").otherwise(F.col("cltype")))\
                                    
    df_lfkludta_lfpclm_source = df_lfkludta_lfpclm_source.join(df_ref_claim_coverage,
                                F.trim(df_lfkludta_lfpclm_source.cltype_join_key) == F.trim(df_ref_claim_coverage.ref_cltype), 'left')

    df_claim_header_source = spark_session.table(
    'processing_ibs.ibs_claim_header')\
    .filter((F.col('notifydate') >= start_date) & (F.col('notifydate') < end_date))
    df_claim_status_source = spark_session.table(
        'processing_ibs.ibs_claim_status')
    df_claim_header_source = df_claim_header_source \
                            .join(df_claim_status_source,
                                df_claim_header_source.clm_cstatus == df_claim_status_source.claimsts, "left") \

    window_spec_hcs = Window.partitionBy("pol_no", "coverplancode").orderBy(col("notifydate").desc())
    df_claim_header_source = df_claim_header_source.withColumn("row_num", F.row_number().over(window_spec_hcs)) \
    .filter(col("row_num") == 1) \
    .select("pol_no", "coverplancode")

    df_lfkludta_lfpucom4_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpucom4')
    df_lfkludta_lfpucom4_source = df_lfkludta_lfpucom4_source\
            .join(df_lfkludta_lfppml_source, df_lfkludta_lfpucom4_source['cpno']==df_lfkludta_lfppml_source['pno'], 'left') \
            .join(df_lfkludta_lfpclm_source, df_lfkludta_lfpucom4_source['cpno']==df_lfkludta_lfpclm_source['clpno'], 'left') \
            .join(df_claim_header_source, 
            (df_lfkludta_lfpucom4_source['cpno']==df_claim_header_source['pol_no']) & 
            (df_lfkludta_lfpucom4_source['cplan']==df_claim_header_source['coverplancode'])
            , 'left') \
            .filter((((F.col('pdot') >= start_date) & (F.col('pdot') < end_date)) |
                    ((F.col('pdoi') >= start_date) & (F.col('pdoi') < end_date))) \
                )\
            .select(df_lfkludta_lfpucom4_source['*'], df_lfkludta_lfpclm_source['*'])

    df_lfkludta_lfpbnfyinf_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfpbnfyinf')\
                .filter((F.col('g07ramtl') >= start_date) & (F.col('g07ramtl') < end_date))
    df_lfkluhst_lfpbnfyh_source = spark_session.table(
        'processing_ibs.ibs_lfkluhst_lfpbnfyh')\
            .filter((F.col('bnupd') >= start_date) & (F.col('bnupd') < end_date))
    df_lfkludta_lfpbnfy_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpbnfy')\
            .filter((F.col('bnupd') >= start_date) & (F.col('bnupd') < end_date))
    df_lfkludta_lfpchgh_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpchgh')\
            .filter((F.col('cdot') >= start_date) & (F.col('cdot') < end_date))
    df_lfkludta_lfpchg_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpchg')\
            .filter((F.col('cdot') >= start_date) & (F.col('cdot') < end_date))
    df_lfkluhst_lfpacth_initial_source = spark_session.table(
        'processing_ibs.ibs_lfkluhst_lfpacth_initial')\
            .filter((F.col('acdot') >= start_date) & (F.col('acdot') < end_date))
    df_lfkludta_lfpact1_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpact1')\
            .filter((F.col('acdot') >= start_date) & (F.col('acdot') < end_date))
    df_lfkludta_lfpact_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpact')\
            .filter((F.col('acdot') >= start_date) & (F.col('acdot') < end_date))
    df_lfkludta_lfpuftdh_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpuftdh')\
            .filter((F.col('m2tdat') >= start_date) & (F.col('m2tdat') < end_date))
    df_lfkludta_lfpuftd_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpuftd')\
            .filter((F.col('n9tdat') >= start_date) & (F.col('n9tdat') < end_date))
    df_lfkludta_lfpcdbfbi_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpcdbfbi')\
            .filter((F.col('q47lupdt') >= start_date) & (F.col('q47lupdt') < end_date))
    print('Reading data source master tables...')
    df_lfklutab_lfpdvdg_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_lfpdvdg')
    df_lfklutab_lfpdvdh_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_lfpdvdh')
    df_dimproduct_basic_source = spark_session.table(
        'processing_ibs.ibs_dimproduct_basic')
    df_lfklutab_tabplan_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_tabplan')
    df_tblagent_lbprofilemovement_source = spark_session.table(
        'processing_ibs.ibs_tblagent_lbprofilemovement')
    df_tblagent_license_source = spark_session.table(
        'processing_ibs.ibs_tblagent_license')
    df_tblagent_pnomapbranch_source = spark_session.table(
        'processing_ibs.ibs_tblagent_pnomapbranch')
    df_lfklutab_lfpdvdg_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_lfpdvdg')
    df_lfklutab_lfpdvdh_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_lfpdvdh')
    df_benefit_master_source = spark_session.table(
        'processing_ibs.ibs_benefit_master')
    df_insured_details_source = spark_session.table(
        'processing_ibs.ibs_insured_details')
    df_t_fund_master_source = spark_session.table(
        'processing_ibs.ibs_t_fund_master')
    df_tblcompany_nameprefix_source = spark_session.table(
        'processing_ibs.ibs_tblcompany_nameprefix')
    df_t_policy_address_source = spark_session.table(
        'processing_ibs.ibs_t_policy_address')
    print('Reading ref tables...')
    df_ref_policy_benefit_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_benefit')
    df_ref_coverage_health_source = spark_session.table(
    'processing_ibs.ibs_ref_coverage_health')
    df_ref_policy_cim_user_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_cim_user')
    df_ref_policy_claim_coverage_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_coverage')
    df_ref_policy_claim_payment_channel_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_payment_channel')
    df_ref_policy_claim_status_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_status')
    df_ref_policy_comp_channel_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_comp_channel')
    df_ref_policy_comp_seller_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_comp_seller')
    df_ref_policy_comp_status_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_comp_status')
    df_ref_policy_comp_toppic_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_comp_toppic')
    df_ref_policy_coverage_code_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_coverage_code')
    df_ref_policy_policy_product_type_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_policy_product_type')
    df_ref_policy_service_md_ref_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_service_md_ref')
    df_ref_location_code_source = spark_session.table(
        'processing_ibs.ibs_ref_location_code')
    df_ref_hospital_code_source = spark_session.table(
        'processing_ibs.ibs_ref_hospital_code')
    df_ref_ordinary_pol_sub_type_lserff = spark_session.table(
        'processing_ibs.ibs_ref_ordinary_pol_sub_type_lserff')
    df_ref_coverage_md = spark_session.table(
    "processing_ibs.ibs_ref_coverage_md")

    print('Declaring Default Params Value...')
    pol_refer_code_of_company_prefix = "POL_KAL_"
    pol_transaction_status = "N"
    pol_company_id = "1022"
    pol_type = "PT-05"
    cust_country_code = "THA"
    insrd_pol_sub_id = 'A'
    insrd_seq = '1'
    cov_plan_seq = '1'

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = None

    NON_REQ_NULL_STR = None
    NON_REQ_NULL_NUM = None
    NON_REQ_NULL_DATE = None

    print('Declaring Dimproduct Basic by map with ref table...')
    df_dimproduct_basic_source_map_ref = df_dimproduct_basic_source \
                                        .join(df_ref_policy_policy_product_type_source,F.trim(df_dimproduct_basic_source['actuarial_type_cate']) ==  F.trim(df_ref_policy_policy_product_type_source['kt_axa']), "left")\
                                        .join(df_lfklutab_tabplan_source,F.trim(df_dimproduct_basic_source['lplan']) ==  F.trim(df_lfklutab_tabplan_source['planc']),"left")\
                                        .withColumnRenamed('kt_axa', 'policy_product_type_ktaxa_code')\
                                        .withColumnRenamed('oic_code', 'policy_product_type_oic_code')\
                                        .withColumn('policy_type2_oic_code', F.when(df_lfklutab_tabplan_source['pland'].like('%ชนิดมีเงินปันผล%'), 'PL')\
                                                                            .otherwise('NP'))

    ##interim solution only mockup data -> in the permanent need to be current data.
    # df_lfkludta_lfpappus_source = df_lfkludta_lfpappus_source.drop(
    #     'yyyy').drop('mm').distinct()

    print('Declaring grouping lfkludta_lfpappus table...')
    df_lfkludta_lfpappus_source_group = df_lfkludta_lfpappus_source.groupBy(
        'uspno', 'usdapp', 'usocl', 'usoocl').count()

    print('Declaring Policy Status by map with ref table...')
    df_ref_policy_service_md_ref_policy_status = df_ref_policy_service_md_ref_source\
                                                    .filter(F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_status')\
                                                    .withColumnRenamed('ktaxa_code', 'policy_status_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'policy_status_oic_code')

    print('Declaring Policy Distribution by map with ref table...')
    df_ref_policy_service_md_ref_policy_distribution = df_ref_policy_service_md_ref_source\
                                                    .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_distribution') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                    .withColumnRenamed('ktaxa_code', 'policy_distribution_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'policy_distribution_oic_code')

    print('Declaring Customer Gender by map with ref table...')
    df_ref_policy_service_md_ref_customer_gender = df_ref_policy_service_md_ref_source\
                                                    .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'customer_gender') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                    .withColumnRenamed('ktaxa_code', 'customer_gender_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'customer_gender_oic_code')

    print('Declaring Policy Beneficiary Relation by map with ref table...')
    df_ref_policy_service_md_ref_pol_beneficiary_relation = df_ref_policy_service_md_ref_source\
                                                            .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'pol_beneficiary_relation') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                            .withColumnRenamed('ktaxa_code', 'pol_beneficiary_relation_ktaxa_code')\
                                                            .withColumnRenamed('id_code', 'pol_beneficiary_relation_oic_code')


    print('Declaring Policy by map with Branch table...')

    df_tblagent_pnomapbranch_map_movement = df_tblagent_pnomapbranch_source.join(df_tblagent_lbprofilemovement_source\
                                                                            ,F.trim(df_tblagent_pnomapbranch_source['lb']) ==  F.trim(df_tblagent_lbprofilemovement_source['lbid']),"inner")\

    df_lfkludta_lfppml_map_branch = df_lfkludta_lfppml_source\
                                    .join(df_tblagent_pnomapbranch_map_movement\
                                        , df_lfkludta_lfppml_source['pno'] == df_tblagent_pnomapbranch_map_movement['pno']\
                                        , "inner")\
                                    .join(df_lfkludta_lfpappus_source\
                                        ,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfpappus_source['uspno']\
                                        ),"inner")\
                                    .filter(df_lfkludta_lfpappus_source['usdapp'].between(df_tblagent_pnomapbranch_map_movement['lbissd']\
                                                                                        ,df_tblagent_pnomapbranch_map_movement['lbexpd'])\
                                        )


    df_lfkludta_lfppml_lb_license = df_lfkludta_lfppml_map_branch.groupBy(df_lfkludta_lfppml_source['pno'].alias('lb_pno')\
                                                                    , 'lblicn').count()\

    print('Remove Duplicated policy in dataframe df_lfkludta_lfppolus_source')
    df_lfkludta_lfppolus_source = df_lfkludta_lfppolus_source.select('uspno','usctz').dropDuplicates()

    ### joined with output_policy_df then returned cartesian -> hard code lilicn
    # df_lfkludta_lfppml_map_branch.show()

    df_lfkludta_lfpchgh_union = df_lfkludta_lfpchg_source.union(df_lfkludta_lfpchgh_source)\
                                                    .withColumnRenamed('pno', 'pno_chg')\
                                                    .select(['pno_chg','ctc', 'ctstu' ,'cdot'])\
                                                    .filter(F.col('ctc')=='4')\
                                                    .orderBy('cdot')

    df_lfkludta_lfpchgh_union = df_lfkludta_lfpchgh_union.groupBy(['pno_chg','ctc', 'ctstu'])\
                                                    .agg(F.max('cdot').alias('max_cdot'))

    df_lfkludta_lfpact_expropriation_value = df_lfkludta_lfpact_source.filter((F.col("actcd") == 'SUR') & (F.col("acsur") < 0)) \
        .groupBy("acpno") \
        .agg(F.abs(F.sum("acsur")).alias("positive_sum_acsur"))

    df_lfkludta_lfpact1_auto_premium_loan = df_lfkludta_lfpact1_source.filter((F.col("actcd") == 'LA')).dropDuplicates()
    window_spec_pact1 = Window.partitionBy("actcd").orderBy(F.col("aceff").desc())
    df_lfkludta_lfpact1_auto_premium_loan = df_lfkludta_lfpact1_auto_premium_loan.withColumn("rank", F.row_number().over(window_spec_pact1))
    df_lfkludta_lfpact1_auto_premium_loan = df_lfkludta_lfpact1_auto_premium_loan.filter(F.col("rank") == 1).drop("rank")

    print('Aggregating Output Policy table...')

    output_policy_df = df_lfkludta_lfppml_source \
                    .join(df_dimproduct_basic_source_map_ref,F.trim(df_lfkludta_lfppml_source['lplan']) ==  F.trim(df_dimproduct_basic_source_map_ref['lplan']),"left")\
                    .join(df_lfkludta_lfpappus_source_group,F.trim(df_lfkludta_lfppml_source['pno'])    ==  F.trim(df_lfkludta_lfpappus_source_group['uspno']),"left")\
                    .join(df_lfkludta_lfpchgh_union, F.trim(df_lfkludta_lfppml_source['pno'])==F.trim(df_lfkludta_lfpchgh_union['pno_chg']), 'left')\
                        .join(df_lfkludta_lfpact_expropriation_value, F.trim(df_lfkludta_lfppml_source['pno'])==F.trim(df_lfkludta_lfpact_expropriation_value['acpno']), 'left')\
                        .join(df_lfkludta_lfpact1_auto_premium_loan, F.trim(df_lfkludta_lfppml_source['pno'])==F.trim(df_lfkludta_lfpact1_auto_premium_loan['acpno']), 'left')\
                    .join(df_lfkludta_lfppml_lb_license, F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfppml_lb_license['lb_pno']),"left")\
                    .join(df_tblagent_license_source, substring(F.trim(df_lfkludta_lfppml_source['ppagt1']), 8, 6).cast(T.IntegerType()) ==  F.trim(df_tblagent_license_source['lcagc']),"left")\
                    .join(df_ref_policy_service_md_ref_policy_distribution, substring(F.trim(df_lfkludta_lfppml_source['ppagt1']), 1, 1) ==  F.trim(df_ref_policy_service_md_ref_policy_distribution['policy_distribution_ktaxa_code']), "left")\
                    .join(df_ref_ordinary_pol_sub_type_lserff,F.trim(df_lfkludta_lfppml_source['lplan']) ==  F.trim(df_ref_ordinary_pol_sub_type_lserff['plan_code']),"left")\
                    .withColumn('pol_refer_code_of_company',concat(lit(pol_refer_code_of_company_prefix), F.trim(df_lfkludta_lfppml_source['pno'])).cast(T.StringType())) \
                    .withColumn('pol_transaction_status',lit(pol_transaction_status).cast(T.StringType())) \
                    .withColumn('pol_company_id',lit(pol_company_id).cast(T.StringType())) \
                    .withColumn('pol_id', F.trim(df_lfkludta_lfppml_source['pno']).cast(T.StringType())) \
                    .withColumn('pol_type', lit(pol_type).cast(T.StringType())) \
                    .withColumn('pol_type_gli_name', lit(NON_REQ_NULL_NUM).cast(T.IntegerType())) \
                    .withColumn('pol_type_gli_plan', lit(NON_REQ_NULL_NUM).cast(T.IntegerType())) \
                    .withColumn('pol_product_type', F.when(F.trim(df_dimproduct_basic_source_map_ref['policy_product_type_oic_code']) != ''\
                                                            ,F.trim(df_dimproduct_basic_source_map_ref['policy_product_type_oic_code'])\
                                                            )\
                                                        .otherwise(lit('PPT-20')).cast(T.StringType())\
                                ) \
                    .withColumn('pol_product_type_other_detail', F.when(F.trim(df_dimproduct_basic_source_map_ref['policy_product_type_oic_code']) == 'PPT-20'\
                                                                        , F.trim(df_dimproduct_basic_source_map_ref['grouplob'])).otherwise(lit(REQ_NULL_STR)).cast(T.StringType()))\
                    .withColumn('pol_type_2', F.trim(df_dimproduct_basic_source_map_ref['policy_type2_oic_code']).cast(T.StringType()))\
                    .withColumn('pol_sub_type', F.when(F.trim(df_ref_ordinary_pol_sub_type_lserff['plan_code']).isNotNull()\
                                                                    , F.trim(df_ref_ordinary_pol_sub_type_lserff['lserff_code']))\
                                            .otherwise(lit('1022S000000000')).cast(T.StringType()))\
                    .withColumn('pol_name', F.trim(df_dimproduct_basic_source_map_ref['pland']).cast(T.StringType())) \
                    .withColumn('pol_name_oic', F.trim(df_dimproduct_basic_source_map_ref['pland']).cast(T.StringType()))\
                    .withColumn('pol_start_date', concat(substring(F.trim(df_lfkludta_lfppml_source['peff']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['peff']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['peff']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_end_date',   concat(substring(F.trim(df_lfkludta_lfppml_source['pexp']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['pexp']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pexp']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_claim_document', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                    .withColumn('pol_payment_term', F.when((F.trim(df_lfkludta_lfppml_source['pptd']) > 0) & (F.trim(df_lfkludta_lfppml_source['pmprem']) > 0)\
                                                    , concat(lit('กำหนดการชำระเบี้ยครั้งถัดไป วันที่ ')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pptd']), 7, 2)\
                                                        , lit('/')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['pptd']), 5, 2)\
                                                        , lit('/')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pptd']), 1, 4)\
                                                        )).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()))\
                    .withColumn("pol_payment_amount", F.when(F.trim(df_lfkludta_lfppml_source['lplan']).rlike('SP'), F.trim(df_lfkludta_lfppml_source['liilp']).cast(T.DecimalType(18, 2)))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'M', (F.trim(df_lfkludta_lfppml_source['pmprem'])*12).cast(T.DecimalType(18, 2)))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'Q', (F.trim(df_lfkludta_lfppml_source['pmprem'])*4).cast(T.DecimalType(18, 2)))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'S', (F.trim(df_lfkludta_lfppml_source['pmprem'])*2).cast(T.DecimalType(18, 2)))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'A', F.trim(df_lfkludta_lfppml_source['pmprem']))\
                                                        .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18, 2))
                                )\
                    .withColumn("pol_payment_period", F.when(F.trim(df_lfkludta_lfppml_source['lplan']).rlike('SP'), lit('05'))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'M', lit('01'))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'Q', lit('02'))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'S', lit('03'))\
                                                        .when(F.trim(df_lfkludta_lfppml_source['pmode']) == 'A', lit('04'))\
                                                        .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                )\
                    .withColumn('pol_info_url', lit('https://www.krungthai-axa.co.th/customer-service/emma-by-axa').cast(T.StringType())) \
                    .withColumn('pol_claim_url',lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                    .withColumn('pol_document_urls',F.array()) \
                    .withColumn('pol_year',F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_lfkludta_lfppml_source['peff']))/10000+1).cast(T.IntegerType())) \
                    .withColumn('pol_status', 
                                F.when((F.trim(F.col('pstu')) == '1') & ((F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_lfkludta_lfppml_source['peff']))/10000+1).cast(T.StringType())) == 1) , '0100') \
                                .when((F.trim(F.col('pstu')) == '1') & ((F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_lfkludta_lfppml_source['peff']))/10000+1).cast(T.StringType())) > 1), '0200') \
                        .when((F.trim(F.col('pstu')) == '1') & (F.trim(F.col('ctc')) == '4'), '0201')\
                        .when((F.trim(F.col('pstu')) == 'F') & (F.trim(F.col('ctc')) == '4'), '0202')\
                        .when((F.trim(F.col('pstu')) == '1') & (F.col('lphofd') > 0) & (F.col('lphotd') == 0), '0203')\
                        .when(F.trim(F.col('pstu')) == '5', '0204')\
                        .when(F.trim(F.col('pstu')) == '6', '0206')\
                        .when(F.trim(F.col('pstu')) == 'B', '0208')\
                        .when((F.trim(F.col('pstu')) == '1') & 
                        ((F.col('lloan').isNotNull()) | (F.col('llni').isNotNull())) &
                        (F.trim(F.col('actcd')) == 'AW'), '0209')\
                        .when((F.trim(F.col('pstu')) == '1') & 
                        ((F.col('lloan').isNotNull()) | (F.col('llni').isNotNull())) &
                        (F.trim(F.col('actcd')) == 'LA'), '0210')\
                        .when(F.trim(F.col('pstu')) == '2', '0211')\
                        .when((F.trim(F.col('pstu')).isin('1', '2')) & (F.date_add(F.to_date(F.trim(F.col('pptd')), 'yyyyMMdd'), 46) < F.current_date()), '0211') \
                        .when(F.trim(df_lfkludta_lfppml_source['lplan']).isin('SPUL01', 'SPUL02'), '0211') \
                        .when((F.trim(F.col('pstu')) == '2') & (F.trim(F.col('plsgpf')) == 'Y'), '0211')\
                        .when((F.trim(df_lfkludta_lfppml_source['lplan'])) == 'IGROW', '0211')\
                        .when(F.trim(F.col('pstu')) == '3', '0300')\
                        .when(F.trim(F.col('pstu')) == 'C', '0400')\
                        .when(F.trim(F.col('pstu')) == 'Z', '0401')\
                        .when(F.trim(F.col('pstu')) == 'A', '0402')\
                        .when(F.trim(F.col('pstu')) == '8', '0500')\
                        .when(F.trim(F.col('pstu')) == '7', '0600')\
                        .when(F.trim(F.col('pstu')) == '9', '0601')\
                        .when(F.trim(F.col('pstu')) == '4', '0700')\
                        .otherwise(lit('9999')).cast(T.StringType())
                    )\
                    .withColumn('pol_status_note', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                    .withColumn('pol_status_date',   F.when(F.trim(df_lfkludta_lfppml_source['pdot']) != '0'\
                                                        ,concat(substring(F.trim(df_lfkludta_lfppml_source['pdot']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['pdot']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pdot']), 7, 2)\
                                                        , lit('T00:00:00+07:00')))\
                                                        .otherwise(concat(substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 1, 4)\
                                                            , lit('-')\
                                                            , substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 5, 2)\
                                                            , lit('-')\
                                                            ,substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 7, 2)\
                                                            , lit('T00:00:00+07:00'))).cast(T.StringType())\
                                ) \
                    .withColumn('pol_expropriation_value', 
                                    F.when(df_lfkludta_lfpact_expropriation_value['positive_sum_acsur'].isNotNull(), df_lfkludta_lfpact_expropriation_value['positive_sum_acsur'].cast(T.DecimalType(18,2)))\
                                    .otherwise(lit(REQ_NULL_DEC))\
                                    .cast(T.DecimalType(18,2))) \
                    .withColumn('pol_immediate_refund', lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)))\
                    .withColumn('pol_sale_date',   F.when((F.trim(df_lfkludta_lfpappus_source_group['usdapp']) != '0') &
                                                            (F.trim(df_lfkludta_lfpappus_source_group['usdapp']).isNotNull())\
                                                        ,concat(substring(F.trim(df_lfkludta_lfpappus_source_group['usdapp']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfpappus_source_group['usdapp']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfpappus_source_group['usdapp']), 7, 2)\
                                                        , lit('T00:00:00+07:00')))\
                                                        .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType())\
                                )\
                    .withColumn('pol_transaction_date',   concat(substring(F.trim(df_lfkludta_lfppml_source['pdos']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['pdos']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pdos']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_approved_date',   concat(substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_issue_date',   concat(substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['pdoi']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_account_date',   concat(substring(F.trim(df_lfkludta_lfppml_source['peff']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['peff']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['peff']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_agree_date',   concat(substring(F.trim(df_lfkludta_lfppml_source['peff']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_lfkludta_lfppml_source['peff']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_lfkludta_lfppml_source['peff']), 7, 2)\
                                                        , lit('T00:00:00+07:00')).cast(T.StringType())) \
                    .withColumn('pol_distribution', F.trim(df_ref_policy_service_md_ref_policy_distribution['policy_distribution_oic_code']).cast(T.StringType()))\
                    .withColumn('pol_distribution_other_detail', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                    .withColumn('pol_license', F.when((substring(F.trim(df_lfkludta_lfppml_source['ppagt1']), 1, 1) == '7') & \
                                                        (df_lfkludta_lfppml_lb_license['lblicn'].isNotNull()), df_lfkludta_lfppml_lb_license['lblicn'])\
                                                .when(df_tblagent_license_source['lcagc'].isNotNull(), df_tblagent_license_source['lcnbr'])\
                                                .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                .alias('pol_license'))\
                    .withColumn('pol_re_insured_contact_number', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                    .withColumn('pol_life_detail', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                    .withColumn('renewal', 
                            F.when((F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_lfkludta_lfppml_source['peff']))/10000+1).cast(T.IntegerType()) == 1),  F.lit('03'))
                            .when((F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_lfkludta_lfppml_source['peff']))/10000+1).cast(T.IntegerType()) > 1),  F.lit('01'))
                            )\
                    .withColumn('renewal_pol_id', df_lfkludta_lfppml_source['pno'])

    print('--Selecting only required columns...')
    output_policy_df = output_policy_df\
    .select('pol_refer_code_of_company'
    ,'pol_transaction_status'
    ,'pol_company_id'
    ,'pol_id'
    ,'pol_type'
    ,'pol_type_gli_name'
    ,'pol_type_gli_plan'
    ,'pol_product_type'
    ,'pol_product_type_other_detail'
    ,'pol_type_2'
    ,'pol_sub_type'
    ,'pol_name'
    ,'pol_name_oic'
    ,'pol_start_date'
    ,'pol_end_date'
    ,'pol_claim_document'
    ,'pol_payment_term'
    ,'pol_payment_amount'
    ,'pol_payment_period'
    ,'pol_info_url'
    ,'pol_claim_url'
    ,'pol_document_urls'
    ,'pol_year'
    ,'pol_status'
    ,'pol_status_note'
    ,'pol_status_date'
    ,'pol_expropriation_value'
    ,'pol_immediate_refund'
    ,'pol_sale_date'
    ,'pol_transaction_date'
    ,'pol_approved_date'
    ,'pol_issue_date'
    ,'pol_account_date'
    ,'pol_agree_date'
    ,'pol_distribution'
    ,'pol_distribution_other_detail'
    ,'pol_license'
    ,'pol_re_insured_contact_number'
    ,'pol_life_detail'
    ,'renewal'
    ,'renewal_pol_id'
    )

    print('--Starting Ordinary Policy Subpolicies...')
    print('----Merging Dividend to master table...')
    df_lfklutab_lfpdvd_merge = df_lfklutab_lfpdvdg_source.unionAll(
        df_lfklutab_lfpdvdh_source)
    df_lfklutab_lfpdvd_merge = df_lfklutab_lfpdvd_merge.withColumn(
        "dveff_old", df_lfklutab_lfpdvd_merge['dveff'])
    # df_lfklutab_lfpdvd_merge.show(100, False)

    df_lfklutab_lfpdvd_merge_max_eff = df_lfklutab_lfpdvd_merge\
                                    .groupBy("lplan", "dvage", "dvsex"\
                                            )\
                                    .agg(F.max(df_lfklutab_lfpdvd_merge['dveff'].cast(T.IntegerType())).alias("dveff_max")
                                        )\

    cond = [(F.trim(col("a.lplan")) ==  F.trim(col("b.lplan"))) &\
            (col("a.dvage").cast(T.IntegerType()) ==  col("b.dvage").cast(T.IntegerType())) &\
            (F.trim(col("a.dvsex")) ==  F.trim(col("b.dvsex"))) &\
            (col("a.dveff_max").cast(T.IntegerType()) ==  col("b.dveff_old").cast(T.IntegerType()))\
            ]

    df_lfklutab_lfpdvd_merge_master = df_lfklutab_lfpdvd_merge_max_eff.alias("a")\
                                    .join(df_lfklutab_lfpdvd_merge.alias("b")\
                                            ,cond\
                                            ,"inner")

    df_lfklutab_lfpdvd_merge_master =  df_lfklutab_lfpdvd_merge_master\
                                    .groupBy( df_lfklutab_lfpdvd_merge_max_eff['lplan']\
                                                ,df_lfklutab_lfpdvd_merge_max_eff['dvage']\
                                                ,df_lfklutab_lfpdvd_merge_max_eff['dvsex']\
                                                ,df_lfklutab_lfpdvd_merge_max_eff['dveff_max']\
                                            )\
                                    .agg(F.sum(df_lfklutab_lfpdvd_merge_master['dv001'].cast(T.DoubleType())).alias('sum_dv001')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv002'].cast(T.DoubleType())).alias('sum_dv002')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv003'].cast(T.DoubleType())).alias('sum_dv003')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv004'].cast(T.DoubleType())).alias('sum_dv004')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv005'].cast(T.DoubleType())).alias('sum_dv005')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv006'].cast(T.DoubleType())).alias('sum_dv006')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv007'].cast(T.DoubleType())).alias('sum_dv007')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv008'].cast(T.DoubleType())).alias('sum_dv008')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv009'].cast(T.DoubleType())).alias('sum_dv009')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv010'].cast(T.DoubleType())).alias('sum_dv010')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv011'].cast(T.DoubleType())).alias('sum_dv011')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv012'].cast(T.DoubleType())).alias('sum_dv012')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv013'].cast(T.DoubleType())).alias('sum_dv013')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv014'].cast(T.DoubleType())).alias('sum_dv014')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv015'].cast(T.DoubleType())).alias('sum_dv015')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv016'].cast(T.DoubleType())).alias('sum_dv016')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv017'].cast(T.DoubleType())).alias('sum_dv017')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv018'].cast(T.DoubleType())).alias('sum_dv018')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv019'].cast(T.DoubleType())).alias('sum_dv019')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv020'].cast(T.DoubleType())).alias('sum_dv020')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv021'].cast(T.DoubleType())).alias('sum_dv021')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv022'].cast(T.DoubleType())).alias('sum_dv022')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv023'].cast(T.DoubleType())).alias('sum_dv023')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv024'].cast(T.DoubleType())).alias('sum_dv024')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv025'].cast(T.DoubleType())).alias('sum_dv025')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv026'].cast(T.DoubleType())).alias('sum_dv026')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv027'].cast(T.DoubleType())).alias('sum_dv027')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv028'].cast(T.DoubleType())).alias('sum_dv028')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv029'].cast(T.DoubleType())).alias('sum_dv029')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv030'].cast(T.DoubleType())).alias('sum_dv030')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv031'].cast(T.DoubleType())).alias('sum_dv031')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv032'].cast(T.DoubleType())).alias('sum_dv032')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv033'].cast(T.DoubleType())).alias('sum_dv033')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv034'].cast(T.DoubleType())).alias('sum_dv034')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv035'].cast(T.DoubleType())).alias('sum_dv035')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv036'].cast(T.DoubleType())).alias('sum_dv036')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv037'].cast(T.DoubleType())).alias('sum_dv037')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv038'].cast(T.DoubleType())).alias('sum_dv038')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv039'].cast(T.DoubleType())).alias('sum_dv039')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv040'].cast(T.DoubleType())).alias('sum_dv040')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv041'].cast(T.DoubleType())).alias('sum_dv041')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv042'].cast(T.DoubleType())).alias('sum_dv042')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv043'].cast(T.DoubleType())).alias('sum_dv043')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv044'].cast(T.DoubleType())).alias('sum_dv044')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv045'].cast(T.DoubleType())).alias('sum_dv045')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv046'].cast(T.DoubleType())).alias('sum_dv046')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv047'].cast(T.DoubleType())).alias('sum_dv047')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv048'].cast(T.DoubleType())).alias('sum_dv048')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv049'].cast(T.DoubleType())).alias('sum_dv049')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv050'].cast(T.DoubleType())).alias('sum_dv050')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv051'].cast(T.DoubleType())).alias('sum_dv051')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv052'].cast(T.DoubleType())).alias('sum_dv052')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv053'].cast(T.DoubleType())).alias('sum_dv053')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv054'].cast(T.DoubleType())).alias('sum_dv054')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv055'].cast(T.DoubleType())).alias('sum_dv055')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv056'].cast(T.DoubleType())).alias('sum_dv056')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv057'].cast(T.DoubleType())).alias('sum_dv057')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv058'].cast(T.DoubleType())).alias('sum_dv058')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv059'].cast(T.DoubleType())).alias('sum_dv059')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv060'].cast(T.DoubleType())).alias('sum_dv060')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv061'].cast(T.DoubleType())).alias('sum_dv061')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv062'].cast(T.DoubleType())).alias('sum_dv062')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv063'].cast(T.DoubleType())).alias('sum_dv063')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv064'].cast(T.DoubleType())).alias('sum_dv064')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv065'].cast(T.DoubleType())).alias('sum_dv065')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv066'].cast(T.DoubleType())).alias('sum_dv066')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv067'].cast(T.DoubleType())).alias('sum_dv067')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv068'].cast(T.DoubleType())).alias('sum_dv068')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv069'].cast(T.DoubleType())).alias('sum_dv069')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv070'].cast(T.DoubleType())).alias('sum_dv070')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv071'].cast(T.DoubleType())).alias('sum_dv071')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv072'].cast(T.DoubleType())).alias('sum_dv072')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv073'].cast(T.DoubleType())).alias('sum_dv073')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv074'].cast(T.DoubleType())).alias('sum_dv074')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv075'].cast(T.DoubleType())).alias('sum_dv075')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv076'].cast(T.DoubleType())).alias('sum_dv076')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv077'].cast(T.DoubleType())).alias('sum_dv077')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv078'].cast(T.DoubleType())).alias('sum_dv078')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv079'].cast(T.DoubleType())).alias('sum_dv079')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv080'].cast(T.DoubleType())).alias('sum_dv080')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv081'].cast(T.DoubleType())).alias('sum_dv081')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv082'].cast(T.DoubleType())).alias('sum_dv082')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv083'].cast(T.DoubleType())).alias('sum_dv083')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv084'].cast(T.DoubleType())).alias('sum_dv084')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv085'].cast(T.DoubleType())).alias('sum_dv085')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv086'].cast(T.DoubleType())).alias('sum_dv086')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv087'].cast(T.DoubleType())).alias('sum_dv087')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv088'].cast(T.DoubleType())).alias('sum_dv088')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv089'].cast(T.DoubleType())).alias('sum_dv089')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv090'].cast(T.DoubleType())).alias('sum_dv090')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv091'].cast(T.DoubleType())).alias('sum_dv091')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv092'].cast(T.DoubleType())).alias('sum_dv092')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv093'].cast(T.DoubleType())).alias('sum_dv093')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv094'].cast(T.DoubleType())).alias('sum_dv094')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv095'].cast(T.DoubleType())).alias('sum_dv095')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv096'].cast(T.DoubleType())).alias('sum_dv096')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv097'].cast(T.DoubleType())).alias('sum_dv097')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv098'].cast(T.DoubleType())).alias('sum_dv098')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv099'].cast(T.DoubleType())).alias('sum_dv099')\
                                        ,F.sum(df_lfklutab_lfpdvd_merge_master['dv100'].cast(T.DoubleType())).alias('sum_dv100')\
                                        )\
                                    .orderBy(df_lfklutab_lfpdvd_merge_max_eff['lplan'], df_lfklutab_lfpdvd_merge_max_eff['dveff_max'])

    sum_dv_cols = [
        'sum_dv001', 'sum_dv002', 'sum_dv003', 'sum_dv004', 'sum_dv005',
        'sum_dv006', 'sum_dv007', 'sum_dv008', 'sum_dv009', 'sum_dv010',
        'sum_dv011', 'sum_dv012', 'sum_dv013', 'sum_dv014', 'sum_dv015',
        'sum_dv016', 'sum_dv017', 'sum_dv018', 'sum_dv019', 'sum_dv020',
        'sum_dv021', 'sum_dv022', 'sum_dv023', 'sum_dv024', 'sum_dv025',
        'sum_dv026', 'sum_dv027', 'sum_dv028', 'sum_dv029', 'sum_dv030',
        'sum_dv031', 'sum_dv032', 'sum_dv033', 'sum_dv034', 'sum_dv035',
        'sum_dv036', 'sum_dv037', 'sum_dv038', 'sum_dv039', 'sum_dv040',
        'sum_dv041', 'sum_dv042', 'sum_dv043', 'sum_dv044', 'sum_dv045',
        'sum_dv046', 'sum_dv047', 'sum_dv048', 'sum_dv049', 'sum_dv050',
        'sum_dv051', 'sum_dv052', 'sum_dv053', 'sum_dv054', 'sum_dv055',
        'sum_dv056', 'sum_dv057', 'sum_dv058', 'sum_dv059', 'sum_dv060',
        'sum_dv061', 'sum_dv062', 'sum_dv063', 'sum_dv064', 'sum_dv065',
        'sum_dv066', 'sum_dv067', 'sum_dv068', 'sum_dv069', 'sum_dv070',
        'sum_dv071', 'sum_dv072', 'sum_dv073', 'sum_dv074', 'sum_dv075',
        'sum_dv076', 'sum_dv077', 'sum_dv078', 'sum_dv079', 'sum_dv080',
        'sum_dv081', 'sum_dv082', 'sum_dv083', 'sum_dv084', 'sum_dv085',
        'sum_dv086', 'sum_dv087', 'sum_dv088', 'sum_dv089', 'sum_dv090',
        'sum_dv091', 'sum_dv092', 'sum_dv093', 'sum_dv094', 'sum_dv095',
        'sum_dv096', 'sum_dv097', 'sum_dv098', 'sum_dv099', 'sum_dv100'
    ]

    df_lfklutab_lfpdvd_merge_master = df_lfklutab_lfpdvd_merge_master.withColumn("min_dvd", F.least(*[\
                                                                                                    F.when(col(c).isNull() | (col(c) == 0), float("inf")).otherwise(col(c))\
                                                                                                    for c in sum_dv_cols\
                                                                                                    ]))\
                                                                    .withColumn('max_dvd', F.greatest(*sum_dv_cols))\
                                                                    .withColumn("year_dvd", F.least(*[\
                                                                                                    F.when((col(c) != 0), lit('ไม่มีเงินคืน')).otherwise(col(c))\
                                                                                                    for c in sum_dv_cols\
                                                                                                    ]))

    print('----Creating table insure benefit master...')

    df_benefit_master_source = df_benefit_master_source.filter(df_benefit_master_source['benefit_form_cd'] == 'IND')\
                                                .withColumnRenamed('coverage_cd', 'benefit_coverage_cd')

    df_benefit_master_source = df_benefit_master_source.join(
    df_ref_coverage_health_source,
    (F.trim(F.col('benefit_en_name')) == F.trim(F.col('map_benefit_en_name'))) &
    (F.trim(F.col('benefit_th_name')) == F.trim(F.col('map_benefit_th_name'))) &
    (F.trim(F.col('benefit_group_name')) == F.trim(F.col('map_benefit_group_name'))) &
    (F.trim(F.col('benefit_group_name_eng')) == F.trim(F.col('map_benefit_group_name_eng'))),
    "inner"
    )

    df_benefit_master_source = df_benefit_master_source.join(df_ref_coverage_md, df_benefit_master_source.map_benefit_oic_code == df_ref_coverage_md.oic_cov_code, 'left')
    df_insured_details_source = df_insured_details_source.withColumn("plan_no", F.when(F.trim(F.col('plan_no'))=='MEA5B2', 'HEA5B2'))
    df_insured_details_benefit = df_insured_details_source\
                            .join(df_benefit_master_source,F.trim(df_insured_details_source['coverage_cd']) ==  F.trim(df_benefit_master_source['benefit_coverage_cd']),"inner")

    pivoted_insured_details_benefit = df_insured_details_benefit.groupBy(
        'policy_no', 
        'plan_no', 
        'benefit_form_cd',
        'benefit_cd',
        'benefit_th_name',
        'benefit_en_name',
        'benefit_coverage_cd',
        'benefit_group_cd',
        'benefit_group_name',
        'benefit_group_name_eng',
        'map_benefit_th_name',
        'map_benefit_en_name',
        'map_benefit_group_name',
        'map_benefit_group_name_eng',
        'map_benefit_oic_code',
        'oic_cov_code',
        'oic_cov_detail_en',
        'oic_cov_detail_th' 
    ).agg(
        F.max(F.when(F.col('limit_type_cd').isin('AY', 'ADA'), F.col('benefit_limit'))).alias('cov_sum_insured_per_year'),
        F.max(F.when(F.col('limit_type_cd').isin('AD', 'APT'), F.col('benefit_limit'))).alias('cov_sum_insured_per_time'),
        F.max(F.when(F.col('limit_type_cd').isin('DAY', 'DDA'), F.col('benefit_limit'))).alias('cov_number_of_day')
    )

    print('----Preparing Com4 with Coverage for creating sub-policies ...')
    cond_insdtl =  [(F.trim(col("com4.cpno"))                  ==  F.trim(col("insdtl.policy_no"))) &\
                    (F.trim(col("com4.cplan"))                 ==  F.trim(col("insdtl.plan_no")))\
                ]

    df_lfkludta_lfpucom4_coverage = df_lfkludta_lfpucom4_source.alias("com4")\
                                    .join(df_lfklutab_tabplan_source,F.trim(df_lfkludta_lfpucom4_source['cplan']) ==  F.trim(df_lfklutab_tabplan_source['planc']),"inner")\
                                    .join(pivoted_insured_details_benefit.alias('insdtl') ,cond_insdtl , "left")\
                                    .withColumn("row_num"\
                                                ,F.when(F.trim(df_lfkludta_lfpucom4_source['cpno']).isNotNull()\
                                                            ,F.row_number()\
                                                                .over(Window.partitionBy(df_lfkludta_lfpucom4_source['cpno'])\
                                                                .orderBy(df_lfkludta_lfpucom4_source['cpno']\
                                                                .desc())\
                                                                    )\
                                                    )\
                                                )\
                                    .withColumnRenamed('cplan', 'cov_cplan')

    window_dedub = Window.partitionBy('cpno', 
                                    'cov_cplan',
                                    'insdtl.oic_cov_code',
                                    'insdtl.oic_cov_detail_th',
                                    'cov_sum_insured_per_year',
                                    'cov_sum_insured_per_time', 
                                    'cov_number_of_day').orderBy('insdtl.oic_cov_code')

    df_with_rownum = df_lfkludta_lfpucom4_coverage.withColumn('row_num_dedub', F.row_number().over(window_dedub))
    df_lfkludta_lfpucom4_coverage = df_with_rownum.filter(F.col('row_num_dedub') == 1).drop('row_num_dedub')

    address_window_spec = Window.partitionBy("policy_number").orderBy(df_t_policy_address_source["created_timestamp"].desc())
    df_t_policy_address_source = df_t_policy_address_source.withColumn("row_num", F.row_number().over(address_window_spec))
    df_t_policy_address_source = df_t_policy_address_source.filter(df_t_policy_address_source["row_num"] == 1).drop("row_num")

    print('--Starting Ordinary Policy Customer...')
    print('----Declaring Customer Name Prefix map with Company Prefix...')
    df_tblcustomer_namemasterbypno_map_company_prefix = df_tblcustomer_namemasterbypno_source\
                                                    .join(df_tblcompany_nameprefix_source, F.trim(df_tblcustomer_namemasterbypno_source['prefix']) ==  F.trim(df_tblcompany_nameprefix_source['abbv']),"left")\
                                                    .withColumnRenamed("abbv", "abbv_insured")
    df_tblcustomer_namemasterbypno_owner_map_company_prefix = df_tblcustomer_namemasterbypno_owner_source\
                                                    .join(df_tblcompany_nameprefix_source, F.trim(df_tblcustomer_namemasterbypno_owner_source['prefix']) ==  F.trim(df_tblcompany_nameprefix_source['abbv']),"left")\
                                                    .withColumnRenamed("abbv", "abbv_owner")
    print('----Declaring Address mapped with location code...')
    df_t_policy_address_source = df_t_policy_address_source.select('policy_number', 'sub_district_loc_cd', 'district_loc_cd', 'province_loc_cd', 'sub_district_th', 'district_th', 'province_th', 'province_en')\


    # df_tblcustomer_namemasterbypno_map_company_prefix.show(200, False)
    print('----Creating Customer Array and Insured Array...')
    policy_customer_df = df_lfkludta_lfppml_source.select('pno', 'pnamf', 'pname', 'pid', 'pdob', 'psex', 'padr1', 'padr2', 'padr3', 'padr4', 'peff', 'ptel1', 'plmbno', 'piemal'\
                                                        ,'pownrf','pmombno','pownid','lcls','pmed')\
                        .join(df_tblcustomer_namemasterbypno_map_company_prefix, F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pno']),"inner")\
                        .join(df_tblcustomer_namemasterbypno_owner_map_company_prefix, F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno']),"left")\
                        .join(df_lfkludta_lfppolus_source, F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfppolus_source['uspno']),"left")\
                        .join(df_lfkludta_lfpappus_source_group,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfpappus_source_group['uspno']),"left")\
                        .join(df_ref_policy_service_md_ref_customer_gender, F.trim(df_lfkludta_lfppml_source['psex']) ==  F.trim(df_ref_policy_service_md_ref_customer_gender['customer_gender_ktaxa_code']),"left")\
                        .join(df_t_policy_address_source, F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_t_policy_address_source['policy_number']),"left")\
                        .withColumn("customer",\
                                    F.struct(F.when(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()\
                                                    ,F.when(\
                                                        df_tblcustomer_namemasterbypno_owner_map_company_prefix['abbv_owner'].isNotNull()\
                                                        ,'O')\
                                                    .when(F.trim(df_lfkludta_lfppolus_source['usctz']) == 'FR', 'F')\
                                                    .when(F.trim(df_lfkludta_lfppml_source['pownrf']).like('^a-zA-Z'), 'F')\
                                                    .otherwise('P')\
                                                    )\
                                            .otherwise(
                                                    F.when(\
                                                        df_tblcustomer_namemasterbypno_map_company_prefix['abbv_insured'].isNotNull()\
                                                        ,'O')\
                                                    .when(F.trim(df_lfkludta_lfppolus_source['usctz']) == 'FR', 'F')\
                                                    .when(F.trim(df_lfkludta_lfppml_source['pnamf']).like('^a-zA-Z'), 'F')\
                                                    .otherwise('P')\
                                                    ).cast(T.StringType())\
                                            .alias("cust_type")\
                                            ,lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_type_other_detail')\
                                            )\
                                )

    policy_customer_df = policy_customer_df\
                        .withColumn("customer",\
                                    F.struct(F.col("customer.*"),\
                                            F.when(\
                                                    F.col("customer.cust_type") ==  'O'\
                                                    ,'99')\
                                            .when(\
                                                    F.col("customer.cust_type") ==  'F'\
                                                    ,'02')\
                                            .when(\
                                                    (F.col("customer.cust_type") ==  'P') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.length(F.trim(df_lfkludta_lfppml_source['pownid'])) == 13)\
                                                    ,'01')\
                                            .when(\
                                                    (F.col("customer.cust_type") ==  'P') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.length(F.trim(df_lfkludta_lfppml_source['pid'])) == 13)\
                                                    ,'01')\
                                            .otherwise('99').cast(T.StringType())\
                                            .alias("cust_id_type")\
                                            ,F.when(\
                                                    df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()\
                                                    , F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pid'])\
                                            )\
                                            .when(\
                                                    df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()\
                                                    , F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pid'])\
                                            )\
                                            .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                            .alias('cust_id')\
                                            ,F.when(\
                                                    (F.col("Customer.cust_type") ==  'O') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull())\
                                                    ,F.concat(F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pnamf']), lit(' '), F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pname']))\
                                            )\
                                            .when(\
                                                    (F.col("Customer.cust_type") ==  'O') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull())\
                                                    ,F.concat(F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pnamf']), lit(' '), F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pname']))\
                                            )\
                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())\
                                            .alias('cust_company_name')\
                                            ,F.when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['nameonly']) != '')\
                                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_tblcustomer_namemasterbypno_owner_map_company_prefix['nameonly'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                            )\
                                            .when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['nameonly']) == '')\
                                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_source['pownrf'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                            )\
                                            .when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['nameonly']) != '')\
                                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_tblcustomer_namemasterbypno_map_company_prefix['nameonly'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                            )\
                                            .when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['nameonly']) == '')\
                                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_source['pnamf'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                            )\
                                            .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                            .alias('cust_first_name')\
                                            ,F.when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull())\
                                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                                )\
                                                .when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull())\
                                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_tblcustomer_namemasterbypno_map_company_prefix['pname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                                )\
                                                .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                .alias('cust_last_name')\
                                            ,F.when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull())\
                                                    , F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pdob']))/10000)\
                                                        .cast(T.StringType())\
                                            )\
                                            .when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull())\
                                                    , F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pdob']))/10000)\
                                                        .cast(T.IntegerType())\
                                            )\
                                            .otherwise(lit(REQ_NULL_INT)).cast(T.IntegerType())\
                                                .alias('cust_age')\
                                            ,F.when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pdob']) != '0')\
                                                    , concat(substring(F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pdob']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pdob']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pdob']), 7, 2)\
                                                        , lit('T00:00:00+07:00'))\
                                            )\
                                            .when(\
                                                    (F.col("Customer.cust_type") !=  'O') &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()) &\
                                                    (F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pdob']) != '0')\
                                                    ,  concat(substring(F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pdob']), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pdob']), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pdob']), 7, 2)\
                                                        , lit('T00:00:00+07:00'))\
                                                    )\
                                            .otherwise(lit('1753-01-01T00:00:00+00:00')).cast(T.StringType())\
                                            .alias('cust_birthday')\
                                            ,F.when(\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()) &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['psex'] == 'M')
                                                    , lit('01')
                                            )\
                                            .when(\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull()) &\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['psex'] == 'F')
                                                    , lit('02')
                                            )\
                                            .when(\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()) &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['psex'] == 'M')
                                                    , lit('01')
                                            )\
                                            .when(\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull()) &\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['psex'] == 'F')
                                                    , lit('02')
                                            )\
                                            .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                            .alias('cust_gender')\
                                            ,F.trim(F.regexp_replace(F.regexp_replace(\
                                                                substring(F.concat(F.trim(df_lfkludta_lfppml_source['padr1'])\
                                                            , lit(' ')\
                                                            , F.trim(df_lfkludta_lfppml_source['padr2'])\
                                                            , lit(' ')\
                                                            , F.trim(df_lfkludta_lfppml_source['padr3'])\
                                                            , lit(' ')\
                                                            , F.trim(df_lfkludta_lfppml_source['padr4'])),1,100)\
                                                            ,"[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""), "\\\\", ""))\
                                                .cast(T.StringType()).alias('cust_address')\
                                            ,F.when(\
                                                        F.trim(df_t_policy_address_source['sub_district_loc_cd']) != ''
                                                        , F.trim(df_t_policy_address_source['sub_district_loc_cd'])\
                                                    )\
                                                .otherwise(lit('00')).cast(T.StringType()).alias('cust_sub_district')\
                                            ,F.when(\
                                                        F.trim(df_t_policy_address_source['district_loc_cd']) != ''
                                                        , F.trim(df_t_policy_address_source['district_loc_cd'])\
                                                    )\
                                                .otherwise(lit('00')).cast(T.StringType()).alias('cust_district')\
                                            ,F.when(\
                                                        F.trim(df_t_policy_address_source['province_loc_cd']) != ''
                                                        , F.trim(df_t_policy_address_source['province_loc_cd'])\
                                                    )\
                                                .otherwise(lit('00')).cast(T.StringType()).alias('cust_province')\
                                            ,F.when(\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull())\
                                                    , F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['zipcode'])\
                                            )\
                                            .when(\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull())\
                                                    , F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['zipcode'])\
                                            )\
                                            .otherwise(lit('00')).cast(T.StringType())\
                                            .alias('cust_zip_code')\
                                            ,lit(cust_country_code).cast(T.StringType()).alias('cust_country_code')\
                                            ,F.when((F.trim(df_lfkludta_lfppml_source['ptel1']) != '') & (F.length(df_lfkludta_lfppml_source['ptel1']) >= 6), F.trim(df_lfkludta_lfppml_source['ptel1']))\
                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_phone_number')\
                                            ,F.when(\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull())\
                                                    , F.trim(df_lfkludta_lfppml_source['plmbno'])\
                                            )\
                                            .when(\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull())\
                                                    , F.trim(df_lfkludta_lfppml_source['pmombno'])\
                                            )\
                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())\
                                            .alias('cust_mobile_number')\
                                            ,F.when(\
                                                        (F.trim(df_lfkludta_lfppml_source['piemal']) != '') & (F.length(df_lfkludta_lfppml_source['piemal']) >= 6)\
                                                        , F.regexp_replace(F.regexp_replace(F.trim(df_lfkludta_lfppml_source['piemal']),\
                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                    ),"\\\\", "")\
                                                    )\
                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_email')\
                                            ,F.when(\
                                                    (df_tblcustomer_namemasterbypno_owner_map_company_prefix['pno'].isNotNull())\
                                                    , F.floor((F.trim(df_lfkludta_lfppml_source['peff'])-F.trim(df_tblcustomer_namemasterbypno_owner_map_company_prefix['pdob']))/10000)\
                                                        .cast(T.IntegerType())\
                                            )\
                                            .when(\
                                                    (df_tblcustomer_namemasterbypno_map_company_prefix['pno'].isNotNull())\
                                                    , F.floor((F.trim(df_lfkludta_lfppml_source['peff'])-F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['pdob']))/10000)\
                                                        .cast(T.IntegerType())\
                                            )\
                                            .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.IntegerType())\
                                            .alias('cust_issued_age')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_race')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_nationality')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_religion')\
                                            ,F.when(F.trim(df_lfkludta_lfpappus_source_group['usoocl']).isin('1','4','6','7'), F.lit('01'))\
                                            .when(F.trim(df_lfkludta_lfpappus_source_group['usoocl'])=='2', F.lit('02'))\
                                            .when(F.trim(df_lfkludta_lfpappus_source_group['usoocl'])=='3', F.lit('03'))\
                                            .when(F.trim(df_lfkludta_lfpappus_source_group['usoocl'])=='5', F.lit('04'))\
                                            .otherwise(lit('99'))\
                                            .alias('cust_occupation_level')\
                                            ,F.when(F.trim(df_lfkludta_lfpappus_source_group['usocl']) <= 5, lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                            .otherwise(F.trim(df_lfkludta_lfpappus_source_group['usocl']))\
                                            .cast(T.StringType()).alias('cust_occupation_level_other_detail')\
                                            ,(lit(REQ_NULL_STR)).cast(T.StringType()).cast(T.StringType()).alias('cust_occupation')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_job')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_position')\
                                            ,lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_business_type')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.DecimalType(18, 2)).alias('cust_income_per_year')\
                                            ,lit('N').cast(T.StringType()).alias('cust_us_person')\
                                            ,lit('N').cast(T.StringType()).alias('cust_us_person_born')\
                                            ,lit('N').cast(T.StringType()).alias('cust_us_person_green_card')\
                                            ,lit('N').cast(T.StringType()).alias('cust_us_person_vat')\
                                            ,lit('N').cast(T.StringType()).alias('cust_us_person_183')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_permanent')\
                                            ,lit('00').cast(T.StringType()).alias('cust_sub_district_permanent')\
                                            ,lit('00').cast(T.StringType()).alias('cust_district_permanent')\
                                            ,lit('00').cast(T.StringType()).alias('cust_province_permanent')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_work')\
                                            ,lit('00').cast(T.StringType()).alias('cust_sub_district_work')\
                                            ,lit('00').cast(T.StringType()).alias('cust_district_work')\
                                            ,lit('00').cast(T.StringType()).alias('cust_province_work')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_current')\
                                            ,lit('00').cast(T.StringType()).alias('cust_sub_district_current')\
                                            ,lit('00').cast(T.StringType()).alias('cust_district_current')\
                                            ,lit('00').cast(T.StringType()).alias('cust_province_current')\
                                            )\
                                    )\
                        .withColumn('insureds', F.array(\
                                                    F.struct(lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('insrd_cov_plan_seq')
                                                            ,lit(insrd_seq).cast(T.IntegerType()).alias('insrd_seq')
                                                            ,F.when(\
                                                                    df_tblcustomer_namemasterbypno_map_company_prefix['abbv_insured'].isNotNull()\
                                                                ,'O')\
                                                                .when(F.trim(df_lfkludta_lfppolus_source['usctz']) == 'FR', 'F')\
                                                                .when(F.trim(df_lfkludta_lfppml_source['pnamf']).like('^a-zA-Z'), 'F')\
                                                                .otherwise('P')\
                                                                .cast(T.StringType()).alias("insrd_type")\
                                                            ,F.when(\
                                                                        F.col("customer.cust_type") ==  'O','99'
                                                                    )\
                                                                .when(\
                                                                        F.col("customer.cust_type") ==  'F','02'
                                                                    )\
                                                                .when(\
                                                                        (F.col("customer.cust_type") ==  'P') & (F.length(F.trim(df_lfkludta_lfppml_source['pid'])) == 13)\
                                                                        ,'01'
                                                                    )\
                                                                .otherwise('99')\
                                                                .cast(T.StringType()).alias("insrd_id_type")\
                                                            ,F.when(\
                                                                        F.trim(df_lfkludta_lfppml_source['pid']) != ''\
                                                                        , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_source['pid'],1,100)),\
                                                                                                    "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                                                ),"\\\\", "")\
                                                                    )\
                                                            .otherwise(\
                                                                            F.regexp_replace(F.regexp_replace(F.trim(substring(df_tblcustomer_namemasterbypno_map_company_prefix['SearchString'],1,100)),\
                                                                                                    "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                                                ),"\\\\", "")\
                                                                        )\
                                                            .cast(T.StringType()).alias('insrd_id')\
                                                            ,F.when(\
                                                                            F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['nameonly']) != ''\
                                                                            , F.regexp_replace(F.regexp_replace(F.trim(substring(df_tblcustomer_namemasterbypno_map_company_prefix['nameonly'],1,100)),\
                                                                                                    "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                                                ),"\\\\", "")\
                                                                    )\
                                                                .otherwise(\
                                                                        F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_source['pnamf'],1,100)),\
                                                                                                "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                                            ),"\\\\", "")\
                                                                        )\
                                                                .cast(T.StringType()).alias('insrd_first_name')\
                                                            ,F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_source['pname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                            ),"\\\\", "")\
                                                                    .cast(T.StringType()).alias('insrd_last_name')\
                                                            ,F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(df_lfkludta_lfppml_source['pdob']))/10000)\
                                                                .cast(T.IntegerType())\
                                                                .alias('insrd_age')\
                                                            ,concat(substring(F.trim(df_lfkludta_lfppml_source['pdob']), 1, 4)\
                                                                    , lit('-')\
                                                                    , substring(F.trim(df_lfkludta_lfppml_source['pdob']), 5, 2)\
                                                                    , lit('-')\
                                                                    ,substring(F.trim(df_lfkludta_lfppml_source['pdob']), 7, 2)\
                                                                    , lit('T00:00:00+07:00'))\
                                                            .cast(T.StringType()).alias('insrd_birthday')\
                                                            ,F.when(F.trim(df_lfkludta_lfppml_source['psex']) != '', F.trim(df_ref_policy_service_md_ref_customer_gender['customer_gender_oic_code']))\
                                                                .otherwise('99')\
                                                            .alias('insrd_gender')\
                                                            ,F.trim(F.regexp_replace(F.regexp_replace(\
                                                                    substring(F.concat(F.trim(df_lfkludta_lfppml_source['padr1'])\
                                                                    , lit(' ')\
                                                                    , F.trim(df_lfkludta_lfppml_source['padr2'])\
                                                                    , lit(' ')\
                                                                    , F.trim(df_lfkludta_lfppml_source['padr3'])\
                                                                    , lit(' ')\
                                                                    , F.trim(df_lfkludta_lfppml_source['padr4'])),1,100)\
                                                                    ,"[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""), "\\\\", ""))\
                                                                    .cast(T.StringType()).alias('insrd_address')\
                                                            ,F.when(\
                                                                        F.trim(df_t_policy_address_source['sub_district_loc_cd']) != ''
                                                                        , F.trim(df_t_policy_address_source['sub_district_loc_cd'])\
                                                                    )\
                                                                .otherwise(lit('00')).cast(T.StringType()).alias('insrd_sub_district')\
                                                            ,F.when(\
                                                                        F.trim(df_t_policy_address_source['district_loc_cd']) != ''
                                                                        , F.trim(df_t_policy_address_source['district_loc_cd'])\
                                                                    )\
                                                                .otherwise(lit('00')).cast(T.StringType()).alias('insrd_district')\
                                                            ,F.when(\
                                                                        F.trim(df_t_policy_address_source['province_loc_cd']) != ''
                                                                        , F.trim(df_t_policy_address_source['province_loc_cd'])\
                                                                    )\
                                                                .otherwise(lit('00')).cast(T.StringType()).alias('insrd_province')\
                                                            ,F.when(\
                                                                        F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['zipcode']) != ''
                                                                        , F.trim(df_tblcustomer_namemasterbypno_map_company_prefix['zipcode'].cast(T.StringType()))\
                                                                    )\
                                                                .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()).alias('insrd_zip_code')\
                                                            ,lit(cust_country_code).alias('insrd_country_code')\
                                                            ,F.when(\
                                                                        (F.trim(df_lfkludta_lfppml_source['ptel1']) != '') & (F.length(df_lfkludta_lfppml_source['ptel1']) >= 6),
                                                                        F.trim(df_lfkludta_lfppml_source['ptel1'])
                                                                    )\
                                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_phone_number')\
                                                            ,F.when(\
                                                                        (F.trim(df_lfkludta_lfppml_source['plmbno']) != '') & (F.length(df_lfkludta_lfppml_source['plmbno']) >= 6),
                                                                        F.trim(df_lfkludta_lfppml_source['plmbno'])
                                                                    )\
                                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_mobile_number')\
                                                            ,F.when(\
                                                                        F.trim(df_lfkludta_lfppml_source['piemal']) != ''\
                                                                        , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_source['piemal'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                            ),"\\\\", "")\
                                                                    )\
                                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_email')\
                                                            ,F.floor((F.trim(df_lfkludta_lfppml_source['peff'])-F.trim(df_lfkludta_lfppml_source['pdob']))/10000)\
                                                                        .cast(T.IntegerType())\
                                                                        .alias('insrd_issued_age')\
                                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_race')\
                                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_nationality')\
                                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_religion')\
                                                            ,F.when(F.trim(df_lfkludta_lfpappus_source['usocl']).isin('1','4','6','7'), F.lit('01'))\
                                                            .when(F.trim(df_lfkludta_lfpappus_source['usocl'])=='2', F.lit('02'))\
                                                            .when(F.trim(df_lfkludta_lfpappus_source['usocl'])=='3', F.lit('03'))\
                                                            .when(F.trim(df_lfkludta_lfpappus_source['usocl'])=='5', F.lit('04'))\
                                                            .otherwise(lit('99'))\
                                                            .cast(T.StringType()).alias('insrd_occupation_level')\
                                                            ,F.when(F.trim(df_lfkludta_lfpappus_source['usocl']) <= 5, lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                                                .otherwise(F.trim(df_lfkludta_lfpappus_source_group['usocl']))\
                                                                .cast(T.StringType()).alias('insrd_occupation_level_other_detail')\
                                                            ,F.when(F.trim(df_lfkludta_lfpappus_source['usocl']) != '', F.trim(df_lfkludta_lfpappus_source['usocl']))\
                                                                .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())\
                                                                .alias('insrd_occupation')\
                                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_job')\
                                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_position')\
                                                            ,lit(REQ_NULL_STR).cast(T.StringType()).alias('insrd_business_type')\
                                                            ,lit(NON_REQ_NULL_NUM).cast(T.DecimalType(18, 2)).alias('insrd_income_per_year')\
                                                            ,F.array().alias('insrd_pol_sub_ids')\
                                                            ,F.array().alias('insrd_beneficiaries')\
                                                            ,F.when(F.trim(F.col('lcls')) == 'S', '01').otherwise('02').cast(T.StringType()).alias('standard_risk')\
                                                            ,F.when(F.trim(F.col('pmed')) == 'Y', '01').otherwise('02').cast(T.StringType()).alias('medical')\
                                                            )\
                                                        )\
                                )

    policy_customer_df = policy_customer_df\
                        .withColumn('cust_pno', df_lfkludta_lfppml_source['pno'])

    policy_customer_df = policy_customer_df.select('cust_pno', 'customer',
                                                'insureds')

    print('policy customer :', policy_customer_df.count())

    cov_plan_cov_code_expr = (
    F.when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_code'].isNotNull()),
        df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_code'])\
    .when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_code'].isNull())&(df_lfkludta_lfpucom4_coverage['com4.oic_cov_code'].isNotNull()), 
        df_lfkludta_lfpucom4_coverage['com4.oic_cov_code'])\
    .when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_code'].isNull())&(df_lfkludta_lfpucom4_coverage['com4.oic_cov_code'].isNull()), 
        F.lit('C9016'))\
    .otherwise(F.lit('C9B35')).cast(T.StringType()))

    cov_plan_cov_detail_expr = (
    F.when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_code']!='C9B35') & (df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_detail_th'].isNotNull()), 
        df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_detail_th'])\
    .when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_detail_th'].isNull())&
        (df_lfkludta_lfpucom4_coverage['com4.oic_cov_detail_th'].isNotNull()),
        df_lfkludta_lfpucom4_coverage['com4.oic_cov_detail_th'])\
    .when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_code']=='C9B35') & 
        (F.trim(df_lfkludta_lfpucom4_coverage['com4.oic_cov_detail_th'])=='B0035 อื่น ๆ โปรดระบุ………') & 
            (F.col('benefit_th_name').isNotNull()), F.concat(F.lit('B0036 - '), F.lit('benefit_th_name')))\
    .when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_detail_th'].isNull()) & 
        (df_lfkludta_lfpucom4_coverage['com4.oic_cov_detail_th'].isNull()) & 
        (F.col('benefit_th_name').isNotNull()), 
            F.concat(F.lit('B0036 - '), F.lit('benefit_th_name')))\
    .when((df_lfkludta_lfpucom4_coverage['insdtl.oic_cov_detail_th'].isNull())&
        (df_lfkludta_lfpucom4_coverage['com4.oic_cov_detail_th'].isNull())&
        (F.col('benefit_th_name').isNull()), 
            F.concat(F.lit('A0023 - '), F.col('pland')))\
    .otherwise(F.concat(F.lit('B0036 - '), F.col('benefit_th_name'))).cast(T.StringType()))

    coverage_struct = F.struct(
        F.lit(REQ_NULL_STR).alias('cov_code'),
        F.lit(REQ_NULL_STR).alias('cov_detail'),
        F.lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).alias('cov_amt'),
        F.lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).alias('cov_sum_insured_per_coverage_per_year'),
        F.lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).alias('cov_sum_insured_per_coverage_per_time'),
        F.lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).alias('cov_co_payment'),
        F.lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).alias('cov_deductible_amount'),
        F.lit(REQ_NULL_STR).alias('cov_deductible_detail'),
        F.lit(REQ_NULL_INT).cast(T.IntegerType()).alias('cov_number_of_day'),
        F.lit(REQ_NULL_INT).cast(T.IntegerType()).alias('cov_cover_year'),
        F.lit(REQ_NULL_INT).cast(T.IntegerType()).alias('cov_payment_year'),
        F.lit(REQ_NULL_STR).alias('cov_claim_status')
    )

    coverage_under_plans_struct = F.struct(
        cov_plan_cov_code_expr.alias('cov_plan_cov_code'),
        cov_plan_cov_detail_expr.alias('cov_plan_cov_detail'),
        df_lfkludta_lfpucom4_coverage['csumin'].cast(T.DecimalType(18, 2)).alias('cov_plan_cov_amt'),

        F.when(
            df_lfkludta_lfpucom4_coverage['cov_sum_insured_per_year'].isNotNull(),
            df_lfkludta_lfpucom4_coverage['cov_sum_insured_per_year'].cast(T.DecimalType(18, 2))\
        ).otherwise(F.lit(NON_REQ_NULL_NUM)).alias('cov_plan_cov_sum_insured_per_coverage_per_year'),

        F.when(
            df_lfkludta_lfpucom4_coverage['cov_sum_insured_per_time'].isNotNull(),
            df_lfkludta_lfpucom4_coverage['cov_sum_insured_per_time'].cast(T.DecimalType(18, 2))\
        ).otherwise(F.lit(NON_REQ_NULL_NUM)).alias('cov_plan_cov_sum_insured_per_coverage_per_time'),

        F.lit(NON_REQ_NULL_NUM).alias('cov_plan_cov_co_payment'),
        F.lit(NON_REQ_NULL_NUM).alias('cov_plan_cov_deductible_amount'),
        F.lit(NON_REQ_NULL_STR).alias('cov_plan_cov_deductible_detail'),

        F.when(
            df_lfkludta_lfpucom4_coverage['cov_number_of_day'].isNotNull(),
            df_lfkludta_lfpucom4_coverage['cov_number_of_day']
        ).otherwise(F.lit(NON_REQ_NULL_NUM)).cast(T.IntegerType()).alias('cov_plan_cov_number_of_day'),

        df_lfkludta_lfpucom4_coverage['plpol'].cast(T.IntegerType()).alias('cov_plan_cov_cover_year'),

        F.when(
            df_lfkludta_lfpucom4_coverage['plprea'] == '1',
            (df_lfkludta_lfpucom4_coverage['plpre'] - df_lfkludta_lfpucom4_coverage['cage'])
            .cast(T.IntegerType())
        ).otherwise(df_lfkludta_lfpucom4_coverage['plpre'].cast(T.IntegerType()))
        .alias('cov_plan_cov_payment_year'),

        lit(None).alias('cov_plan_cov_claim_status')
    )

    # Final aggregation
    df_lfkludta_lfpucom4_coverage_output = df_lfkludta_lfpucom4_coverage.groupBy('cpno', 'cov_cplan').agg(
        F.collect_list(lit(None)).alias('coverages'),
        F.collect_list(coverage_under_plans_struct).alias('coverages_under_plans')
    )


    print('----Starting Sub-Policies Aggregation ...')
    cond =  [(F.trim(col("com4.cov_cplan"))                 ==  F.trim(col("dvd.lplan"))) &\
            (col("com4.cage").cast(T.IntegerType())    ==  col("dvd.dvage").cast(T.IntegerType())) &\
            (F.trim(col("com4.csex"))                  ==  F.trim(col("dvd.dvsex")))&\
            (F.concat(F.trim(substring(col("com4.cexdte"), 7, 4))\
                    ,F.trim(substring(col("com4.cexdte"), 4, 2))\
                    ,F.trim(substring(col("com4.cexdte"), 1, 2))) >=  col("dvd.dveff_max"))\
            ]

    df_lfkludta_lfpucom4_sub_policies_df = df_lfkludta_lfpucom4_coverage.select('cno', 'cpno', 'cov_cplan', 'cefdte', 'cexdte', 'csumin', 'cage', 'csex', 'csex', 'cexdte').alias('com4')\
                                        .join(df_lfklutab_tabplan_source.alias("tab"),F.trim(df_lfkludta_lfpucom4_coverage['cov_cplan']) ==  F.trim(df_lfklutab_tabplan_source['planc']),"inner")\
                                        .join(df_lfklutab_lfpdvd_merge_master.alias("dvd")\
                                            ,cond\
                                            ,"left")\
                                        .join(df_lfkludta_lfpucom4_coverage_output.alias('cov'),(F.trim(col('com4.cpno')) ==  F.trim(col('cov.cpno')))\
                                                                                                    & (F.trim(col('com4.cov_cplan')) ==  F.trim(col('cov.cov_cplan'))) ,"inner")\
                                        .select(col('com4.*'),
                                                col('tab.*'),
                                                col('dvd.*'),
                                                df_lfkludta_lfpucom4_coverage_output['coverages'], 
                                                df_lfkludta_lfpucom4_coverage_output['coverages_under_plans'])\
                                        .join(df_ref_ordinary_pol_sub_type_lserff,
                                            F.trim(col('com4.cov_cplan')) ==  F.trim(df_ref_ordinary_pol_sub_type_lserff['plan_code']),"left")\
                                        .withColumn("row_num", F.row_number().over(Window.partitionBy(df_lfkludta_lfpucom4_coverage['cpno'], df_lfkludta_lfpucom4_coverage['cov_cplan']).orderBy(df_lfkludta_lfpucom4_coverage['cno'].desc(), df_lfkludta_lfpucom4_coverage['cov_cplan'])))\
                                        .drop(col('cov.cpno'))

    window_unique_cplan = Window.partitionBy("cpno", "cov_cplan").orderBy(F.col("cno").desc())
    df_lfkludta_lfpucom4_sub_policies_df = df_lfkludta_lfpucom4_sub_policies_df.withColumn("plan_rank", F.row_number().over(window_unique_cplan))\
                                                        .filter(F.col("plan_rank") == 1)

    window_cpno = Window.partitionBy("cpno").orderBy(F.col("cno").desc(), F.col("cov_cplan"))
    df_lfkludta_lfpucom4_sub_policies_df = df_lfkludta_lfpucom4_sub_policies_df \
    .withColumn("row_num_plan", F.row_number().over(window_cpno))

    sub_policies_df = df_lfkludta_lfpucom4_sub_policies_df.withColumn(
        "sub_pol_id",
        F.concat(F.col("cpno"), F.lit("_"), F.col("row_num_plan")).cast(T.StringType())
    )
    # สร้าง sub_pol_id
    sub_policies_df = sub_policies_df.withColumn('sub_pol_id',F.concat(df_lfkludta_lfpucom4_source['cpno'],F.lit('_'),df_lfkludta_lfpucom4_sub_policies_df['row_num_plan'],F.lit('|'),df_lfkludta_lfpucom4_sub_policies_df['row_num_plan']).cast(T.StringType()))
    # รวม sub_pol_id ต่อ cpno
    sub_pol_ids_df = sub_policies_df.groupBy('cpno').agg(F.collect_list('sub_pol_id').alias('insrd_pol_sub_ids'))
    # 1. explode insureds
    policy_customer_df = policy_customer_df.withColumn("insured", F.explode("insureds"))
    # 2. join กับ sub_pol_ids_df
    policy_customer_df = policy_customer_df.join(sub_pol_ids_df,policy_customer_df["cust_pno"] == sub_pol_ids_df["cpno"],how="left")
    # 3. ใช้ withField เพื่ออัปเดตฟิลด์ใน struct
    policy_customer_df = policy_customer_df.withColumn("insured",F.col("insured").withField("insrd_pol_sub_ids", F.col("insrd_pol_sub_ids")))
    # 4. รวมกลับเป็น array
    policy_customer_df = policy_customer_df.groupBy("cust_pno", "customer").agg(F.collect_list("insured").alias("insureds"))

    df_lfkludta_lfpucom4_sub_policies_df = df_lfkludta_lfpucom4_sub_policies_df.orderBy('row_num_plan')

    df_lfkludta_lfpucom4_sub_policies_df = df_lfkludta_lfpucom4_sub_policies_df.groupBy('cpno')\
                                                                            .agg(F.sort_array(F.collect_list(\
                                                                                                    F.struct(\
                                                                                                            F.concat(df_lfkludta_lfpucom4_source['cpno'],lit('_'),df_lfkludta_lfpucom4_sub_policies_df['row_num_plan']).cast(T.StringType()).alias('sub_pol_id')\
                                                                                                            ,
                                                                                                            F.when(df_lfkludta_lfpucom4_sub_policies_df['cno'] != '', 'A')\
                                                                                                            .otherwise('B').cast(T.StringType()).alias('sub_pol_type')
                                                                                                            ,F.when(F.trim(df_ref_ordinary_pol_sub_type_lserff['plan_code']).isNotNull()\
                                                                                                                , F.trim(df_ref_ordinary_pol_sub_type_lserff['lserff_code']))\
                                                                                                                .otherwise(lit('1022S000000000')).cast(T.StringType())\
                                                                                                                .alias('sub_pol_ppt')\
                                                                                                            ,concat(substring(F.trim(df_lfkludta_lfpucom4_sub_policies_df['cefdte']), 7, 4)\
                                                                                                                    , lit('-')\
                                                                                                                    , substring(F.trim(df_lfkludta_lfpucom4_sub_policies_df['cefdte']), 4, 2)\
                                                                                                                    , lit('-')\
                                                                                                                    ,substring(F.trim(df_lfkludta_lfpucom4_sub_policies_df['cefdte']), 1, 2)\
                                                                                                                    , lit('T00:00:00+07:00'))\
                                                                                                            .cast(T.StringType()).alias('sub_pol_start_date')\
                                                                                                            ,concat(substring(F.trim(df_lfkludta_lfpucom4_sub_policies_df['cexdte']), 7, 4)\
                                                                                                                    , lit('-')\
                                                                                                                    , substring(F.trim(df_lfkludta_lfpucom4_sub_policies_df['cexdte']), 4, 2)\
                                                                                                                    , lit('-')\
                                                                                                                    ,substring(F.trim(df_lfkludta_lfpucom4_sub_policies_df['cexdte']), 1, 2)\
                                                                                                                    , lit('T00:00:00+07:00'))\
                                                                                                            .cast(T.StringType()).alias('sub_pol_end_date')\
                                                                                                            ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).alias('sub_pol_sum_insured_per_year_exclude')\
                                                                                                            ,F.trim(df_lfkludta_lfpucom4_sub_policies_df['csumin']).cast(T.DecimalType(18, 2)).alias('sub_pol_sum_insured_per_year_include')\
                                                                                                            ,F.array(\
                                                                                                                        F.struct(\
                                                                                                                                    df_lfkludta_lfpucom4_sub_policies_df['row_num_plan'].cast(T.IntegerType()).alias('cov_plan_seq')\
                                                                                                                                    ,F.trim(df_lfkludta_lfpucom4_sub_policies_df['pland']).cast(T.StringType()).alias('cov_plan_name')\
                                                                                                                                    ,df_lfkludta_lfpucom4_sub_policies_df['csumin'].cast(T.DecimalType(18, 2)).alias('cov_plan_sum_insured')\
                                                                                                                                    ,((df_lfkludta_lfpucom4_sub_policies_df['csumin']*df_lfkludta_lfpucom4_sub_policies_df['plmatr'])/100).cast(T.DecimalType(18, 2))\
                                                                                                                                    .alias('cov_plan_maturity_benefit')
                                                                                                                                    ,F.when(df_lfklutab_lfpdvd_merge_master['lplan'].isNotNull()\
                                                                                                                                            ,(df_lfkludta_lfpucom4_sub_policies_df['min_dvd']*df_lfkludta_lfpucom4_sub_policies_df['csumin'])/100)\
                                                                                                                                    .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18, 2)).alias('cov_plan_survival_benefit_min')\
                                                                                                                                    ,F.when(df_lfklutab_lfpdvd_merge_master['lplan'].isNotNull()\
                                                                                                                                            ,(df_lfkludta_lfpucom4_sub_policies_df['max_dvd']*df_lfkludta_lfpucom4_sub_policies_df['csumin'])/100)\
                                                                                                                                    .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18, 2)).alias('cov_plan_survival_benefit_max')\
                                                                                                                                    ,df_lfkludta_lfpucom4_coverage_output['coverages_under_plans'].alias('coverages_under_plans')\
                                                                                                                                )\
                                                                                                                    ).alias('policy_cov_plan_seqs')\
                                                                                                            ,df_lfkludta_lfpucom4_coverage_output['coverages']
                                                                                                            )\
                                                                                                )).alias('sub_policies')\
                                                                                )

    df_lfkludta_lfpucom4_sub_policies_df = df_lfkludta_lfpucom4_sub_policies_df\
                                        .withColumn('sub_pol', df_lfkludta_lfpucom4_sub_policies_df['cpno'])



    print(df_lfkludta_lfpucom4_sub_policies_df.count())

    print('--Starting Ordinary Policy Beneficiary...')
    print('----Preparing Beneficiary by map Ref...')


    df_lfkluhst_lfpbnfyh_source_prep = df_lfkluhst_lfpbnfyh_source.select(\
                                                                    F.trim(df_lfkluhst_lfpbnfyh_source['bnpno']).alias('bnpno')\
                                                                    ,F.trim(df_lfkluhst_lfpbnfyh_source['bnbnfpc']).alias('bnbnfpc')\
                                                                    ,F.trim(df_lfkluhst_lfpbnfyh_source['bnbnff']).alias('bnbnff')\
                                                                    ,F.trim(df_lfkluhst_lfpbnfyh_source['bnbnfy']).alias('bnbnfy')\
                                                                    ,F.trim(df_lfkluhst_lfpbnfyh_source['bnrelatn']).alias('bnrelatn')\
                                                                    )

    df_lfkludta_lfpbnfy_source_prep = df_lfkludta_lfpbnfy_source.select(\
                                                                    F.trim(df_lfkludta_lfpbnfy_source['bnpno']).alias('bnpno')\
                                                                    ,F.trim(df_lfkludta_lfpbnfy_source['bnbnfpc']).alias('bnbnfpc')\
                                                                    ,F.trim(df_lfkludta_lfpbnfy_source['bnbnff']).alias('bnbnff')\
                                                                    ,F.trim(df_lfkludta_lfpbnfy_source['bnbnfy']).alias('bnbnfy')\
                                                                    ,F.trim(df_lfkludta_lfpbnfy_source['bnrelatn']).alias('bnrelatn')\
                                                                    )

    df_lfkludta_lfpbnfyinf_source_prep = df_lfkludta_lfpbnfyinf_source.select(\
                                                                    F.trim(df_lfkludta_lfpbnfyinf_source['g07pno']).alias('bnpno')\
                                                                    ,F.trim(df_lfkludta_lfpbnfyinf_source['g07pct']).alias('bnbnfpc')\
                                                                    ,F.trim(df_lfkludta_lfpbnfyinf_source['g07namef']).alias('bnbnff')\
                                                                    ,F.trim(df_lfkludta_lfpbnfyinf_source['g07name']).alias('bnbnfy')\
                                                                    ,lit(None).alias('bnrelatn')\
                                                                    )

    df_beneficiary_union_prep = df_lfkludta_lfpbnfy_source_prep.union(df_lfkluhst_lfpbnfyh_source_prep\
                                                                        .join(df_lfkludta_lfpbnfy_source_prep,\
                                                                            df_lfkluhst_lfpbnfyh_source_prep['bnpno'] == df_lfkludta_lfpbnfy_source_prep['bnpno'],\
                                                                            "leftanti")\
                                                                        ).dropDuplicates()

    df_beneficiary_union_prep = df_lfkludta_lfpbnfyinf_source_prep.union(df_beneficiary_union_prep\
                                                                        .join(df_lfkludta_lfpbnfyinf_source_prep,\
                                                                            df_beneficiary_union_prep['bnpno'] == df_lfkludta_lfpbnfyinf_source_prep['bnpno'],\
                                                                            "leftanti")\
                                                                        ).dropDuplicates()


    df_beneficiary_union_full_join_map_ref_prep = df_beneficiary_union_prep\
                                        .join(df_ref_policy_service_md_ref_pol_beneficiary_relation\
                                                ,F.trim(df_beneficiary_union_prep['bnrelatn']) ==  F.trim(df_ref_policy_service_md_ref_pol_beneficiary_relation['pol_beneficiary_relation_ktaxa_code']),"left")\

    print('----Starting Aggregation Policy Beneficiary...')

    df_lfkludta_lfppml_beneficiary_df =    df_lfkludta_lfppml_source.select('pno').alias('pol')\
                                        .join(df_beneficiary_union_full_join_map_ref_prep,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_beneficiary_union_full_join_map_ref_prep['bnpno']),"left")\
                                        .withColumn("row_num"\
                                                    ,F.when(F.trim(df_beneficiary_union_full_join_map_ref_prep['bnpno']).isNotNull()\
                                                            ,F.row_number()\
                                                                .over(Window.partitionBy(df_lfkludta_lfppml_source['pno'])\
                                                                .orderBy(df_beneficiary_union_full_join_map_ref_prep['bnbnfpc']\
                                                                .desc())\
                                                                    )\
                                                            )\
                                                        .otherwise(lit(REQ_NULL_INT)).cast(T.IntegerType())\
                                                    )\
                                        .withColumn("beneficiary_name"\
                                                    ,F.when(F.trim(df_beneficiary_union_full_join_map_ref_prep['bnpno']).isNotNull()\
                                                            , F.concat(F.trim(df_beneficiary_union_full_join_map_ref_prep['bnbnff']),lit(' '),F.trim(df_beneficiary_union_full_join_map_ref_prep['bnbnfy']))
                                                            )\
                                                        .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())
                                                    )\
                                        .withColumn("beneficiary_relation"\
                                                        ,F.when(F.trim(df_beneficiary_union_full_join_map_ref_prep['bnpno']).isNotNull()\
                                                            , df_beneficiary_union_full_join_map_ref_prep['pol_beneficiary_relation_oic_code']
                                                            )\
                                                        .otherwise(lit('99')).cast(T.StringType())\
                                                    )\
                                        .withColumn("benefit_ratio"\
                                                    ,F.when((F.trim(df_beneficiary_union_full_join_map_ref_prep['bnpno']).isNotNull()) & \
                                                            (F.trim(df_beneficiary_union_full_join_map_ref_prep['bnpno']) != '')\
                                                            , F.trim(df_beneficiary_union_full_join_map_ref_prep['bnbnfpc']).cast(T.DecimalType(18, 2))
                                                            )\
                                                        .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18,2))\
                                                    )


    beneficiaries_struct = F.struct(\
                            F.when((df_lfkludta_lfppml_beneficiary_df['row_num'].isNotNull()) | (df_lfkludta_lfppml_beneficiary_df['row_num']!=0)\
                                    , df_lfkludta_lfppml_beneficiary_df['row_num']\
                                    ).otherwise(lit(1)).cast(T.IntegerType()).alias('beneficiary_seq_no')\
                            ,F.when(df_lfkludta_lfppml_beneficiary_df['beneficiary_name'].isNotNull()\
                                    , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkludta_lfppml_beneficiary_df['beneficiary_name'],1,100)),\
                                                "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                ),"\\\\", "")\
                                    ).otherwise(lit(REQ_NULL_STR)).cast(T.StringType()).alias('beneficiary_name')\
                            ,F.when(df_lfkludta_lfppml_beneficiary_df['beneficiary_relation'].isNotNull()\
                                    , df_lfkludta_lfppml_beneficiary_df['beneficiary_relation']\
                                    ).otherwise(lit('99')).cast(T.StringType()).alias('beneficiary_relation')\
                            ,F.when((df_lfkludta_lfppml_beneficiary_df['beneficiary_relation'] == '99')&(df_lfkludta_lfppml_beneficiary_df['bnrelatn'].isNotNull())\
                                    , df_lfkludta_lfppml_beneficiary_df['bnrelatn']\
                                    )
                                .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('beneficiary_relation_other_detail')\
                            ,F.when((df_lfkludta_lfppml_beneficiary_df['benefit_ratio'].isNotNull()) & (F.trim(df_lfkludta_lfppml_beneficiary_df['benefit_ratio']) != 'null')\
                                    , df_lfkludta_lfppml_beneficiary_df['benefit_ratio'].cast(T.DecimalType(18, 2))\
                                    ).otherwise(lit(NON_REQ_NULL_NUM)).cast(T.DecimalType(18, 2)).alias('benefit_ratio')\
                            )\

    df_lfkludta_lfppml_beneficiary_df = df_lfkludta_lfppml_beneficiary_df.groupBy('pno')\
                                                                            .agg(F.collect_list(\
                                                                                F.when((df_lfkludta_lfppml_beneficiary_df['row_num'].isNotNull()), beneficiaries_struct)
                                                                                .otherwise(lit(None))
                                                                                ).alias('beneficiaries')\
                                                                                )




    df_lfkludta_lfppml_beneficiary_df = df_lfkludta_lfppml_beneficiary_df\
                                    .withColumn('beneficiary_pno', df_lfkludta_lfppml_beneficiary_df['pno'])

    print('--Starting Ordinary Policy Endorsement...')
    print('----Preparing Policy Change by merging current with history...')
    df_lfkludta_lfpchgh_union = df_lfkludta_lfpchg_source.union(df_lfkludta_lfpchgh_source)\
                                                    .withColumnRenamed('pno', 'pno_chg')\
                                                    .select(['pno_chg','cref','ctc', 'ctstu' ,'cdot'])\
                                                    .orderBy('cdot')

    df_lfkludta_lfpchgh_union  = df_lfkludta_lfpchgh_union.groupBy(['pno_chg','cref','ctc', 'ctstu'])\
                                                    .agg(F.max('cdot').alias('max_cdot'))\
                                                    .filter((F.trim(df_lfkludta_lfpchgh_union['ctc']).isin('2', '4', '5', '6'))\
                                                            & (F.trim(df_lfkludta_lfpchgh_union['ctstu']) == '09'))\

    print('----Starting Aggregation Policy Endorsement...')
    df_lfkludta_lfppml_endorsement = df_lfkludta_lfppml_source.select('pno')\
                                .join(df_lfkludta_lfpchgh_union,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfpchgh_union['pno_chg']),"left")\

    endorsements_struct = F.struct(\
        F.when(F.trim(df_lfkludta_lfppml_endorsement['pno_chg']).isNotNull()\
                , df_lfkludta_lfppml_endorsement['cref'])\
        .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
        .alias('endorse_id')\
        ,F.when(F.trim(df_lfkludta_lfppml_endorsement['pno_chg']).isNotNull()\
                , lit('01'))\
        .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
        .alias('endorse_type_1')\
        ,F.when(F.trim(df_lfkludta_lfppml_endorsement['pno_chg']).isNull()\
                , lit(REQ_NULL_STR).cast(T.StringType()))\
        .when(F.trim(df_lfkludta_lfppml_endorsement['pno_chg']) == '2'\
                , lit('01'))\
        .otherwise(lit('02'))\
        .cast(T.StringType()).alias('endorse_type_2')\
        ,lit(REQ_NULL_STR).cast(T.StringType()).alias('endorse_sub_pol_id')\
        ,F.when(F.trim(df_lfkludta_lfppml_endorsement['pno_chg']).isNull()\
                , lit(NON_REQ_NULL_STR).cast(T.StringType()))\
        .when(F.trim(df_lfkludta_lfppml_endorsement['ctc']) == '4'\
                , lit('Reinstatement Date'))\
        .when(F.trim(df_lfkludta_lfppml_endorsement['ctc']) == '2'\
                , lit('Finance'))\
        .when(F.trim(df_lfkludta_lfppml_endorsement['ctc']) == '5'\
                , lit('ETI'))\
        .when(F.trim(df_lfkludta_lfppml_endorsement['ctc']) == '6'\
                , lit('RPU'))\
        .cast(T.StringType()).alias('endorse_detail')\
        ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_status')\
        )

    df_lfkludta_lfppml_endorsement = df_lfkludta_lfppml_endorsement.groupBy('pno', 'pno_chg')\
                                                                            .agg(F.collect_list(\
                                                                                F.when(F.trim(df_lfkludta_lfppml_endorsement['pno_chg']).isNotNull(), endorsements_struct)\
                                                                                .otherwise(lit(None))
                                                                                ).alias('endorsements')\
                                                                                )

    df_lfkludta_lfppml_endorsement = df_lfkludta_lfppml_endorsement\
                                    .withColumn('endorsement_pno', df_lfkludta_lfppml_endorsement['pno'])

    print('--Starting Ordinary Policy Loan...')

    print(
        '----Preparing Policy Activities by merging current with history and additional condition...'
    )

    df_lfkludta_lfpact_merge = df_lfkluhst_lfpacth_initial_source.union(df_lfkludta_lfpact1_source).dropDuplicates()
    df_lfkludta_lfpact_loan_merge = df_lfkludta_lfpact_merge.filter(
        df_lfkludta_lfpact_merge['actcd'].isin('LA', 'AW'))
    df_lfkludta_lfpact_loan_payment_merge = df_lfkludta_lfpact_merge.filter((F.trim(df_lfkludta_lfpact_merge['actcd']).isin('DCR', 'LR'))\
                                                                            & (F.trim(df_lfkludta_lfpact_merge['acln']) > 0)\
                                                                            )

    df_lfkludta_lfpact_loan_payment_merge = df_lfkludta_lfpact_loan_payment_merge.withColumn("row_num_payment", F.row_number().over(Window.partitionBy(\
                                                                                                    df_lfkludta_lfpact_merge['acpno'], 
                                                                                                    df_lfkludta_lfpact_merge['actcd'])\
                                                    .orderBy(df_lfkludta_lfpact_merge['aceff'].desc()\
                                                            )\
                                                    )\
                                                ).filter(F.col('row_num_payment')=='1').drop('row_num_payment')

    print('----Starting Aggregation Policy Loan...')
    df_lfkludta_lfppml_loan = df_lfkludta_lfppml_source.select('pno', 'lplan', 'lloan', 'llni')\
                                    .join(df_lfkludta_lfpact_loan_merge,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfpact_loan_merge['acpno']),"left")\
                                    .join(df_lfklutab_tabplan_source,F.trim(df_lfkludta_lfppml_source['lplan']) ==  F.trim(df_lfklutab_tabplan_source['planc']),"inner")\
                                    .withColumn("row_num"\
                                                , F.row_number()\
                                                .over(Window.partitionBy(df_lfkludta_lfppml_source['pno'], df_lfkludta_lfpact_loan_merge['acpno'])\
                                                    .orderBy(df_lfkludta_lfpact_merge['aceff'].desc()\
                                                            )\
                                                    )\
                                                )\
                                    .filter(F.col('row_num')=='1')

    loans_struct = F.struct(
                    F.when(df_lfkludta_lfppml_loan['acpno'].isNotNull()\
                    ,F.trim(df_lfkludta_lfppml_loan['row_num']).cast(T.StringType())\
                    ).otherwise(lit(REQ_NULL_STR)).cast(T.StringType()).alias('loan_id')\
                    ,F.when(F.trim(df_lfkludta_lfppml_loan['actcd']) == 'LA', lit('APL'))\
                    .when(F.trim(df_lfkludta_lfppml_loan['actcd']) == 'AW', lit('PL'))\
                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()).alias('loan_type')\
                    ,F.when(df_lfkludta_lfppml_loan['aceff'].isNotNull()\
                    ,concat(substring(F.trim(df_lfkludta_lfppml_loan['aceff']), 1, 4)\
                    , lit('-')\
                    , substring(F.trim(df_lfkludta_lfppml_loan['aceff']), 5, 2)\
                    , lit('-')\
                    ,substring(F.trim(df_lfkludta_lfppml_loan['aceff']), 7, 2)\
                    , lit('T00:00:00+07:00')\
                    )\
                    ).otherwise(lit(REQ_NULL_DATE)).cast(T.StringType()).alias('loan_date')\
                    ,F.when(df_lfkludta_lfppml_loan['acpno'].isNotNull()\
                    ,F.trim(df_lfkludta_lfppml_loan['pllnr']).cast(T.DecimalType(18, 2))\
                    ).otherwise(lit(REQ_NULL_DEC)).alias('loan_interest_rate')\
                    ,F.when(df_lfkludta_lfppml_loan['acpno'].isNotNull()\
                    ,F.trim((F.col("lloan").cast(T.DecimalType(18, 2)) + F.col("llni").cast(T.DecimalType(18, 2)))).cast(T.DecimalType(18, 2))\
                    ).otherwise(lit(REQ_NULL_DEC)).alias('loan_amount')\
                    ,F.when(df_lfkludta_lfppml_loan['acpno'].isNotNull()\
                    ,F.trim(df_lfkludta_lfppml_loan['llni']).cast(T.DecimalType(18, 2))\
                    ).otherwise(lit(REQ_NULL_DEC)).alias('loan_interest_amount')\
                    ,F.when((F.col("actcd").isin("AW", "LA")) & ((F.col("lloan").cast(T.DecimalType(18, 2)) + F.col("llni").cast(T.DecimalType(18, 2))) < 0), "01")\
                    .when((F.col("actcd").isin("AW", "LA")) & 
                        ((F.col("lloan").cast(T.DecimalType(18, 2)) + F.col("llni").cast(T.DecimalType(18, 2))) >= 0) &
                        ((F.col("acdvd").cast(T.DecimalType(18, 2)) + F.col("acdvdi").cast(T.DecimalType(18, 2))) == 0)
                        ,"03")\
                    .otherwise(REQ_NULL_STR).cast(T.StringType()).alias('loan_status')\
    )

    df_lfkludta_lfppml_loan = df_lfkludta_lfppml_loan.groupBy('pno')\
                                                    .agg(F.collect_list(\
                                                                        F.when(\
                                                                            (df_lfkludta_lfppml_loan['acpno'].isNotNull()) &
                                                                            (
                                                                                (F.col("actcd").isin("AW", "LA")) &
                                                                                (
                                                                                    (F.col("lloan").cast(T.DecimalType(18, 2)) + F.col("llni").cast(T.DecimalType(18, 2)) < 0) |
                                                                                    (
                                                                                        (F.col("lloan").cast(T.DecimalType(18, 2)) + F.col("llni").cast(T.DecimalType(18, 2)) >= 0) &
                                                                                        (F.col("acdvd").cast(T.DecimalType(18, 2)) + F.col("acdvdi").cast(T.DecimalType(18, 2)) == 0)
                                                                                    )
                                                                                )
                                                                            ),
                                                                            loans_struct
                                                                        )\
                                                                        .otherwise(lit(None))
                                                                        ).alias('loans')\
                                                        )

    df_lfkludta_lfppml_loan = df_lfkludta_lfppml_loan\
                            .withColumn('loan_pno', df_lfkludta_lfppml_loan['pno'])

    print('--Starting Ordinary Policy Loan Payment...')

    df_lfkludta_lfppml_loan_payment = df_lfkludta_lfppml_source.select('pno', 'lplan')\
                                    .join(df_lfkludta_lfpact_loan_payment_merge,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfpact_loan_payment_merge['acpno']),"left")\

    loan_payments_struct = F.struct(\
                        F.when(df_lfkludta_lfppml_loan_payment['acdcr'].isNotNull()\
                        ,F.trim(df_lfkludta_lfppml_loan_payment['acdcr'].cast(T.StringType()))\
                        ).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('loan_payment_id')
                        ,F.when(df_lfkludta_lfppml_loan_payment['aceff'].isNotNull()\
                        ,concat(substring(F.trim(df_lfkludta_lfppml_loan_payment['aceff']), 1, 4)\
                        , lit('-')\
                        , substring(F.trim(df_lfkludta_lfppml_loan_payment['aceff']), 5, 2)\
                        , lit('-')\
                        ,substring(F.trim(df_lfkludta_lfppml_loan_payment['aceff']), 7, 2)\
                        , lit('T00:00:00+07:00')\
                        )\
                        ).otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType()).alias('loan_payment_date')\
                        ,F.when((df_lfkludta_lfppml_loan_payment['acdcr'].isNotNull())\
                        ,(F.trim(df_lfkludta_lfppml_loan_payment['acln']) + F.trim(df_lfkludta_lfppml_loan_payment['aclni'])).cast(T.DecimalType(18, 2))\
                        ).otherwise(lit(NON_REQ_NULL_NUM)).alias('loan_payment_amount')\
                        ,F.when(df_lfkludta_lfppml_loan_payment['acdcr'].isNotNull()\
                        ,F.trim(df_lfkludta_lfppml_loan_payment['aclni']).cast(T.DecimalType(18, 2))\
                        ).otherwise(lit(NON_REQ_NULL_NUM)).alias('loan_payment_interest')\
                        ,F.when(df_lfkludta_lfppml_loan_payment['acdcr'].isNotNull()\
                        ,F.trim(df_lfkludta_lfppml_loan_payment['acln']).cast(T.DecimalType(18, 2))\
                        ).otherwise(lit(NON_REQ_NULL_NUM)).alias('loan_payment_principal')\
                        ) 

    df_lfkludta_lfppml_loan_payment = df_lfkludta_lfppml_loan_payment.groupBy('pno')\
                                                    .agg(F.collect_list(\
                                                                        F.when(df_lfkludta_lfppml_loan_payment['acdcr'].isNotNull(),loan_payments_struct)
                                                                        .otherwise(lit(None))
                                                                        ).alias('loan_payments')\
                                                        )
    df_lfkludta_lfppml_loan_payment = df_lfkludta_lfppml_loan_payment\
                            .withColumn('loan_payment_pno', df_lfkludta_lfppml_loan_payment['pno'])

    print('--Starting Ordinary Policy Investment...')
    print('----Preparing fund mastar map with fund transactional...')

    df_t_fund_master_source = df_t_fund_master_source.withColumn('OIC_Inv_fund_type',\
                                                                F.when(F.trim(F.col('investment_policy')) == 'กองทุนที่มีการลงทุนในทรัพย์สินทางเลือก', '08')\
                                                                .when(F.trim(F.col('investment_policy')) == 'กองทุนรวมตราสารหนี้', '04')\
                                                                .when(F.trim(F.col('investment_policy')) == 'กองทุนรวมตลาดเงินที่ลงทุนเฉพาะในประเทศ', '01')\
                                                                .when(F.trim(F.col('investment_policy')) == 'กองทุนรวมพันธบัตรรัฐบาล', '03')\
                                                                .when(F.trim(F.col('investment_policy')) == 'กองทุนรวมตราสารทุน', '06')\
                                                                .when(F.trim(F.col('investment_policy')) == 'กองทุนรวมผสม', '05')\
                                                                .otherwise('09'))

    df_lfkludta_lfpcdbfbi_fund_map = df_lfkludta_lfpcdbfbi_source \
                            .join(df_t_fund_master_source,F.trim(df_lfkludta_lfpcdbfbi_source['q47code']) ==  F.trim(df_t_fund_master_source['code']),"inner")

    print('----Preparing fund codes...')
    df_lfkludta_lfpuftdh_merge_mstr = df_lfkludta_lfpuftdh_source.alias('fund_txn')\
                                .withColumn('cast_m2amtu', df_lfkludta_lfpuftdh_source['m2amtu'].cast(T.DecimalType(26,4)))\
                                .withColumn('cast_m2amtp', df_lfkludta_lfpuftdh_source['m2amtp'].cast(T.DecimalType(26,4)))\
                                .withColumn('m2ptyp', 
                                            F.when(F.col('m2ptyp').isin(['P','I']), lit('B'))\
                                            .when(F.col('m2ptyp').isin(['R','O']), lit('S'))
                                        )

    df_lfkludta_lfpuftdh_merge_mstr = df_lfkludta_lfpuftdh_merge_mstr.alias('fund_txn')\
                                .select(['m2pno','m2code', 'm2tdat', 'm2ptyp', 'cast_m2amtu', 'cast_m2amtp'])\
                                .filter(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2code']) != 'UL901')\
                                .groupBy(['m2pno','m2code', 'm2tdat', 'm2ptyp'])\
                                .agg(F.sum('cast_m2amtu').alias('sum_m2amtu')\
                                    ,F.sum('cast_m2amtp').alias('sum_m2amtp'))

    print('--Starting Aggregation Policy Investment...')
    cond_fundcode = [ (F.trim(col("fund_txn.m2code"))                 ==  F.trim(col("fund_mstr.code")))\
            ]


    df_lfkludta_lfpuftdh_merge_mstr = df_lfkludta_lfpuftdh_merge_mstr.alias('fund_txn')\
                                    .join(df_t_fund_master_source.alias('fund_mstr'),cond_fundcode,"inner")\
                                    .groupBy('m2pno', 'm2tdat', 'm2ptyp')\
                                    .agg(F.collect_list(\
                                                    F.struct(\
                                                                F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                            ,F.trim(df_t_fund_master_source['code_name'])\
                                                                            .cast(T.StringType())\
                                                                    ).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('inv_fund_code')\
                                                                , F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                                ,F.trim(df_lfkludta_lfpuftdh_merge_mstr['sum_m2amtu'])\
                                                                                .cast(T.DecimalType(26, 2))\
                                                                        ).otherwise(lit(NON_REQ_NULL_NUM)).cast(T.DecimalType(26, 2)).alias('inv_fund_unit')\
                                                                , F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                                ,F.trim(df_lfkludta_lfpuftdh_merge_mstr['sum_m2amtp'])\
                                                                                .cast(T.DecimalType(26, 2))\
                                                                        ).otherwise(lit(NON_REQ_NULL_NUM)).cast(T.DecimalType(26, 2)).alias('inv_fund_nav')\
                                                                , F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                                ,substring(F.trim(df_t_fund_master_source['fund_name_th']), 1, 60)\
                                                                        ).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('inv_fund_thai_name')\
                                                                , F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                                ,substring(F.trim(df_t_fund_master_source['fund_name_en']), 1, 60)\
                                                                        ).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('inv_fund_eng_name')\
                                                                , F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                                ,F.trim(df_t_fund_master_source['OIC_Inv_fund_type'])\
                                                                        ).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('inv_fund_type')\
                                                                , F.when(F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull()\
                                                                                ,F.trim(df_t_fund_master_source['asset_management_company'])\
                                                                        ).otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('inv_fund_amc')\
                                                                , F.when((F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']).isNotNull())
                                                                        ,lit('01'))\
                                                                .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('inv_fund_data_status')\
                                                            )\
                                                    ).alias('policy_fund_codes')\
                                        )

    df_lfkludta_lfppml_investment = df_lfkludta_lfppml_source.select('pno')\
                                .join(df_lfkludta_lfpuftdh_merge_mstr,F.trim(df_lfkludta_lfppml_source['pno']) ==  F.trim(df_lfkludta_lfpuftdh_merge_mstr['m2pno']),"left")

    investments_struct = F.struct(\
                            F.when(df_lfkludta_lfppml_investment['m2pno'].isNotNull()\
                            ,concat(substring(F.trim(df_lfkludta_lfppml_investment['m2tdat']), 1, 4)\
                            , lit('-')\
                            , substring(F.trim(df_lfkludta_lfppml_investment['m2tdat']), 5, 2)\
                            , lit('-')\
                            ,substring(F.trim(df_lfkludta_lfppml_investment['m2tdat']), 7, 2)\
                            , lit('T00:00:00+07:00')\
                            )\
                            ).otherwise(lit(REQ_NULL_DATE)).cast(T.StringType()).alias('inv_transaction_date')\
                            ,F.when(F.trim(df_lfkludta_lfppml_investment['m2ptyp']).isin('P', 'I', 'B'), lit('B'))\
                            .when(F.trim(df_lfkludta_lfppml_investment['m2ptyp']).isin('R', 'O', 'S'), lit('S'))\
                            .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()).alias('inv_transaction_type')\
                            ,F.when(df_lfkludta_lfppml_investment['policy_fund_codes'].isNotNull()\
                            , df_lfkludta_lfppml_investment['policy_fund_codes']\
                            ).otherwise(\
                                    F.array(\
                                    F.struct(\
                                    lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_code')\
                                    ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('inv_fund_unit')\
                                    ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('inv_fund_nav')\
                                    ,lit(REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_thai_name')\
                                    ,lit(REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_eng_name')\
                                    ,lit(REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_type')\
                                    ,lit(REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_amc')\
                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_data_status')\
                                    )\
                                    )\
                            ).alias('policy_fund_codes')
                        )

    df_lfkludta_lfppml_investment = df_lfkludta_lfppml_investment.groupBy('pno')\
                                                    .agg(F.collect_list(\
                                                                        F.when(df_lfkludta_lfppml_investment['m2pno'].isNotNull(),investments_struct)
                                                                        .otherwise(lit(None))
                                                                        ).alias('investments')\
                                                        )

    df_lfkludta_lfppml_investment = df_lfkludta_lfppml_investment\
                                .withColumn('invest_pno',df_lfkludta_lfppml_investment['pno'])

    print('--Consolidating every elements to final Policy Dataframe...')

    print('policy customer :', policy_customer_df.count())
    print('df_lfkludta_lfpucom4_sub_policies_df :', df_lfkludta_lfpucom4_sub_policies_df.count())
    print('df_lfkludta_lfppml_beneficiary_df :', df_lfkludta_lfppml_beneficiary_df.count())
    print('df_lfkludta_lfppml_endorsement :', df_lfkludta_lfppml_endorsement.count())
    print('df_lfkludta_lfppml_loan :', df_lfkludta_lfppml_loan.count())
    print('df_lfkludta_lfppml_loan_payment :', df_lfkludta_lfppml_loan_payment.count())
    print('df_lfkludta_lfppml_investment :', df_lfkludta_lfppml_investment.count())

    output_policy_df_final = output_policy_df\
                            .join(policy_customer_df,F.trim(output_policy_df['pol_id']) ==  F.trim(policy_customer_df['cust_pno']),"inner")\
                            .join(df_lfkludta_lfpucom4_sub_policies_df,F.trim(output_policy_df['pol_id']) ==  F.trim(df_lfkludta_lfpucom4_sub_policies_df['sub_pol']),"inner")\
                            .join(df_lfkludta_lfppml_beneficiary_df,F.trim(output_policy_df['pol_id']) ==  F.trim(df_lfkludta_lfppml_beneficiary_df['beneficiary_pno']),"inner")\
                            .join(df_lfkludta_lfppml_endorsement,F.trim(output_policy_df['pol_id']) ==  F.trim(df_lfkludta_lfppml_endorsement['endorsement_pno']),"inner")\
                            .join(df_lfkludta_lfppml_loan,F.trim(output_policy_df['pol_id']) ==  F.trim(df_lfkludta_lfppml_loan['loan_pno']),"inner")\
                            .join(df_lfkludta_lfppml_loan_payment,F.trim(output_policy_df['pol_id']) ==  F.trim(df_lfkludta_lfppml_loan_payment['loan_payment_pno']),"inner")\
                            .join(df_lfkludta_lfppml_investment,F.trim(output_policy_df['pol_id']) ==  F.trim(df_lfkludta_lfppml_investment['invest_pno']),"inner")\

    output_policy_df_final = output_policy_df_final.drop(
        'cust_pno', 'cpno', 'sub_pol', 'pno', 'beneficiary_pno',
        'endorsement_pno', 'loan_pno', 'loan_payment_pno', 'invest_pno',
        'pno_chg')    

    if(run_params.workflow_type == 'Daily') :
        print('Writing data lastest transaction date to start_date_ref_table...')
            
        # Write start_date_ref_table 
        result_df = write_start_date_ref_table(
            spark_session=spark_session,
            source_table="processing_ibs.ibs_lfkludta_lfppml",
            s3_path="s3://kt-th-dl-prod-ibs/data/spec4/aggregation/start_date_ref_table",
            # workflow_run_date="20251223",  # optionally provide; else uses Bangkok local date
            header=False,
            mode="append",
            coalesce_to_one=True,
        )   
    
    return output_policy_df_final
