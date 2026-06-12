'''
job name     : IBS Ordinary Claim Utilities module.
Created by   : Natchanon Thepyasuwan
Created date : 6 Mar 2022
version      : 0.1
'''

from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.window import *


def ordinary_claim_aggregation(run_params, spark_session, start_date, end_date):
    print('--Reading source tables...')
    df_lfkludta_lfpclm_source = spark_session.table(
        'processing_ibs.ibs_lfkludta_lfpclm')\
            .filter((F.col('cldot') >= start_date) & (F.col('cldot') < end_date))\
            .filter(F.trim(F.col('clstu')) != 'C')
    if(run_params.workflow_type == 'Daily') :
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')
    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml').filter((F.col('pstu').isin('1','2','5','6','B','F')))
    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')\
                .filter(~F.col('pstu').isin('1','2','5','6','B','F'))

    print('Declaring Dimproduct Basic by map with ref table...')
    df_dimproduct_basic_source = spark_session.table(
        'processing_ibs.ibs_dimproduct_basic')
    df_ref_policy_policy_product_type_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_policy_product_type')
    df_lfklutab_tabplan_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_tabplan')
    df_dimproduct_basic_source_map_ref = df_dimproduct_basic_source \
                                        .join(df_ref_policy_policy_product_type_source,F.trim(df_dimproduct_basic_source['actuarial_type_cate']) ==  F.trim(df_ref_policy_policy_product_type_source['kt_axa']), "left")\
                                        .join(df_lfklutab_tabplan_source,F.trim(df_dimproduct_basic_source['lplan']) ==  F.trim(df_lfklutab_tabplan_source['planc']),"left")\
                                        .withColumnRenamed('kt_axa', 'policy_product_type_ktaxa_code')\
                                        .withColumnRenamed('oic_code', 'policy_product_type_oic_code')\
                                        .withColumn('policy_type2_oic_code', F.when(df_lfklutab_tabplan_source['pland'].like('%ชนิดมีเงินปันผล%'), 'PL')\
                                                                            .otherwise('NP'))

    df_lfkludta_lfppml_col = df_lfkludta_lfppml_source.select('pno','pid', 'ldvdo','lplan')
    df_lfkludta_lfppml_col = df_lfkludta_lfppml_col.join(df_dimproduct_basic_source_map_ref,
                                                        F.trim(df_lfkludta_lfppml_col['lplan']) ==  F.trim(df_dimproduct_basic_source_map_ref['lplan']),
                                                        "left")\
                                                    .select(df_lfkludta_lfppml_col['*'], df_dimproduct_basic_source_map_ref['pland'])
    df_lfkludta_lfpbnfy = spark_session.table('processing_ibs.ibs_lfkludta_lfpbnfy')
    df_lfkludta_lfpbnfy = df_lfkludta_lfpbnfy.select('bnpno', 'bnbnff', 'bnbnfy','bnbnfpc', 'bnupd')
    df_lfkludta_lfpbnfy_max = df_lfkludta_lfpbnfy.groupBy('bnpno').agg(F.max('bnbnfpc').alias('max_bnbnfpc'),F.max('bnupd').alias('max_bnupd'))
    df_lfkludta_lfpbnfy_col = df_lfkludta_lfpbnfy.join(df_lfkludta_lfpbnfy_max,
                                                (df_lfkludta_lfpbnfy['bnpno'] == df_lfkludta_lfpbnfy_max['bnpno']) &
                                                (df_lfkludta_lfpbnfy['bnbnfpc'] == df_lfkludta_lfpbnfy_max['max_bnbnfpc']) &
                                                (df_lfkludta_lfpbnfy['bnupd'] == df_lfkludta_lfpbnfy_max['max_bnupd']),
                                                how='inner').select(df_lfkludta_lfpbnfy['*'])

    # Step 4: รวมชื่อและนามสกุล แล้ว group โดย bnpno
    df_lfkludta_lfpbnfy_col = df_lfkludta_lfpbnfy_col \
        .withColumn('full_name', F.concat_ws(' ', F.trim(F.col('bnbnff')), F.trim(F.col('bnbnfy')))).groupBy('bnpno', 'bnbnfpc', 'bnupd')\
        .agg(F.concat_ws(', ', F.collect_list('full_name')).alias('claim_payment_beneficiary_name'))

    df_claim_header_source = spark_session.table('processing_ibs.ibs_claim_header')\
        .filter(
            (F.col('notifydate') >= start_date) &
            (F.col('notifydate') < end_date) &
            ~F.col('pol_no').startswith('8') &
            ~F.col('pol_no').startswith('C') &
            ~F.col('pol_no').startswith('L')
        )
    df_claim_details_payment_source = spark_session.table(
        'processing_ibs.ibs_claim_details_payment')\
            .filter((F.col('notifydate') >= start_date) & (F.col('notifydate') < end_date))
    df_payment_details = spark_session.table(
        'processing_ibs.ibs_payment_details')\
            .filter((F.col('notifydate') >= start_date) & (F.col('notifydate') < end_date))

    df_claim_status_source = spark_session.table(
        'processing_ibs.ibs_claim_status')
    df_lfklutab_lfpicdes_source = spark_session.table(
        'processing_ibs.ibs_lfklutab_lfpicdes')
    df_benefit_master_source = spark_session.table(
        'processing_ibs.ibs_benefit_master')
    df_ref_coverage_health_source = spark_session.table(
    'processing_ibs.ibs_ref_coverage_health')
    df_ref_hospital_code = spark_session.table(
        'processing_ibs.ibs_ref_hospital_code')
    df_ref_claim_status = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_status')
    df_ref_major_claim_status = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_status_major_claim')
    df_ref_claim_coverage = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_coverage')

    df_ref_coverage_md = spark_session.table(
    "processing_ibs.ibs_ref_coverage_md")

    df_ref_coverage_code = spark_session.table(
        'processing_ibs.ibs_ref_policy_coverage_code')
    df_ref_policy_claim_payment_channel = spark_session.table(
        'processing_ibs.ibs_ref_policy_claim_payment_channel')

    df_ref_oic_claim_coverage = spark_session.table(
        'processing_ibs.ibs_ref_oic_claim_coverage')
    df_ref_policy_service_md_ref = spark_session.table(
        'processing_ibs.ibs_ref_policy_service_md_ref')

    df_ref_claim_causes_of_death = spark_session.table(
        'processing_ibs.ibs_ref_claim_causes_of_death')

    df_licensedoctorname = spark_session.table(
        'processing_ibs.ibs_licensedoctorname')\
        .select('licensedoctor', 'doctorname')\
        .withColumnRenamed('licensedoctor', 'ref_licensedoctor') \

    md_transaction_status = df_ref_policy_service_md_ref.filter(
        F.col('md_name') == 'transaction_status')
    md_claim_type = df_ref_policy_service_md_ref.filter(
        F.col('md_name') == 'claim_type')
    md_policy_type = df_ref_policy_service_md_ref.filter(
        F.col('md_name') == 'policy_type')

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = None

    NON_REQ_NULL_STR = None
    NON_REQ_NULL_NUM = None
    NON_REQ_NULL_DATE = None

    df_ref_claim_status = df_ref_claim_status.withColumnRenamed('claimsts', 'ref_claimsts') \
                                            .withColumnRenamed('claimstsgroup', 'ref_claimstsgroup') \
                                            .withColumnRenamed('claimstsdes', 'ref_claimstsdes') \
                                            .withColumnRenamed('oic_code', 'oic_code_claimsts')

    df_ref_major_claim_status = df_ref_major_claim_status.withColumnRenamed('clstu', 'ref_clstu') \
                                                        .withColumnRenamed('description', 'ref_claim_description') \
                                                        .withColumnRenamed('oic_code', 'oic_code_claimsts')

    df_ref_claim_coverage = df_ref_claim_coverage.withColumnRenamed('cltype', 'ref_cltype') \
                                                .withColumnRenamed('cl_type_description', 'ref_cl_type_description') \
                                                .withColumnRenamed('oic_code', 'oic_code_claimcoverage')\

    df_ref_claim_coverage = df_ref_claim_coverage.join(df_ref_coverage_md, df_ref_claim_coverage.oic_code_claimcoverage == df_ref_coverage_md.oic_cov_code, 'left')    

    df_ref_claim_causes_of_death = df_ref_claim_causes_of_death.select('clicde_causes_of_death', 'oic_causes_of_death')

    df_payment_details = df_payment_details.withColumnRenamed(
        'notifyno', 'pmt_notifyno_temp')
    df_payment_details = df_payment_details.select('pmt_notifyno_temp',
                                                'paymentno', 'paid_type',
                                                'paid_date')

    df_ref_policy_claim_payment_channel = df_ref_policy_claim_payment_channel.withColumnRenamed('paid_type', 'paid_type_temp') \
                                                                            .withColumnRenamed('oic_code', 'oic_code_claim_pmt_channel')
    print('--Transforming data...')
    df_lfklutab_lfpicdes_cleaned = df_lfklutab_lfpicdes_source.filter(
        F.col('icsect') == 'CLM')
    df_lfkludta_lfpclm_source = df_lfkludta_lfpclm_source \
                                .join(df_lfklutab_lfpicdes_cleaned,
                                    df_lfkludta_lfpclm_source.clicde == df_lfklutab_lfpicdes_cleaned.iccde, 'left') \
                                .join(df_ref_major_claim_status,
                                    df_lfkludta_lfpclm_source.clstu == df_ref_major_claim_status.ref_clstu, 'left') \
                                .withColumn("cltype_join_key", F.when(F.col("cltype").like("HP%"), "HP%").otherwise(F.col("cltype")))\

    df_lfkludta_lfpclm_source = df_lfkludta_lfpclm_source.join(df_ref_claim_coverage,
                                F.trim(df_lfkludta_lfpclm_source.cltype_join_key) == F.trim(df_ref_claim_coverage.ref_cltype), 'left')

    df_lfkludta_lfpclm_source = df_lfkludta_lfpclm_source \
                                .join(df_ref_claim_causes_of_death,
                                    df_lfkludta_lfpclm_source.clicde == df_ref_claim_causes_of_death.clicde_causes_of_death, 'left')

    df_claim_header_source = df_claim_header_source \
                            .join(df_claim_status_source,
                                df_claim_header_source.clm_cstatus == df_claim_status_source.claimsts, "left") \
                            .join(df_ref_claim_status,
                                df_claim_header_source.clm_cstatus == df_ref_claim_status.ref_claimsts, "left") \
                            .join(df_payment_details,
                                df_claim_header_source.notifyno == df_payment_details.pmt_notifyno_temp, "left")
    
    df_claim_header_source = df_claim_header_source.filter(F.col('claimstsgroup')!='Cancelled')

    df_claim_header_source = df_claim_header_source \
                            .join(df_ref_policy_claim_payment_channel,
                                df_claim_header_source.paid_type==df_ref_policy_claim_payment_channel.paid_type_temp, "left")

    df_claim_details_payment_source = df_claim_details_payment_source.withColumnRenamed(
        'notifyno', 'notifyno_temp')
    df_claim_details_payment_col = df_claim_details_payment_source.select(
        'notifyno_temp', 'benefitid', 'maxbendtmamount', 'netapproveamount',
        'actualamount')
    df_benefit_master_col = df_benefit_master_source.select(
        'benefit_cd', 'limit_type_cd', 'benefit_th_name', 'benefit_limit')

    df_benefit_master_source = df_benefit_master_source.join(df_ref_coverage_health_source,
        (F.trim(F.col('benefit_en_name')) == F.trim(F.col('map_benefit_en_name'))) &
        (F.trim(F.col('benefit_th_name')) == F.trim(F.col('map_benefit_th_name'))) &
        (F.trim(F.col('benefit_group_name')) == F.trim(F.col('map_benefit_group_name'))) &
        (F.trim(F.col('benefit_group_name_eng')) == F.trim(F.col('map_benefit_group_name_eng'))),"inner")\

    df_benefit_master_source = df_benefit_master_source.join(df_ref_coverage_md, df_benefit_master_source.map_benefit_oic_code == df_ref_coverage_md.oic_cov_code, 'left')

    df_claim_header_col = df_claim_header_source.select('notifyno')

    df_claim_header_claim_coverage_code = df_claim_header_col \
                                        .join(df_claim_details_payment_source,
                                            df_claim_header_col.notifyno == df_claim_details_payment_col.notifyno_temp, 'left') \
                                        .join(df_benefit_master_source,
                                            df_claim_details_payment_col['benefitid'].cast("int") == regexp_extract(df_benefit_master_source['benefit_cd'],'[0-9]+', 0).cast("int"), 'left')\

    cov_plan_cov_code_expr = (
    F.when((F.col('oic_cov_code').isNotNull()), 
        F.col('oic_cov_code'))\
    .otherwise(F.lit('C9B35')).cast(T.StringType()))

    cov_plan_cov_detail_expr = (
        F.when((F.col('oic_cov_code')!='C9B35')&(F.col('oic_cov_detail_th').isNotNull()), F.col('oic_cov_detail_th'))\
        .when((F.col('oic_cov_code')=='C9B35')&(F.trim(F.col('oic_cov_detail_th'))=='B0035 อื่น ๆ โปรดระบุ………'), F.concat(F.lit('B0036 - '), F.trim(F.col('benefit_th_name'))))\
        .when((F.col('oic_cov_detail_th').isNull()) & (F.col('benefit_th_name').isNotNull()), F.concat(F.lit('B0036 - '), F.trim(F.col('benefit_th_name'))))\
        .when((F.col('oic_cov_code').isNull())&(F.col('benefit_th_name').isNull()), F.lit('benefit detail จะแสดงเมื่อสถานะ claim = paid'))\
        .otherwise(F.concat(F.lit('B0036 - '), F.trim(F.col('benefit_th_name')))).cast(T.StringType()))

    df_claim_header_claim_coverage_code = df_claim_header_claim_coverage_code.groupby('notifyno')\
                                                                            .agg(F.collect_list(\
                                                                                                F.struct(\
                                                                                                        cov_plan_cov_code_expr.alias('claim_coverage_code'),
                                                                                                        cov_plan_cov_detail_expr.alias('claim_coverage_detail'),

                                                                                                        F.when(((F.col('maxbendtmamount'))==(F.col('benefit_limit'))) & (F.trim(F.col('limit_type_cd')).isin(['AY','ADA'])),
                                                                                                                F.col('maxbendtmamount').cast(T.DecimalType(18,2))) \
                                                                                                        .otherwise(lit(REQ_NULL_DEC))\
                                                                                                        .cast(T.DecimalType(18,2)).alias('claim_sum_insured_per_coverage_per_year'),

                                                                                                        F.when(((F.col('maxbendtmamount'))==(F.col('benefit_limit'))) & (F.trim(F.col('limit_type_cd')).isin(['AD','APT'])),
                                                                                                                F.col('maxbendtmamount').cast(T.DecimalType(18,2))) \
                                                                                                        .otherwise(lit(REQ_NULL_DEC))\
                                                                                                        .cast(T.DecimalType(18,2)).alias('claim_sum_insured_per_coverage_per_time'),

                                                                                                        F.when(F.col('netapproveamount').isNotNull(), F.col('netapproveamount').cast(T.DecimalType(18,2)))
                                                                                                        .otherwise(lit(NON_REQ_NULL_NUM))\
                                                                                                        .alias('claim_coverage_amt'),

                                                                                                        lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_deductible_amt'),

                                                                                                            F.when(F.col('actualamount').isNotNull(), F.col('actualamount').cast(T.DecimalType(18,2)))
                                                                                                        .otherwise(lit(NON_REQ_NULL_NUM))\
                                                                                                        .alias('claim_amount'),

                                                                                                        F.array(\
                                                                                                                    F.struct(\
                                                                                                                        lit('105090000').cast(T.StringType()).alias('claim_pol_coverage_detail_simb2'),
                                                                                                                        lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_pol_coverage_detail_simb2_amt'),
                                                                                                                    )
                                                                                                                ).alias('simb2s')
                                                                                                        )
                                                                                                ).alias('claim_coverages')
                                                                                )\
                                                                            .withColumnRenamed('notifyno', 'notifyno_temp')

    df_lfkludta_lfpclm_col = df_lfkludta_lfpclm_source
    df_lfkludta_lfpclm_col    = df_lfkludta_lfpclm_col      .withColumn('claim_type', lit(None)) \
                                                            .withColumn('claim_flag', lit('death_claim')) \
                                                            .withColumn('admitdate', lit(None).cast(T.StringType())) \
                                                            .withColumn('dischdate', lit(None).cast(T.StringType())) \
                                                            .withColumn('hosp_name', lit(None).cast(T.StringType())) \
                                                            .join(df_lfkludta_lfppml_col, df_lfkludta_lfpclm_col['clpno']==df_lfkludta_lfppml_col['pno'], 'inner')
                                                            
    df_lfkludta_lfpclm_col = df_lfkludta_lfpclm_col.join(df_lfkludta_lfpbnfy_col, df_lfkludta_lfpclm_col['clpno']==df_lfkludta_lfpbnfy_col['bnpno'], 'inner')

    df_lfkludta_lfpclm_col = df_lfkludta_lfpclm_col.select('claim_flag',
        'clclm','cladj', 'clpno', 'cldoi', 'claim_type','ldvdo', 'admitdate', 'cldor',
        'dischdate', 'hosp_name', 'cldot', 'clstu', 'cllter',
        lit(None).cast(T.StringType()).alias('ld_mainstatus'),
        lit(None).cast(T.StringType()).alias('claimstsgroup'),
        lit(None).cast(T.StringType()).alias('clm_cstatus'),
        lit(None).cast(T.StringType()).alias('claimsts'),
        lit(None).cast(T.StringType()).alias('claimstsdes'), 'clrea', 'cltype', 'clicde', 'oic_causes_of_death',
        'pid', 
        lit(None).cast(T.StringType()).alias('icd_10'),
        lit(None).cast(T.StringType()).alias('icd_09'), 'oic_code_claimsts',
        'oic_cov_code', 'oic_cov_detail_th', 'clpay',
        lit(None).cast(T.StringType()).alias('paymentno'),
        lit(None).cast(T.StringType()).alias('oic_code_claim_pmt_channel'),
        lit(None).cast(T.StringType()).alias('paid_date'),
        lit(None).cast(T.StringType()).alias('actualbill'),
        lit(None).cast(T.StringType()).alias('totnetappamt'),
        'claim_payment_beneficiary_name',
        lit(None).cast(T.StringType()).alias('doctorname'),
        'pland')
    df_lfkludta_lfpclm_col   = df_lfkludta_lfpclm_col.withColumn('claim_coverages',F.array(\
                                                                                        F.struct(\
                                                                                                F.when((F.col('claim_flag')=='death_claim') & (F.trim(F.col('oic_cov_code')).isNotNull()), F.trim(F.col('oic_cov_code')))\
                                                                                                .otherwise(lit('C9016'))\
                                                                                                .cast(T.StringType()).alias('claim_coverage_code'),

                                                                                                F.when((F.col('claim_flag')=='death_claim') & (F.trim(F.col('oic_cov_code')).isNotNull()), F.trim(F.col('oic_cov_detail_th')))\
                                                                                                .when((F.col('claim_flag')=='death_claim') & (F.trim(F.col('oic_cov_code')).isNull()), F.concat(F.lit('A0023 - '), F.trim(F.col('pland'))))\
                                                                                                .otherwise(F.concat(F.lit('A0023 - '), F.trim(F.col('pland'))))\
                                                                                                .cast(T.StringType()).alias('claim_coverage_detail'),

                                                                                                lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('claim_sum_insured_per_coverage_per_year'),
                                                                                                lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('claim_sum_insured_per_coverage_per_time'),

                                                                                                F.when(F.col('clpay').isNotNull(), F.col('clpay').cast(T.DecimalType(18,2)))
                                                                                                .otherwise(lit(NON_REQ_NULL_NUM)).alias('claim_coverage_amt'),

                                                                                                lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_deductible_amt'),

                                                                                                F.when(F.col('clpay').isNotNull(), F.col('clpay').cast(T.DecimalType(18,2)))
                                                                                                .otherwise(lit(NON_REQ_NULL_NUM)).alias('claim_amount'),

                                                                                                F.array(\
                                                                                                        F.struct(\
                                                                                                                lit('105090000').cast(T.StringType()).alias('claim_pol_coverage_detail_simb2'),
                                                                                                                lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_pol_coverage_detail_simb2_amt'),
                                                                                                                )
                                                                                                        ).alias('simb2s')
                                                                                                )
                                                                                        )
                                                                )

    df_claim_header_col = df_claim_header_source
    df_claim_header_col = df_claim_header_col.withColumn('claim_type', lit(None))\
                                            .withColumn('claim_flag', lit('health_claim')) \
                                            .join(df_lfkludta_lfppml_col, df_claim_header_col['pol_no']==df_lfkludta_lfppml_col['pno'], 'inner')
    df_claim_header_col = df_claim_header_col.join(df_lfkludta_lfpbnfy_col, df_claim_header_col['pol_no']==df_lfkludta_lfpbnfy_col['bnpno'], 'inner')\
                                            .join(df_licensedoctorname, df_claim_header_col['licensedoctor']==df_licensedoctorname['ref_licensedoctor'], 'left')

    df_claim_header_col = df_claim_header_col.select('claim_flag',
        'notifyno', lit(None).cast(T.StringType()).alias('cladj'),'pol_no', 'acc_date', 'claim_type','ldvdo', 'admitdate',
        'notifydate', 'dischdate', 'hosp_name', 'assess_date',
        lit(None).cast(T.StringType()).alias('clstu'), 
        lit(None).cast(T.StringType()).alias('cllter'), 
        'ld_mainstatus', 'claimstsgroup',
        'clm_cstatus','claimsts', 'claimstsdes',
        lit(None).cast(T.StringType()).alias('clrea'),
        lit(None).cast(T.StringType()).alias('cltype'),
        lit(None).cast(T.StringType()).alias('clicde'),
        lit(None).cast(T.StringType()).alias('oic_causes_of_death'),
        'cust_id',
        lit(None).cast(T.StringType()).alias('icd_10'),
        lit(None).cast(T.StringType()).alias('icd_09'), 'oic_code_claimsts',
        lit(None).cast(T.StringType()).alias('oic_cov_code'),
        lit(None).cast(T.StringType()).alias('oic_cov_detail_th'),
        lit(None).cast(T.StringType()).alias('clpay'), 'paymentno', 'oic_code_claim_pmt_channel',
        'paid_date', 'actualbill', 'totnetappamt',
        'claim_payment_beneficiary_name','doctorname',
        lit(None).cast(T.StringType()).alias('pland'))
    df_claim_header_col = df_claim_header_col.join(
        df_claim_header_claim_coverage_code, df_claim_header_col.notifyno ==
        df_claim_header_claim_coverage_code.notifyno_temp,
        'left').drop('notifyno_temp')

    union_df = df_lfkludta_lfpclm_col.union(df_claim_header_col)

    #hospital field
    df_ref_hospital_code = df_ref_hospital_code.select(
        'code', 'th_name').dropDuplicates(['th_name'])
    union_df = union_df.join(
        df_ref_hospital_code,
        union_df.hosp_name == df_ref_hospital_code.th_name, "left")

    claim_refer_code_of_company_prefix = "CLM_KAL_"
    claim_pol_refer_code_of_company_prefix = "POL_KAL_"
    output_df = union_df \
                .withColumn('claim_refer_code_of_company', 
                            F.when(
                                    F.col('cladj').isNull(),
                                    F.concat(
                                        F.lit(claim_refer_code_of_company_prefix),
                                        F.trim(F.col('clclm'))
                                    )
                                ).otherwise(
                                    concat(
                                        F.lit(claim_refer_code_of_company_prefix),
                                        F.trim(F.col('clclm')),
                                        F.lit("_"),
                                        F.trim(F.col('cladj'))
                                    )
                                )\
                            .cast(T.StringType())) \
                .withColumn('claim_transaction_status', lit('N').cast(T.StringType())) \
                .withColumn('claim_company_id', lit('1022').cast(T.StringType())) \
                .withColumn('claim_pol_id', F.trim(union_df['clpno']).cast(T.StringType())) \
                .withColumn('claim_pol_refer_code_of_company', concat(lit(claim_pol_refer_code_of_company_prefix), F.trim(union_df['clpno'])).cast(T.StringType())) \
                .withColumn('claim_id', F.trim(union_df['clclm']).cast(T.StringType())) \
                .withColumn('claim_type', 
                            F.when(F.trim(F.col('clclm')).isNotNull(), lit('01'))\
                            .when((F.trim(F.col('clclm')).isNull())&(F.col('ldvdo')=='1'), lit('02'))\
                            .otherwise('99')
                        )\
                .withColumn('claim_no', F.trim(union_df['clclm']).cast(T.StringType())) \
                .withColumn('claim_pol_type', lit('PT-05').cast(T.StringType())) \
                .withColumn('claim_incurred_date', F.when((F.col('claim_flag')=='death_claim') & (F.col('cldoi').isNotNull()) & (F.col('cldoi')!='0'),
                                                        concat(substring(F.trim(F.col('cldoi')), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(F.col('cldoi')), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(F.col('cldoi')), 7, 2)\
                                                        , lit('T00:00:00+07:00')))
                                                    .when((F.col('claim_flag')=='health_claim') & (F.col('admitdate').isNotNull()) & (F.col('admitdate')!='0'),
                                                        concat(substring(F.trim(F.col('admitdate')), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(F.col('admitdate')), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(F.col('admitdate')), 7, 2)\
                                                        , lit('T00:00:00+07:00')))
                                                    .otherwise(lit(REQ_NULL_DATE)).cast(T.StringType())
                        ) \
                .withColumn('claim_report_date', F.when((F.col('cldor').isNotNull()) & (F.col('cldor').isNotNull()) & (F.col('cldor')!='0'),
                                                        concat(substring(F.trim(F.col('cldor')), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(F.col('cldor')), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(F.col('cldor')), 7, 2)\
                                                        , lit('T00:00:00+07:00'))
                                                    )
                                                .otherwise(lit(REQ_NULL_DATE)).cast(T.StringType())
                        ) \
                .withColumn('claim_notify_date', F.when((F.col('claim_flag')=='death_claim') & (F.col('cldoi').isNotNull()) & (F.col('cldoi')!='0'),
                                                        concat(substring(F.trim(F.col('cldoi')), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(F.col('cldoi')), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(F.col('cldoi')), 7, 2)\
                                                        , lit('T00:00:00+07:00')))
                                                    .when((F.col('claim_flag')=='health_claim') & (F.col('admitdate').isNotNull()) & (F.col('admitdate')!='0'),
                                                        concat(substring(F.trim(F.col('admitdate')), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(F.col('admitdate')), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(F.col('admitdate')), 7, 2)\
                                                        , lit('T00:00:00+07:00')))
                                                    .otherwise(lit(REQ_NULL_DATE)).cast(T.StringType())
                        ) \
                .withColumn('claim_discharge_date', F.when((F.col('dischdate').isNotNull()) & (F.col('dischdate').isNotNull()) & (F.col('dischdate')!='0'),
                                                        concat(substring(F.trim(F.col('dischdate')), 1, 4)\
                                                        , lit('-')\
                                                        , substring(F.trim(F.col('dischdate')), 5, 2)\
                                                        , lit('-')\
                                                        ,substring(F.trim(F.col('dischdate')), 7, 2)\
                                                        , lit('T00:00:00+07:00'))
                                                        )
                                                    .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType())
                        ) \
                .withColumn('claim_hospital_id', F.when((F.col('code').isNotNull()) | (F.col('code')!=''), F.col('code'))
                                                    .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())) \
                .withColumn('claim_hospital_name',  F.when((F.col('th_name').isNotNull()) | (F.col('th_name')!=''), 
                                                                            F.regexp_replace(F.regexp_replace(F.trim(F.substring((F.col('th_name')), 1, 100)),
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""),"\\\\", "")\
                                                                            .cast(T.StringType()))
                                                    .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())) \
                .withColumn('claim_doctor_name', F.col('doctorname').cast(T.StringType())) \
                .withColumn('claim_completion_date', F.when((F.col('claim_flag')=='death_claim') & (F.col('clstu').isin(['P','R'])) & (F.col('cldot').isNotNull()) & (F.col('cldot')!='0'),
                                                            concat(substring(F.trim(F.col('cldot')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('cldot')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('cldot')), 7, 2)\
                                                                , lit('T00:00:00+07:00'))
                                                        )
                                                    .when((F.col('claim_flag')=='health_claim') & (~(F.col('claimstsgroup').isin(["Pending","Accept","Reclaim"]))) & (F.col('ld_mainstatus').isNotNull()) & (F.col('ld_mainstatus')!='0') ,
                                                        concat(substring(F.trim(F.col('ld_mainstatus')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('ld_mainstatus')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('ld_mainstatus')), 7, 2)\
                                                                , lit('T00:00:00+07:00'))
                                                        )
                                                    .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType())
                        ) \
                .withColumn('claim_close_date', F.when((F.col('claim_flag')=='death_claim') & (F.col('clstu').isin(['P','R'])) & (F.col('cldot').isNotNull()) & (F.col('cldot')!='0'),
                                                    concat(substring(F.trim(F.col('cldot')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('cldot')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('cldot')), 7, 2)\
                                                                , lit('T00:00:00+07:00'))
                                                    )
                                                .when((F.col('claim_flag')=='health_claim') & (~(F.col('claimstsgroup').isin(["Pending","Accept","Reclaim"]))) & (F.col('ld_mainstatus').isNotNull()) & (F.col('ld_mainstatus')!='0') ,
                                                    concat(substring(F.trim(F.col('ld_mainstatus')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('ld_mainstatus')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('ld_mainstatus')), 7, 2)\
                                                                , lit('T00:00:00+07:00'))
                                                    )
                                                .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType())
                        ) \
                .withColumn('claim_status', F.when((F.col('clstu')=='S'), lit('03'))\
                                            .when((F.trim(F.col('clstu'))=='P'), lit('01'))\
                                            .when((F.trim(F.col('clstu'))=='R'), lit('05'))\
                                            .when(F.trim(F.col('clstu')).isNull(), F.col('oic_code_claimsts'))
                                            .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())) \
                .withColumn('claim_reject_reason', F.when(F.trim(F.col('claim_status'))=='05', 
                                                        F.when(F.lower(F.trim(F.col('clrea'))).like('%wait%180%'), lit('03'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%wait%120%'), lit('03'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%wait%'), lit('02'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%รอคอย%'), lit('02'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%ยกเว้นทั่วไป%'), lit('04'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%ยกเว้น%'), lit('07'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%exclus%'), lit('07'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%void%'), lit('08'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%exist%'), lit('08'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%non%disclos%'), lit('08'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%fraud%'), lit('08'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%ก่อน%ประกัน%'), lit('08'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%ปกปิด%'), lit('08'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%suicide%'), lit('09'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%ฆ่าตัวตาย%'), lit('09'))\
                                                        .when(F.lower(F.trim(F.col('clrea'))).like('%ผู้รับประโยชน์%'), lit('11'))\
                                                        .when(F.trim(F.col('claimsts')) == 'D1', lit('03'))\
                                                        .when(F.trim(F.col('claimsts')).isin('D3', 'D32'), lit('07'))\
                                                        .when(F.trim(F.col('claimsts')).isin('D12', 'D2', 'R6'), lit('08'))
                                                        .otherwise(lit('99'))
                                                        )
                                                .otherwise(lit(None)).cast(T.StringType())
                        ) \
                .withColumn('claim_document_link', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                .withColumn('claim_causes_of_death', F.when((F.trim(F.col('cltype'))=='DEATH') & (F.col('oic_causes_of_death').isNotNull()), F.trim(F.col('oic_causes_of_death')))
                                                    .otherwise(lit('98')).cast(T.StringType())
                        ) \
                .withColumn('claim_oic_case_id', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                .withColumn('claim_oic_case_detail', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                .withColumn('claim_request_channel', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                .withColumn('claim_insrd_ids', 
                            F.array(\
                                                F.when(F.col('pid').isNotNull(), F.col('pid').cast(T.StringType()))
                                                .otherwise(None).cast(T.StringType())
                                                )
                            )\
                .withColumn('claim_icd_10_codes', F.array(lit("OTHER99")))\
                .withColumn('claim_icd_9_codes', F.array(lit("OTHER99")))

    claim_payments_struct = F.array(F.struct(\
                                                    F.when((F.col('clstu').isin(['P'])), F.trim(union_df['clclm']))
                                                    .when(F.col('paymentno').isNotNull(), F.trim(F.col('paymentno')))
                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()).alias('claim_payment_id'),
                                                    lit('01').cast(T.StringType()).alias('claim_payment_type'),
                                                    lit(1).cast(T.IntegerType()).alias('claim_payment_term'),
                                                    lit(1).cast(T.IntegerType()).alias('claim_payment_term_paid'),
                                                    F.when((F.col('claim_flag')=='death_claim'), lit('99'))\
                                                    .when((F.col('claim_flag')=='health_claim') & (F.col('oic_code_claim_pmt_channel').isNotNull()),
                                                        F.col('oic_code_claim_pmt_channel'))
                                                    .otherwise(lit("99")).cast(T.StringType()).alias('claim_payment_channel'),

                                                    F.when((F.col('claim_flag')=='death_claim') & (F.col('clstu').isin(['P'])) & (F.col('cldot').isNotNull()) & (F.col('cldot')!='0'),
                                                        F.trim(concat(substring(F.trim(F.col('cldot')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('cldot')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('cldot')), 7, 2)\
                                                                , lit('T00:00:00+07:00')))
                                                        )
                                                    .when((F.col('claim_flag')=='health_claim') & (F.col('paid_date').isNotNull()) & (F.col('paid_date').isNotNull()) & (F.col('paid_date')!='0'),
                                                        F.trim(concat(substring(F.trim(F.col('paid_date')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('paid_date')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('paid_date')), 7, 2)\
                                                                , lit('T00:00:00+07:00')))
                                                        )
                                                    .otherwise(lit(REQ_NULL_DATE)).cast(T.StringType()).alias('claim_payment_date'),
                                                    F.when((F.col('clstu').isin(['P'])) & (F.col('cldot').isNotNull()) & (F.col('cldot')!='0'),
                                                        F.trim(concat(substring(F.trim(F.col('cldot')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('cldot')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('cldot')), 7, 2)\
                                                                , lit('T00:00:00+07:00')))
                                                        )
                                                    .when(F.col('paid_date').isNotNull() & (F.col('paid_date').isNotNull()) & (F.col('paid_date')!='0'),
                                                        F.trim(concat(substring(F.trim(F.col('paid_date')), 1, 4)\
                                                                , lit('-')\
                                                                , substring(F.trim(F.col('paid_date')), 5, 2)\
                                                                , lit('-')\
                                                                ,substring(F.trim(F.col('paid_date')), 7, 2)\
                                                                , lit('T00:00:00+07:00')))
                                                        )
                                                    .otherwise(lit(REQ_NULL_DATE)).cast(T.StringType()).alias('claim_payment_account_date'),
                                                    F.when((F.col('claim_flag')=='death_claim') & (F.col('clstu').isin(['P'])), F.col('clpay').cast(T.DecimalType(18,2)))
                                                    .when((F.col('claim_flag')=='health_claim'), F.col('actualbill').cast(T.DecimalType(18,2)))
                                                    .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18,2)).alias('claim_payment_invoice_amt'),
                                                    F.when((F.col('claim_flag')=='death_claim') & (F.col('clstu').isin(['P'])), F.col('clpay').cast(T.DecimalType(18,2)))
                                                    .when((F.col('claim_flag')=='health_claim'), F.col('totnetappamt').cast(T.DecimalType(18,2)))
                                                    .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18,2)).alias('claim_payment_amt'),
                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_payment_other_invoice_amt'),
                                                    F.when(F.col('claim_payment_beneficiary_name').isNotNull(), F.col('claim_payment_beneficiary_name'))\
                                                    .otherwise(lit('ยังไม่ระบุผู้รับผลประโยชน์')).cast(T.StringType()).alias('claim_payment_beneficiary_name')
                                                ))

    output_df = output_df.withColumn('claim_payments',
                                        F.when((F.col('clstu').isin(['P'])), claim_payments_struct)\
                                        .when(F.col('paymentno').isNotNull(), claim_payments_struct)\
                                        .otherwise(F.array())
                                    )

    estimates_struct = F.array(F.struct(\
                F.when((F.col('claim_id').isNotNull()), lit(1))\
                .otherwise(lit(REQ_NULL_INT))
                .cast(T.IntegerType()).alias('claim_estimate_seq'),
                lit(output_df['claim_incurred_date']).cast(T.StringType()).alias('claim_estimate_date'),
                F.when((F.col('claim_flag')=='death_claim') & (F.col('clstu').isin(['P'])), F.col('clpay').cast(T.DecimalType(18,2)))
                .when((F.col('claim_flag')=='health_claim'), F.col('totnetappamt').cast(T.DecimalType(18,2)))
                .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18,2)).alias('claim_estimate_amount')
                ))
    output_df = output_df.withColumn('estimates', 
                F.when((F.col('claim_id').isNotNull()), estimates_struct)\
                .otherwise(F.array()) 
    )

    output_df = output_df \
    .select('claim_refer_code_of_company',
            'claim_transaction_status',
            'claim_company_id',
            'claim_pol_id',
            'claim_pol_refer_code_of_company',
            'claim_id',
            'claim_no',
            'claim_type',
            'claim_pol_type',
            'claim_incurred_date',
            'claim_report_date',
            'claim_notify_date',
            'claim_discharge_date',
            'claim_hospital_id',
            'claim_hospital_name',
            'claim_doctor_name',
            'claim_completion_date',
            'claim_close_date',
            'claim_status',
            'claim_reject_reason',
            'claim_document_link',
            'claim_causes_of_death',
            'claim_oic_case_id',
            'claim_oic_case_detail',
            'claim_request_channel',
            'claim_insrd_ids',
            'claim_icd_10_codes',
            'claim_icd_9_codes',
            'claim_coverages',
            'estimates',
            'claim_payments'
        )

    return output_df
