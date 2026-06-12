'''
job name     : IBS PA Policy Utilities module.
Created by   : Kiatipoom Poopanitpun
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
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.window import *


def pa_policy_aggregation(run_params, spark_session, start_date, end_date):
    print('Reading data source transactional tables...')
    if(run_params.workflow_type == 'Daily') :
        df_lfkpa_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkpa_lfppml')\
                    .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
                    .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
                    .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                            ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date)))
                           )\
                    .drop('lstdate_cleaned', 'pdoi_cleaned')
    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_lfkpa_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkpa_lfppml')\
                    .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
                    .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
                    .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                            ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date))) &
                            (F.col('pstu').isin('1'))\
                           )\
                    .drop('lstdate_cleaned', 'pdoi_cleaned')
    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_lfkpa_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkpa_lfppml')\
                    .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
                    .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
                    .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                            ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date))) &
                            (~F.col('pstu').isin('1'))\
                           )\
                    .drop('lstdate_cleaned', 'pdoi_cleaned')

    df_lfkpa_application_source = spark_session.table(
        'processing_ibs.ibs_lfkpa_application')
    df_lfkpa_application_source = df_lfkpa_application_source.withColumnRenamed('lstdate', 'dcrdetail_lstdate')
    df_lfkpa_application_source = df_lfkpa_application_source \
                .join(df_lfkpa_lfppml_source, df_lfkpa_application_source['pno']==df_lfkpa_lfppml_source['pno'], 'left') \
                .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
                .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
                .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                        ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date))) \
                        )\
                .select(df_lfkpa_application_source['*'])


    print('Reading data source master tables...')
    df_lfkpa_plan_source = spark_session.table(
        'processing_ibs.ibs_lfkpa_plan')
    df_pa_status_source = spark_session.table('processing_ibs.ibs_pa_status')
    print('Reading ref tables...')
    df_ref_pa_coverage = spark_session.table('processing_ibs.ibs_ref_pa_coverage')
    df_ref_policy_service_md_ref_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_service_md_ref')
    df_ref_pa_pol_sub_type_lserff = spark_session.table(
        'processing_ibs.ibs_ref_pa_pol_sub_type_lserff')
    ibs_ref_location_code                       = spark_session.table('processing_ibs.ibs_ref_location_code')
    ibs_ref_province_location_code              = spark_session.table('processing_ibs.ibs_ref_province_location_code')
    ibs_ref_district_location_code              = spark_session.table('processing_ibs.ibs_ref_district_location_code')
    ibs_ref_sub_district_location_code          = spark_session.table('processing_ibs.ibs_ref_sub_district_location_code')
    ibs_ref_pa_policy_location_map              = spark_session.table('processing_ibs.ibs_ref_pa_policy_location_map')

    print('Declaring Default Params Value...')
    ##default code parameters.
    pol_refer_code_of_company_prefix = "POL_"
    pol_transaction_status = "N"
    pol_company_id = "1022"
    pol_type = "PT-09"
    pol_product_type = 'PPT-08-1'
    pol_payment_period = '04'
    pol_distribution = '03'
    cov_deductible_amount = '0.00'
    cov_cover_year = '1'
    cov_payment_year = '1'

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = '0000-00-00T00:00:00+07:00'

    NON_REQ_NULL_STR = 'null'
    NON_REQ_NULL_NUM = 'null'
    NON_REQ_NULL_DATE = 'null'

    print('Declaring Customer Gender by map with ref table...')
    df_ref_policy_service_md_ref_customer_gender = df_ref_policy_service_md_ref_source\
                                                    .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'customer_gender') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                    .withColumnRenamed('ktaxa_code', 'customer_gender_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'customer_gender_oic_code')

    df_ref_policy_service_md_ref_pol_beneficiary_relation = df_ref_policy_service_md_ref_source\
                                                        .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'pol_beneficiary_relation') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                        .withColumnRenamed('ktaxa_code', 'pol_beneficiary_relation_ktaxa_code')\
                                                        .withColumnRenamed('id_code', 'pol_beneficiary_relation_oic_code')

    df_ref_policy_service_md_ref_policy_status = df_ref_policy_service_md_ref_source\
                                                .filter(F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_status')\
                                                .withColumnRenamed('ktaxa_code', 'policy_status_ktaxa_code')\
                                                .withColumnRenamed('id_code', 'policy_status_oic_code')


    print('Preparing Location code dataframe for address columns...')

    ibs_ref_district_location_code = ibs_ref_district_location_code\
                                     .filter(F.trim(ibs_ref_district_location_code['district_th']) != '')

    ibs_ref_sub_district_location_code = ibs_ref_sub_district_location_code\
                                 .filter(F.trim(ibs_ref_sub_district_location_code['sub_district_th']) != '')

    df_lfkpa_lfppml_loc_prep = df_lfkpa_lfppml_source.alias('a').select('pno', 'insuprov', 'insudist')\
                                                 .join(ibs_ref_province_location_code.alias('b'),\
                                                      F.trim(col('a.insuprov'))\
                                                       .contains(F.trim(col('b.province_th')))\
                                                      ,'left')\
                                                 .join(ibs_ref_pa_policy_location_map.alias('c'),\
                                                      F.trim(col('a.insuprov')) == F.trim(col('c.pa_location_incorrect_th'))\
                                                      ,'left')\
                                                 .join(ibs_ref_district_location_code.alias('c'),\
                                                      F.trim(col('a.insudist'))\
                                                       .contains(F.concat(lit('อ.'),F.trim(col('c.district_th'))))\
                                                      ,'left')\
                                                 .join(ibs_ref_sub_district_location_code.alias('d'),\
                                                      F.trim(col('a.insudist'))\
                                                       .contains(F.concat(lit('ต.'),F.trim(col('d.sub_district_th'))))\
                                                      ,'left')\

    df_lfkpa_lfppml_loc_prep = df_lfkpa_lfppml_loc_prep.withColumn('output_province',\
                                                                   F.when(df_lfkpa_lfppml_loc_prep['oic_province_code'].isNotNull()\
                                                                            ,df_lfkpa_lfppml_loc_prep['province_th'])\
                                                                    .when(df_lfkpa_lfppml_loc_prep['pa_oic_province_code'].isNotNull()\
                                                                            ,df_lfkpa_lfppml_loc_prep['pa_location_correct_th']\
                                                                         )\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                                  )\
                                                        .withColumn('output_province_code',\
                                                                   F.when(df_lfkpa_lfppml_loc_prep['oic_province_code'].isNotNull()\
                                                                            ,df_lfkpa_lfppml_loc_prep['oic_province_code'])\
                                                                    .when(df_lfkpa_lfppml_loc_prep['pa_oic_province_code'].isNotNull()\
                                                                            ,df_lfkpa_lfppml_loc_prep['pa_oic_province_code']\
                                                                         )\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                                  )


    df_lfkpa_lfppml_loc_prep = df_lfkpa_lfppml_loc_prep.filter(\
                                                               ((df_lfkpa_lfppml_loc_prep['output_province_code'] == substring(df_lfkpa_lfppml_loc_prep['oic_district_code'],1,2)) &\
                                                               (df_lfkpa_lfppml_loc_prep['output_province_code'] == substring(df_lfkpa_lfppml_loc_prep['oic_sub_district_code'],1,2)) &\
                                                               (df_lfkpa_lfppml_loc_prep['oic_district_code'] == substring(df_lfkpa_lfppml_loc_prep['oic_sub_district_code'],1,4))
                                                               )
                                                              )

    df_lfkpa_lfppml_loc_prep = df_lfkpa_lfppml_loc_prep.withColumn('output_district_code', substring(df_lfkpa_lfppml_loc_prep['oic_district_code'],3,2))\
                                                       .withColumn('output_sub_district_code', substring(df_lfkpa_lfppml_loc_prep['oic_sub_district_code'],5,2))\
                                                       .withColumn("leven", F.levenshtein(col("insudist"), col("sub_district_th")) )\


    df_min_levenshtein = df_lfkpa_lfppml_loc_prep.groupBy('pno')\
                                                 .agg(F.min("leven").alias("min_leven"))\
                                                 .withColumnRenamed('pno', 'leven_pno')


    df_lfkpa_lfppml_loc_prep = df_lfkpa_lfppml_loc_prep.join(df_min_levenshtein, \
                                                         (df_lfkpa_lfppml_loc_prep['pno'] == df_min_levenshtein['leven_pno']) & \
                                                         (df_lfkpa_lfppml_loc_prep['leven'] == df_min_levenshtein['min_leven']), 'inner')\
                                                         .withColumnRenamed('pno', 'loc_pno')\
                                                         .select(df_lfkpa_lfppml_loc_prep['insuprov'],\
                                                                 df_lfkpa_lfppml_loc_prep['insudist'],\
                                                                 'sub_district_th',\
                                                                 'loc_pno',\
                                                                 'output_province_code',\
                                                                 'output_district_code',\
                                                                 'output_sub_district_code')


    print('Preparing Coverage Dataframe...')
    df_coverage_prep = df_lfkpa_plan_source\
                      .select('plancode'\
                              ,'plannameth'\
                              ,'plannameng'\
                              ,df_lfkpa_plan_source['paadb'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              ,df_lfkpa_plan_source['patpd'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              ,df_lfkpa_plan_source['paadd'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              ,df_lfkpa_plan_source['pame'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              ,df_lfkpa_plan_source['pama'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              ,df_lfkpa_plan_source['pamc'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              ,df_lfkpa_plan_source['weekbnf'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                             )
    df_coverage      = df_coverage_prep.selectExpr("plancode",\
                                                   "weekbnf",\
                                                   "plannameth",\
                                                   "plannameng",\
                                                   "stack(6,'PAADB', paadb,'PATPD', patpd,'PAADD', paadd,'PAME' , pame, 'PAMA' , pama,'PAMC', pamc) as (cov_code, cov_amt)")

    df_coverage = df_coverage.filter(df_coverage['cov_amt'] != '0.00')

    df_coverage_output  =  df_coverage\
                           .join(df_ref_pa_coverage,F.trim(df_coverage['cov_code']) ==  F.trim(df_ref_pa_coverage['cov_code']),"inner")\
                        .groupBy(df_coverage['plancode'], df_coverage['plannameth'], df_coverage['plannameng'])\
                        .agg(F.collect_list(\
                                           F.struct(\
                                                     F.trim(df_coverage['cov_code']).cast(T.StringType()).alias('cov_code')\
                                                     ,F.trim(df_ref_pa_coverage['cov_desc']).cast(T.StringType()).alias('cov_detail')\
                                                     ,F.trim(df_coverage['cov_amt']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_amt')\
                                                     ,F.trim(df_coverage['cov_amt']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_sum_insured_per_coverage_per_year')\
                                                     ,F.trim(df_coverage['weekbnf']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_sum_insured_per_coverage_per_time')\
                                                     ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cov_co_payment')\
                                                     ,lit(cov_deductible_amount).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_deductible_amount')\
                                                     ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_deductible_detail')\
                                                     ,F.trim(df_ref_pa_coverage['number_of_day']).cast(T.IntegerType()).cast(T.StringType()).alias('cov_number_of_day')\
                                                     ,lit(cov_cover_year).cast(T.IntegerType()).cast(T.StringType()).alias('cov_cover_year')\
                                                     ,lit(cov_payment_year).cast(T.IntegerType()).cast(T.StringType()).alias('cov_payment_year')\
                                                     ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_claim_status')\
                                                   )\
                                           ).alias('coverages')\
                              ,F.collect_list(\
                                           F.struct(\
                                                     F.trim(df_coverage['cov_code']).cast(T.StringType()).alias('cov_plan_cov_code')\
                                                     ,F.trim(df_ref_pa_coverage['cov_desc']).cast(T.StringType()).alias('cov_plan_cov_detail')\
                                                     ,F.trim(df_coverage['cov_amt']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_cov_amt')\
                                                     ,F.trim(df_coverage['cov_amt']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_cov_sum_insured_per_coverage_per_year')\
                                                     ,F.trim(df_coverage['weekbnf']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_cov_sum_insured_per_coverage_per_time')\
                                                     ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cov_plan_cov_co_payment')\
                                                     ,lit(cov_deductible_amount).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_cov_deductible_amount')\
                                                     ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_plan_cov_deductible_detail')\
                                                     ,F.trim(df_ref_pa_coverage['number_of_day']).cast(T.IntegerType()).cast(T.StringType()).alias('cov_plan_cov_number_of_day')\
                                                     ,lit(cov_cover_year).cast(T.IntegerType()).cast(T.StringType()).alias('cov_plan_cov_cover_year')\
                                                     ,lit(cov_payment_year).cast(T.IntegerType()).cast(T.StringType()).alias('cov_plan_cov_payment_year')\
                                                     ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_plan_cov_claim_status')\
                                                   )\
                                           ).alias('coverages_under_plans')\
                                )
    print('Preparing Coverage Dataframe...')

    df_beneficiaries_prep = df_lfkpa_lfppml_source.select('pno','bnfname_1'
    ,'bnfname_2'
    ,'bnfname_3'
    ,'bnfname_4'
    ,'bnfname_5'
    ,'bnfname_6'
    ,'bnfrt_1'
    ,'bnfrt_2'
    ,'bnfrt_3'
    ,'bnfrt_4'
    ,'bnfrt_5'
    ,'bnfrt_6'
    ,'bnfpct_1'
    ,'bnfpct_2'
    ,'bnfpct_3'
    ,'bnfpct_4'
    ,'bnfpct_5'
    ,'bnfpct_6')


    df_beneficiaries      = df_beneficiaries_prep.selectExpr('pno',"stack(6,'1', bnfname_1, bnfrt_1, bnfpct_1\
                                                                     ,'2', bnfname_2, bnfrt_2, bnfpct_2\
                                                                     ,'3', bnfname_3, bnfrt_3, bnfpct_3\
                                                                     ,'4', bnfname_4, bnfrt_4, bnfpct_4\
                                                                     ,'5', bnfname_5, bnfrt_5, bnfpct_5\
                                                                     ,'6', bnfname_6, bnfrt_6, bnfpct_6) \
                                                                     as (beneficiary_seq_no\
                                                                     , beneficiary_name\
                                                                     , beneficiary_relation_other_detail\
                                                                     , benefit_ratio)")
    df_beneficiaries  = df_beneficiaries.join(df_ref_policy_service_md_ref_pol_beneficiary_relation\
                                              ,F.trim(df_beneficiaries['beneficiary_relation_other_detail']) ==  F.trim(df_ref_policy_service_md_ref_pol_beneficiary_relation['pol_beneficiary_relation_ktaxa_code'])\
                                              ,"left")\
                                        .filter((df_beneficiaries['beneficiary_name'].isNotNull()))\
                                        .orderBy('pno', 'beneficiary_seq_no')

    # df_beneficiaries.select('benefit_ratio').filter(df_beneficiaries['benefit_ratio'] == '').show(200,False)

    df_beneficiaries = df_beneficiaries.withColumn('beneficiary_relation', \
                                                   F.when(df_beneficiaries['pol_beneficiary_relation_ktaxa_code'].isNotNull(),\
                                                         df_beneficiaries['pol_beneficiary_relation_oic_code'])\
                                                    .otherwise('99'))\
                                                    .select('pno', 'beneficiary_seq_no' ,'beneficiary_name',\
                                                            'beneficiary_relation', 'beneficiary_relation_other_detail',\
                                                            'benefit_ratio')


    df_beneficiaries      =  df_beneficiaries\
                            .groupBy(df_beneficiaries['pno'])\
                            .agg(F.collect_list(\
                                           F.struct(\
                                                     F.when((df_beneficiaries['beneficiary_seq_no'].isNotNull()) & (F.trim(df_beneficiaries['beneficiary_seq_no']) != '')\
                                                             , df_beneficiaries['beneficiary_seq_no']\
                                                           ).otherwise(lit(REQ_NULL_INT)).cast(T.IntegerType()).cast(T.StringType()).alias('beneficiary_seq_no')\
                                                     ,F.when((df_beneficiaries['beneficiary_name'].isNotNull()) & (F.trim(df_beneficiaries['beneficiary_name']) != '')\
                                                             , df_beneficiaries['beneficiary_name']\
                                                           ).otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                            .alias('beneficiary_name')\
                                                     ,F.when((df_beneficiaries['beneficiary_relation'].isNotNull()) & (F.trim(df_beneficiaries['beneficiary_relation']) != '')\
                                                             , df_beneficiaries['beneficiary_relation']\
                                                           ).otherwise(lit('99').cast(T.StringType()))\
                                                            .alias('beneficiary_relation')\
                                                     ,F.when((df_beneficiaries['beneficiary_relation_other_detail'].isNotNull()) & (F.trim(df_beneficiaries['beneficiary_relation_other_detail']) != '')\
                                                             , df_beneficiaries['beneficiary_relation_other_detail']\
                                                            ).otherwise(lit(NON_REQ_NULL_STR).cast(T.StringType())).alias('beneficiary_relation_other_detail')\
                                                     ,F.when((df_beneficiaries['benefit_ratio'].isNotNull()) & (F.trim(df_beneficiaries['benefit_ratio']) != '')\
                                                             , df_beneficiaries['benefit_ratio'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                                                            ).otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('benefit_ratio')\
                                                   )\
                                           ).alias('beneficiaries')\
                            )\
                            .withColumnRenamed('pno','beneficiaries_pno')




    print('Aggregating Output PA Policy table...')
    print('--Starting build Output PA Policy table...')

    output_policy_df = df_lfkpa_lfppml_source \
                   .join(df_lfkpa_application_source,F.trim(df_lfkpa_lfppml_source['pno']) ==  F.trim(df_lfkpa_application_source['pno']),"left")\
                   .join(df_pa_status_source,F.trim(df_lfkpa_lfppml_source['pstu']) ==  F.trim(df_pa_status_source['code']),"left")\
                   .join(df_coverage_output,F.upper(F.trim(df_lfkpa_lfppml_source['plancode'])) ==  F.trim(df_coverage_output['plancode']),"inner")\
                   .join(df_ref_policy_service_md_ref_policy_status, F.trim(df_lfkpa_lfppml_source['pstu']) ==  F.trim(df_ref_policy_service_md_ref_policy_status['policy_status_ktaxa_code']),"left")\
                   .join(df_beneficiaries,F.trim(df_lfkpa_lfppml_source['pno']) ==  F.trim(df_beneficiaries['beneficiaries_pno']),"left")\
                   .join(df_lfkpa_lfppml_loc_prep,F.trim(df_lfkpa_lfppml_source['pno']) ==  F.trim(df_lfkpa_lfppml_loc_prep['loc_pno']),"left")\
                   .join(df_ref_pa_pol_sub_type_lserff,F.upper(F.trim(df_lfkpa_lfppml_source['plancode'])) ==  F.trim(df_ref_pa_pol_sub_type_lserff['plan_code']),"left")\
                   .withColumn('pol_refer_code_of_company',concat(lit(pol_refer_code_of_company_prefix), F.trim(df_lfkpa_lfppml_source['pno'])).cast(T.StringType()))\
                   .withColumn('pol_transaction_status',lit(pol_transaction_status).cast(T.StringType())) \
                   .withColumn('pol_company_id',lit(pol_company_id).cast(T.StringType())) \
                   .withColumn('pol_id', F.trim(df_lfkpa_lfppml_source['pno']).cast(T.StringType())) \
                   .withColumn('pol_type', lit(pol_type).cast(T.StringType())) \
                   .withColumn('pol_type_gli_name', lit(NON_REQ_NULL_NUM).cast(T.StringType())\
                              )\
                   .withColumn('pol_type_gli_plan', lit(NON_REQ_NULL_NUM).cast(T.StringType())\
                              )\
                   .withColumn('pol_product_type', lit(pol_product_type).cast(T.StringType()))\
                   .withColumn('pol_product_type_other_detail', lit(REQ_NULL_STR).cast(T.StringType())\
                              )\
                   .withColumn('pol_type_2', lit('NP').cast(T.StringType())\
                              )\
                   .withColumn('pol_sub_type', F.when(F.trim(df_ref_pa_pol_sub_type_lserff['plan_code']).isNotNull()\
                                                                       , F.trim(df_ref_pa_pol_sub_type_lserff['lserff_code']))\
                                               .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()))\
                   .withColumn('pol_name', F.trim(df_coverage['plannameth']).cast(T.StringType())) \
                   .withColumn('pol_name_oic', F.trim(df_coverage['plannameth']).cast(T.StringType()))\
                   .withColumn('pol_start_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['peff']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                              )\
                   .withColumn('pol_end_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['ppytd']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                              )\
                   .withColumn('pol_claim_document', lit(NON_REQ_NULL_STR).cast(T.StringType())\
                              )\
                   .withColumn('pol_payment_term', concat(lit('กำหนดการชำระเบี้ยครั้งถัดไป วันที่ '),\
                                                          F.from_unixtime(df_lfkpa_lfppml_source['ppytd']/1000,'dd/MM/yyyy')\
                                                       ).cast(T.StringType())\
                              )\
                   .withColumn('pol_payment_amount', df_lfkpa_lfppml_source['pmprem'].cast(T.DecimalType(18, 2)).cast(T.StringType())\
                              )\
                   .withColumn('pol_payment_period', lit(pol_payment_period).cast(T.StringType())\
                              )\
                   .withColumn('pol_info_url', lit(NON_REQ_NULL_STR).cast(T.StringType())\
                              )\
                   .withColumn('pol_claim_url', lit(NON_REQ_NULL_STR).cast(T.StringType())\
                              )\
                   .withColumn('pol_document_urls', F.array()\
                              )\
                   .withColumn('pol_year', lit('1').cast(T.IntegerType()).cast(T.StringType())\
                              )\
                   .withColumn('pol_status', F.trim(df_ref_policy_service_md_ref_policy_status['policy_status_oic_code'])\
                               .cast(T.StringType())\
                              )\
                   .withColumn('pol_status_note', F.when(F.trim(df_lfkpa_application_source['pendingremark']) != ''\
                                                         , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_application_source['pendingremark'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                       ),"\\\\", "")\
                                                         )\
                                                   .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())
                              )\
                   .withColumn('pol_status_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['lstdate']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                              )\
                   .withColumn('pol_expropriation_value', lit(NON_REQ_NULL_NUM).cast(T.StringType())\
                              )\
                   .withColumn('pol_immediate_refund', lit(NON_REQ_NULL_NUM).cast(T.StringType())\
                              )\
                   .withColumn('pol_sale_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['appdate']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                               )\
                   .withColumn('pol_transaction_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['crtdate']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                               )\
                   .withColumn('pol_approved_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['pdoi']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                               )\
                   .withColumn('pol_issue_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['pdoi']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                               )\
                   .withColumn('pol_account_date', lit(NON_REQ_NULL_DATE).cast(T.StringType())\
                              )\
                   .withColumn('pol_agree_date', concat(F.from_unixtime(df_lfkpa_lfppml_source['peff']/1000,'yyyy-MM-dd'),\
                                                       lit('T00:00:00+07:00')\
                                                       ).cast(T.StringType())\
                               )\
                   .withColumn('pol_distribution', lit(pol_distribution).cast(T.StringType())\
                              )\
                   .withColumn('pol_distribution_other_detail', lit(NON_REQ_NULL_STR).cast(T.StringType())\
                              )\
                   .withColumn('pol_license', F.trim(df_lfkpa_lfppml_source['aglncbr']).cast(T.StringType())\
                              )\
                   .withColumn('pol_re_insured_contact_number', lit(NON_REQ_NULL_STR).cast(T.StringType())\
                              )\
                   .withColumn('pol_life_detail', F.when(F.trim(df_lfkpa_application_source['rejecmemo']) != ''\
                                                         , F.trim(df_lfkpa_application_source['rejecmemo']))\
                                                   .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())
                              )\
                   .withColumn("customer",\
                                            F.struct(\
                                                       F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != '' ,\
                                                             F.when(F.trim(df_lfkpa_lfppml_source['ownname'])\
                                                                    .like('^a-zA-Z') , 'F')\
                                                             .otherwise('P')\
                                                             )\
                                                        .when(F.trim(df_lfkpa_lfppml_source['ownname']) == '' ,\
                                                             F.when(\
                                                                   (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['orthidno']), '-', '') != '')\
                                                                 & (~F.trim(df_lfkpa_lfppml_source['orthidno']).like('%-%'))\
                                                                 & (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') == '') ,\
                                                                   'F')\
                                                              .otherwise('P')\
                                                             )\
                                                         .otherwise(lit('99'))\
                                                         .cast(T.StringType()).alias('cust_type')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_type_other_detail')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != '' ,\
                                                             F.when(F.trim(df_lfkpa_lfppml_source['ownname'])\
                                                                    .like('^a-zA-Z') , '02')\
                                                             .otherwise('01')\
                                                             )\
                                                        .when(F.trim(df_lfkpa_lfppml_source['ownname']) == '' ,\
                                                             F.when(\
                                                                   (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['orthidno']), '-', '') != '')\
                                                                 & (~F.trim(df_lfkpa_lfppml_source['orthidno']).like('%-%'))\
                                                                 & (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') == '') ,\
                                                                   '02')\
                                                              .otherwise('01')\
                                                             )\
                                                         .otherwise(lit('99'))\
                                                         .cast(T.StringType()).alias('cust_id_type')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != '' ,\
                                                                 F.trim(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['ownidno']), '-', ''))\
                                                              )\
                                                        .when(F.trim(df_lfkpa_lfppml_source['ownname']) == '' ,\
                                                             F.when(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') != '' ,\
                                                                      F.trim(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', ''))\
                                                                   )\
                                                              .otherwise(F.trim(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['orthidno']), '-', '')))\
                                                             )\
                                                         .otherwise(lit(REQ_NULL_STR))
                                                         .cast(T.StringType()).alias('cust_id')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_company_name')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != ''\
                                                                   , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['ownname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                        ),"\\\\", "")\
                                                                )\
                                                          .when(F.trim(df_lfkpa_lfppml_source['ownname']) == ''\
                                                                   , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insuname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                       ),"\\\\", "")\
                                                               )\
                                                           .otherwise(lit(REQ_NULL_STR))\
                                                           .cast(T.StringType()).alias('cust_first_name')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != ''\
                                                                  , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['ownlname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                       ),"\\\\", "")\
                                                               )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['ownname']) == ''\
                                                                  , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insulname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                       ),"\\\\", "")\
                                                              )\
                                                          .otherwise(lit(REQ_NULL_STR))\
                                                          .cast(T.StringType()).alias('cust_last_name')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != '' ,\
                                                                 F.trim(df_lfkpa_lfppml_source['ownage']).cast(T.IntegerType())\
                                                              )\
                                                        .when(F.trim(df_lfkpa_lfppml_source['ownname']) == '' ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insuage']).cast(T.IntegerType())\
                                                             )\
                                                         .otherwise(lit(NON_REQ_NULL_NUM))\
                                                         .cast(T.StringType()).alias('cust_age')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['ownname']) != '' ,\
                                                                 F.concat(F.from_unixtime(df_lfkpa_lfppml_source['insudob']/1000,'yyyy-MM-dd')\
                                                                 ,lit('T00:00:00+07:00'))\
                                                              )\
                                                        .when(F.trim(df_lfkpa_lfppml_source['ownname']) == '' ,\
                                                                 F.concat(F.from_unixtime(df_lfkpa_lfppml_source['insudob']/1000,'yyyy-MM-dd')\
                                                                 ,lit('T00:00:00+07:00'))\
                                                             )\
                                                        .cast(T.StringType()).alias('cust_birthday')\
                                                       ,F.when((F.trim(df_lfkpa_lfppml_source['ownname']) != '') & (F.trim(df_lfkpa_lfppml_source['ownsex']) != '') ,\
                                                                 F.when(F.trim(df_lfkpa_lfppml_source['ownsex']) == 'M','01')\
                                                                  .when(F.trim(df_lfkpa_lfppml_source['ownsex']) == 'F','02')\
                                                              )\
                                                        .when((F.trim(df_lfkpa_lfppml_source['ownname']) == '') & (F.trim(df_lfkpa_lfppml_source['insusex']) != ''),\
                                                                 F.when(F.trim(df_lfkpa_lfppml_source['insusex']) == 'M','01')\
                                                                  .when(F.trim(df_lfkpa_lfppml_source['insusex']) == 'F','02')\
                                                             )\
                                                         .otherwise('99')\
                                                         .cast(T.StringType()).alias('cust_gender')\
                                                       , F.trim(F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insuadd'],1,100)),\
                                                                                 "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                            ),"\\\\", ""))\
                                                          .cast(T.StringType()).alias('cust_address')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code'])\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_sub_district')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_district_code'])\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_district')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_province_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_province_code'])\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_province')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['insupost']) != '' ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insupost'])\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_zip_code')\
                                                       ,lit('THA').cast(T.StringType()).alias('cust_country_code')\
                                                       ,F.when((F.trim(df_lfkpa_lfppml_source['insuhtel']) != '') &\
                                                               (F.trim(df_lfkpa_lfppml_source['insuhtel']).isNotNull()) &\
                                                               (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuhtel']), '-', '') != '') ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insuhtel'])\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR))\
                                                        .cast(T.StringType()).alias('cust_phone_number')\
                                                       ,F.when((F.trim(df_lfkpa_lfppml_source['insumtel']) != '') &\
                                                               (F.trim(df_lfkpa_lfppml_source['insumtel']).isNotNull()) &\
                                                               (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insumtel']), '-', '') != '') ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insumtel'])\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR))\
                                                       .cast(T.StringType()).alias('cust_mobile_number')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_email')\
                                                       ,F.trim(df_lfkpa_lfppml_source['insuage']).cast(T.IntegerType()).cast(T.StringType()).alias('cust_issued_age')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_race')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_nationality')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_religion')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_source['cls']) == '01' ,\
                                                                F.trim(df_lfkpa_lfppml_source['cls'])\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '02' ,\
                                                                F.trim(df_lfkpa_lfppml_source['cls'])\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '03' ,\
                                                                F.trim(df_lfkpa_lfppml_source['cls'])\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '04' ,\
                                                                F.trim(df_lfkpa_lfppml_source['cls'])\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '1' ,\
                                                                lit('01')\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '2' ,\
                                                                lit('02')\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '3' ,\
                                                                lit('03')\
                                                              )\
                                                         .when(F.trim(df_lfkpa_lfppml_source['cls']) == '4' ,\
                                                                lit('04')\
                                                              )\
                                                         .otherwise(lit('99'))\
                                                       .cast(T.StringType()).alias('cust_occupation_level')\
                                                       ,lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_occupation_level_other_detail')\
                                                       ,lit(REQ_NULL_STR).cast(T.StringType()).alias('cust_occupation')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_job')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_position')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_business_type')\
                                                       ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cust_income_per_year')\
                                                       ,lit('N').cast(T.StringType()).alias('cust_us_person')\
                                                       ,lit('N').cast(T.StringType()).alias('cust_us_person_born')\
                                                       ,lit('N').cast(T.StringType()).alias('cust_us_person_green_card')\
                                                       ,lit('N').cast(T.StringType()).alias('cust_us_person_vat')\
                                                       ,lit('N').cast(T.StringType()).alias('cust_us_person_183')\
                                                       ,F.trim(df_lfkpa_lfppml_source['insuadd']).cast(T.StringType()).alias('cust_address_permanent')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code'])\
                                                              )\
                                                         .otherwise(lit('00')).alias('cust_sub_district_permanent')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_district_code'])\
                                                              )\
                                                         .otherwise(lit('00')).alias('cust_district_permanent')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_province_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_province_code'])\
                                                              )\
                                                         .otherwise(lit('00')).alias('cust_province_permanent')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_address_work')\
                                                       ,lit('00').cast(T.StringType()).alias('cust_sub_district_work')\
                                                       ,lit('00').cast(T.StringType()).alias('cust_district_work')\
                                                       ,lit('00').cast(T.StringType()).alias('cust_province_work')\
                                                       ,F.trim(df_lfkpa_lfppml_source['insuadd']).cast(T.StringType()).alias('cust_address_current')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code'])\
                                                              )\
                                                         .otherwise(lit('00')).alias('cust_sub_district_current')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_district_code'])\
                                                              )\
                                                         .otherwise(lit('00')).alias('cust_district_current')\
                                                       ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_province_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_province_code'])\
                                                              )\
                                                         .otherwise(lit('00')).alias('cust_province_current')\
                                                    )\
                              )\
                     .withColumn('insureds', F.array(\
                                                  F.struct( lit('1').cast(T.IntegerType()).cast(T.StringType()).alias('insrd_cov_plan_seq')\
                                                           ,lit('1').cast(T.IntegerType()).cast(T.StringType()).alias('insrd_seq')\
                                                           ,F.when(\
                                                                   (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['orthidno']), '-', '') != '')\
                                                                 & (~F.trim(df_lfkpa_lfppml_source['orthidno']).like('%-%'))\
                                                                 & (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') == '') ,\
                                                                   'F')\
                                                              .otherwise('P')\
                                                              .cast(T.StringType()).alias("insrd_type")\
                                                           ,F.when(\
                                                                   (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['orthidno']), '-', '') != '')\
                                                                 & (~F.trim(df_lfkpa_lfppml_source['orthidno']).like('%-%')) ,\
                                                                   '02')\
                                                              .when(\
                                                                   (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') != '')\
                                                                 & (~F.trim(df_lfkpa_lfppml_source['insuidno']).like('%-%'))\
                                                                 & (F.trim(df_lfkpa_lfppml_source['insuidno']) > 14),\
                                                                   '01')\
                                                              .when(\
                                                                   (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') != '')\
                                                                 & (~F.trim(df_lfkpa_lfppml_source['insuidno']).like('%-%'))\
                                                                 & (F.trim(df_lfkpa_lfppml_source['insuidno']) <= 14),\
                                                                   '03')\
                                                              .otherwise('99')\
                                                               .cast(T.StringType()).alias("insrd_id_type")\
                                                           ,F.when(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuidno']), '-', '') != '' ,\
                                                                      F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insuidno'],1,100)), "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""),"\\\\", "")\
                                                                   )\
                                                              .otherwise(F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['orthidno'],1,100)), "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""),"\\\\", ""))\
                                                               .cast(T.StringType()).alias('insrd_id')\
                                                           , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insuname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                       ),"\\\\", "")\
                                                              .cast(T.StringType()).alias('insrd_first_name')\
                                                           , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insulname'],1,100)),\
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                       ),"\\\\", "")\
                                                              .cast(T.StringType()).alias('insrd_last_name')\
                                                           ,F.trim(df_lfkpa_lfppml_source['insuage']).cast(T.IntegerType()).cast(T.StringType()).alias('insrd_age')\
                                                           ,F.concat(F.from_unixtime(df_lfkpa_lfppml_source['insudob']/1000,'yyyy-MM-dd')\
                                                                 ,lit('T00:00:00+07:00'))\
                                                                   .cast(T.StringType()).alias('insrd_birthday')\
                                                           ,F.when(F.trim(df_lfkpa_lfppml_source['insusex']) == 'M','01')\
                                                             .when(F.trim(df_lfkpa_lfppml_source['insusex']) == 'F','02')\
                                                             .otherwise(lit('99')).cast(T.StringType()).alias('insrd_gender')\
                                                           , F.regexp_replace(F.regexp_replace(F.trim(substring(df_lfkpa_lfppml_source['insuadd'],1,100)),\
                                                                               "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                          ),"\\\\", "")\
                                                              .alias('insrd_address')\
                                                           ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_sub_district_code'])\
                                                              )\
                                                             .otherwise(lit('00')).alias('insrd_sub_district')\
                                                           ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_district_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_district_code'])\
                                                              )\
                                                             .otherwise(lit('00')).alias('insrd_district')\
                                                           ,F.when(F.trim(df_lfkpa_lfppml_loc_prep['output_province_code']).isNotNull() ,\
                                                                 F.trim(df_lfkpa_lfppml_loc_prep['output_province_code'])\
                                                              )\
                                                             .otherwise(lit('00')).alias('insrd_province')\
                                                           ,F.when(F.trim(df_lfkpa_lfppml_source['insupost']) != '' ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insupost'])\
                                                              )\
                                                            .otherwise(lit('00')).cast(T.StringType()).alias('insrd_zip_code')\
                                                           ,lit('THA').cast(T.StringType()).alias('insrd_country_code')\
                                                           ,F.when((F.trim(df_lfkpa_lfppml_source['insuhtel']) != '') &\
                                                               (F.trim(df_lfkpa_lfppml_source['insuhtel']).isNotNull()) &\
                                                               (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insuhtel']), '-', '') != '') ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insuhtel'])\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR))
                                                            .cast(T.StringType()).alias('insrd_phone_number')\
                                                           ,F.when((F.trim(df_lfkpa_lfppml_source['insumtel']) != '') &\
                                                               (F.trim(df_lfkpa_lfppml_source['insumtel']).isNotNull()) &\
                                                               (F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insumtel']), '-', '') != '') ,\
                                                                 F.trim(df_lfkpa_lfppml_source['insumtel'])\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR))\
                                                           .cast(T.StringType()).alias('insrd_mobile_number')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_email')\
                                                           ,F.trim(df_lfkpa_lfppml_source['insuage']).cast(T.IntegerType()).cast(T.StringType()).alias('insrd_issued_age')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_race')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_nationality')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_religion')\
                                                            ,F.when(F.trim(df_lfkpa_lfppml_source['cls']) == '01' ,\
                                                                F.trim(df_lfkpa_lfppml_source['cls'])\
                                                              )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '02' ,\
                                                                     F.trim(df_lfkpa_lfppml_source['cls'])\
                                                                   )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '03' ,\
                                                                     F.trim(df_lfkpa_lfppml_source['cls'])\
                                                                   )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '04' ,\
                                                                     F.trim(df_lfkpa_lfppml_source['cls'])\
                                                                   )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '1' ,\
                                                                     lit('01')\
                                                                   )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '2' ,\
                                                                     lit('02')\
                                                                   )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '3' ,\
                                                                     lit('03')\
                                                                   )\
                                                              .when(F.trim(df_lfkpa_lfppml_source['cls']) == '4' ,\
                                                                     lit('04')\
                                                                   )\
                                                              .otherwise(lit('99'))\
                                                              .cast(T.StringType()).alias('insrd_occupation_level')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation_level_other_detail')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_job')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_position')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_business_type')\
                                                           ,F.when((F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insurin']), '\/|\,|\ |\>', '').cast("int").isNotNull()) &\
                                                                   (F.trim(df_lfkpa_lfppml_source['insurin']) != ''),\
                                                                   F.when(F.split(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insurin']), '\,|\ |\>', ''), "/").getItem(1).cast("int").isNotNull()\
                                                                          ,(F.split(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insurin']), '\,|\ |\>', ''), "/").getItem(0)/\
                                                                           F.split(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insurin']), '\,|\ |\>', ''), "/").getItem(1)).cast(T.DecimalType(18, 2))
                                                                         )\
                                                                    .otherwise(F.split(F.regexp_replace(F.trim(df_lfkpa_lfppml_source['insurin']), '\,|\ |\>', ''), "/").getItem(0)).cast(T.DecimalType(18, 2))\
                                                                  )\
                                                             .otherwise(lit(NON_REQ_NULL_NUM))\
                                                           .cast(T.StringType()).alias('insrd_income_per_year')\
                                                           ,F.array(lit('{}').cast(T.StringType())).alias('insrd_pol_sub_ids')\
                                                           ,F.array().alias('insrd_beneficiaries')\
                                                          )\
                                                      )\
                               )\
                   .withColumn('sub_policies', \
                           F.array(\
                             F.struct(\
                                      F.concat(\
                                               F.trim(df_lfkpa_lfppml_source['pno']),lit('_1')\
                                              ).cast(T.StringType()).alias('sub_pol_id')\
                                      ,lit('01').cast(T.StringType()).alias('sub_pol_type')\
                                      ,F.when(F.trim(df_ref_pa_pol_sub_type_lserff['plan_code']).isNotNull()\
                                              , F.trim(df_ref_pa_pol_sub_type_lserff['lserff_code']))\
                                              .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                              .alias('sub_pol_ppt')\
                                      ,concat(F.from_unixtime(df_lfkpa_lfppml_source['peff']/1000,'yyyy-MM-dd'),\
                                                   lit('T00:00:00+07:00')).cast(T.StringType()).alias('sub_pol_start_date')\
                                      ,concat(F.from_unixtime(df_lfkpa_lfppml_source['ppytd']/1000,'yyyy-MM-dd'),\
                                                   lit('T00:00:00+07:00')).cast(T.StringType()).alias('sub_pol_end_date')\
                                      ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('sub_pol_sum_insured_per_year_exclude')\
                                      ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('sub_pol_sum_insured_per_year_include')\
                                      ,F.array(\
                                                  F.struct(\
                                                             lit('1').cast(T.IntegerType()).cast(T.StringType()).alias('cov_plan_seq')\
                                                             ,F.trim(df_lfkpa_lfppml_source['plancode']).cast(T.StringType()).alias('cov_plan_name')\
                                                             ,F.trim(df_lfkpa_lfppml_source['paadb']).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_sum_insured')\
                                                             ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_maturity_benefit')
                                                             ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_survival_benefit_min')\
                                                             ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_survival_benefit_max')\
                                                             ,df_coverage_output['coverages_under_plans'].alias('coverages_under_plans')\
                                                          )\
                                              ).alias('policy_cov_plan_seqs')\
                                      ,df_coverage_output['coverages'].alias('coverages')\
                                     )\
                                   )\
                               )\
                   .withColumn('beneficiaries', \
                                   df_beneficiaries['beneficiaries']
                              )\
                   .withColumn('endorsements', \
                           F.array(\
                                  F.struct(\
                                             lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_id')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_type_1')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_type_2')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_sub_pol_id')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_detail')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('endorse_status')\
                                          )\
                                  )\
                              )\
                   .withColumn('loans', \
                           F.array(\
                                  F.struct(\
                                             lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('loan_id')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('loan_type')\
                                            ,lit(NON_REQ_NULL_DATE).cast(T.StringType()).alias('loan_date')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('loan_interest_rate')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('loan_amount')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('loan_interest_amount')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('loan_status')\
                                          )\
                                  )\
                              )\
                   .withColumn('loan_payments', \
                           F.array(\
                                  F.struct(\
                                             lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('loan_payment_id')\
                                            ,lit(NON_REQ_NULL_DATE).cast(T.StringType()).alias('loan_payment_date')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('loan_payment_amount')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('loan_payment_interest')\
                                            ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('loan_payment_principal')\
                                          )\
                                  )\
                              )\
                   .withColumn('investments', \
                           F.array(\
                                  F.struct(\
                                             lit(NON_REQ_NULL_DATE).cast(T.StringType()).alias('inv_transaction_date')\
                                            ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_transaction_type')\
                                            ,F.array(\
                                                    F.struct(\
                                                              lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_code')\
                                                             ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('inv_fund_unit')\
                                                             ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('inv_fund_nav')\
                                                             ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_thai_name')\
                                                             ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_eng_name')\
                                                             ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_type')\
                                                             ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_amc')\
                                                             ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('inv_fund_data_status')\
                                                            )\
                                                    ).alias('policy_fund_codes')\
                                          )\
                                  )\
                              )\

    print('--Consolidating every elements to final PA Policy Dataframe...')
    output_policy_df_final = output_policy_df.select(\
    'pol_refer_code_of_company'
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
    ,'customer'
    ,'insureds'
    ,'sub_policies'
    ,'beneficiaries'
    ,'endorsements'
    ,'loans'
    ,'loan_payments'
    ,'investments')
    return output_policy_df_final
