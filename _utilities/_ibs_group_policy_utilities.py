'''
job name     : IBS Group Policy Utilities module.
Created by   : Kiatipoom Poopanitpun
Created date : 6 Dec 2022
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

import ibs_utilities as ibs_utils


def group_policy_aggregation(params, spark_session, start_date, end_date):
    print('Reading data source transactional tables...')
    df_policy_grp = spark_session.table(
        'processing_ibs.ibs_atlldta_chdrpf')

    df_policy_ilcl = spark_session.table(
        'processing_ibs.ibs_atlldta_chdrpf_ilcl')

    df_member_grp = spark_session.table(
        'processing_ibs.ibs_atlldta_gmhdpf')

    df_member_ilcl = spark_session.table(
        'processing_ibs.ibs_atlldta_gmhdpf_ilcl')

    df_policy_hist = spark_session.table(
        'processing_ibs.ibs_atlldta_gchipf')

    df_policy_hist_ilcl = spark_session.table(
        'processing_ibs.ibs_atlldta_gchipf_ilcl')

    df_plan_benefit_all = spark_session.table(
        'processing_ibs.ibs_atlldta_bnfhpf')

    df_clnt_all = spark_session.table(
        'processing_ibs.ibs_atlldta_clntpf')

    df_policy_owner_info_all = spark_session.table(
        'processing_ibs.ibs_atlldta_clexpf')

    df_plan_coverage = spark_session.table(
        'processing_ibs.ibs_atlldta_gxhipf')

    df_plan_coverage_ilcl = spark_session.table(
        'processing_ibs.ibs_atlldta_gxhipf_ilcl')

    df_benefit_all = spark_session.table(
        'processing_ibs.ibs_atlldta_bnflpf')

    df_policy_grp_all = df_policy_grp.unionAll(df_policy_ilcl.drop('yyyy', 'mm'))

    df_member_grp_all = df_member_grp.unionAll(df_member_ilcl.drop('yyyy', 'mm')).alias('member')\
                                               .join(df_policy_grp_all.alias('policy')\
                                                     ,F.trim(col('member.chdrnum')) ==  F.trim(col('policy.chdrnum'))\
                                                     ,'leftsemi')

    df_policy_hist_all = df_policy_hist.unionAll(df_policy_hist_ilcl.drop('zsurchrg', 'zrbtcost', 'zadmchg', 'zmodfvar', 'yyyy', 'mm'))

    df_plan_coverage_all = df_plan_coverage.unionAll(df_plan_coverage_ilcl)



    df_policy_owner_all = df_policy_grp_all.alias('pol')\
                      .join(df_clnt_all.alias('clnt'),\
                           col('pol.cownnum') == col('clnt.clntnum'), 'inner')\
                      .select(col('pol.chdrnum'), col('clnt.*'))

    df_policy_owner_info_all = df_policy_grp_all.alias('pol')\
                              .join(df_policy_owner_info_all.alias('clnt'),\
                               col('pol.cownnum') == col('clnt.clntnum'), 'inner')\
                              .select(col('pol.chdrnum'), col('clnt.*'))\
                              
    df_plan_coverage_all = df_plan_coverage_all.alias('cov')\
                              .join(df_member_grp_all.alias('mem'),\
                               (F.trim(col('cov.chdrnum')) == F.trim(col('mem.chdrnum'))) &\
                               (F.trim(col('cov.mbrno')) == F.trim(col('mem.mbrno'))) &\
                               (F.trim(col('cov.dpntno')) == F.trim(col('mem.dpntno')))
                                    , 'inner')\
                              .select(col('mem.clntnum'), col('cov.*'))\
                              
                              
    df_clnt_all = df_clnt_all.alias('clnt')\
                      .join(df_member_grp_all.alias('mem'),\
                           col('clnt.clntnum') == col('mem.clntnum'), 'inner')\
                      .select(col('mem.chdrnum'), col('mem.mbrno'), col('mem.dpntno'), col('clnt.*'))\
                      .filter(col('clnt.validflag') == '1')

    print('Policy delta new and change...')
    key_cols = ['chdrnum']
    schema = T.StructType([T.StructField(f'{key_cols[0]}', T.StringType(),True)
                            ])
    df_policy_delta_key = spark_session.createDataFrame("", schema)

    ##df_policy_grp_all
    df_policy_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_policy_delta_key, df_policy_grp_all, key_cols, start_date, end_date)

    ##df_policy_hist_all
    df_policy_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_policy_delta_key, df_policy_hist_all, key_cols, start_date, end_date)

    ##df_plan_benefit_all
    df_policy_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_policy_delta_key, df_plan_benefit_all, key_cols, start_date, end_date)

    ##df_policy_owner
    df_policy_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_policy_delta_key, df_policy_owner_all, key_cols, start_date, end_date)

    ##df_policy_owner_info_all
    df_policy_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_policy_delta_key, df_policy_owner_info_all, key_cols, start_date, end_date)


    df_policy_delta_key = df_policy_delta_key.alias('key').groupBy(key_cols).count()\
                               .filter(F.substring(col('key.chdrnum'),1 ,1) != '9')\
                               .select(key_cols)

    print('Member delta new and change...')
    key_cols = ['chdrnum', 'mbrno', 'dpntno', 'clntnum']
    schema = T.StructType([T.StructField(f'{key_cols[0]}', T.StringType(),True),
                           T.StructField(f'{key_cols[1]}', T.StringType(),True),
                           T.StructField(f'{key_cols[2]}', T.StringType(),True),
                           T.StructField(f'{key_cols[3]}', T.StringType(),True)
                            ])
    df_member_delta_key = spark_session.createDataFrame("", schema)

    ##df_member_grp_all
    df_member_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_member_delta_key, df_member_grp_all, key_cols, start_date, end_date)

    ##df_plan_coverage_all
    df_member_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_member_delta_key, df_plan_coverage_all, key_cols, start_date, end_date)

    ##df_clnt_all
    df_member_delta_key = ibs_utils.build_policy_delta_key(spark_session, df_member_delta_key, df_clnt_all, key_cols, start_date, end_date)


    df_member_delta_key = df_member_delta_key.alias('key').groupBy(key_cols).count()\
                               .filter(F.substring(col('key.chdrnum'),1 ,1) != '9')\
                               .select(key_cols)

    print('Exclude policy key and keep only member key to df_member_delta_key...')
    df_member_delta_key = df_member_delta_key.alias('mem')\
                          .join(df_policy_delta_key.alias('pol'),\
                                col('mem.chdrnum') == col('pol.chdrnum')\
                                ,'left_anti'
                               )

    print('Passing policy key to each relation table of policy...')
    ##df_policy_grp_all
    df_policy_pol_delta_prep = df_policy_grp_all.alias('pol')\
                                 .join(df_policy_delta_key.alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_mem_delta_prep = df_policy_grp_all.alias('pol')\
                                 .join(df_member_delta_key.groupBy('chdrnum').count().alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_grp_all = df_policy_pol_delta_prep.unionAll(df_policy_mem_delta_prep)

    ##df_policy_hist_all
    df_policy_hist_pol_delta_prep = df_policy_hist_all.alias('pol')\
                                 .join(df_policy_delta_key.alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_hist_mem_delta_prep = df_policy_hist_all.alias('pol')\
                                 .join(df_member_delta_key.groupBy('chdrnum').count().alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_hist_all = df_policy_hist_pol_delta_prep.unionAll(df_policy_hist_mem_delta_prep)


    ##df_plan_benefit_all
    df_plan_benefit_pol_delta_prep = df_plan_benefit_all.alias('pol')\
                                 .join(df_policy_delta_key.alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_plan_benefit_mem_delta_prep = df_plan_benefit_all.alias('pol')\
                                 .join(df_member_delta_key.groupBy('chdrnum').count().alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_plan_benefit_all = df_plan_benefit_pol_delta_prep.unionAll(df_plan_benefit_mem_delta_prep)


    ##df_policy_owner_all
    df_policy_owner_pol_delta_prep = df_policy_owner_all.alias('pol')\
                                 .join(df_policy_delta_key.alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_owner_mem_delta_prep = df_policy_owner_all.alias('pol')\
                                 .join(df_member_delta_key.groupBy('chdrnum').count().alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_owner_all = df_policy_owner_pol_delta_prep.unionAll(df_policy_owner_mem_delta_prep)


    ##df_policy_owner_info_all
    df_policy_owner_info_pol_delta_prep = df_policy_owner_info_all.alias('pol')\
                                 .join(df_policy_delta_key.alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_owner_info_mem_delta_prep = df_policy_owner_info_all.alias('pol')\
                                 .join(df_member_delta_key.groupBy('chdrnum').count().alias('key'), col('pol.chdrnum') == col('key.chdrnum'), "leftsemi")
    df_policy_owner_info_all = df_policy_owner_info_pol_delta_prep.unionAll(df_policy_owner_info_mem_delta_prep)

    print("-- df_policy_grp_all : ", df_policy_grp_all.count())
    print("-- df_policy_hist_all : ", df_policy_hist_all.count())
    print("-- df_benefit_all : ", df_benefit_all.count())
    print("-- df_plan_benefit_all : ", df_plan_benefit_all.count())
    print("-- df_policy_owner_all : ", df_policy_owner_all.count())
    print("-- df_policy_owner_info_all : ", df_policy_owner_info_all.count())

    print('Passing member key to each relation table of member...')   
    ##df_member_grp_all
    df_member_pol_delta_prep = df_member_grp_all.alias('mem')\
                                 .join(df_policy_delta_key.alias('key'), \
                                      (col('mem.chdrnum') == col('key.chdrnum'))
                                       , "leftsemi")
    df_member_mem_delta_prep = df_member_grp_all.alias('mem')\
                                 .join(df_member_delta_key.alias('key'), \
                                      (col('mem.chdrnum') == col('key.chdrnum')) &\
                                      (col('mem.mbrno') == col('key.mbrno')) &\
                                      (col('mem.dpntno') == col('key.dpntno')) &\
                                      (col('mem.clntnum') == col('key.clntnum')) \
                                       , "leftsemi")
    df_member_grp_all = df_member_pol_delta_prep.unionAll(df_member_mem_delta_prep)


    ##df_plan_coverage_all
    df_coverage_pol_delta_prep = df_plan_coverage_all.alias('mem')\
                                 .join(df_policy_delta_key.alias('key'), \
                                      (col('mem.chdrnum') == col('key.chdrnum'))
                                       , "leftsemi")
    df_coverage_mem_delta_prep = df_plan_coverage_all.alias('mem')\
                                 .join(df_member_delta_key.alias('key'), \
                                      (col('mem.chdrnum') == col('key.chdrnum')) &\
                                      (col('mem.mbrno') == col('key.mbrno')) &\
                                      (col('mem.dpntno') == col('key.dpntno')) &\
                                      (col('mem.clntnum') == col('key.clntnum')) \
                                       , "leftsemi")
    df_plan_coverage_all = df_coverage_pol_delta_prep.unionAll(df_coverage_mem_delta_prep)


    ##df_clnt_all
    df_client_pol_delta_prep = df_clnt_all.alias('mem')\
                                 .join(df_policy_delta_key.alias('key'), \
                                      (col('mem.chdrnum') == col('key.chdrnum'))
                                       , "leftsemi")
    df_client_mem_delta_prep = df_clnt_all.alias('mem')\
                                 .join(df_member_delta_key.alias('key'), \
                                      (col('mem.chdrnum') == col('key.chdrnum')) &\
                                      (col('mem.mbrno') == col('key.mbrno')) &\
                                      (col('mem.dpntno') == col('key.dpntno')) &\
                                      (col('mem.clntnum') == col('key.clntnum')) \
                                       , "leftsemi")
    df_clnt_all = df_client_pol_delta_prep.unionAll(df_client_mem_delta_prep)

    print("-- df_member_grp_all : ", df_member_grp_all.count())
    print("-- df_plan_coverage_all : ", df_plan_coverage_all.count())
    print("-- df_clnt_all : ", df_clnt_all.count())

    print('Preparation latest effective of each policy, member, benefit plan and owner...')   
    df_member_grp_all_max_dtetrm = df_member_grp_all.alias('mem').groupBy('chdrnum', 'clntnum')\
                                     .agg(F.max(col('mem.dteatt').cast(T.IntegerType())).alias('max_dteatt'),\
                                          F.max(col('mem.tranno').cast(T.IntegerType())).alias('max_tranno'),\
                                          F.max(col('mem.mbrno').cast(T.IntegerType())).alias('max_mbrno'),
                                         )\
                                     .withColumnRenamed('chdrnum', 'max_chdrnum')\
                                     .withColumnRenamed('clntnum', 'max_clntnum')\
                                    
                                    
    df_member_grp_all = df_member_grp_all.alias('member').join(df_member_grp_all_max_dtetrm.alias('max_member')\
                                            ,(F.trim(col('member.chdrnum')) == F.trim(col('max_member.max_chdrnum')))\
                                            &(F.trim(col('member.clntnum')) == F.trim(col('max_member.max_clntnum')))\
                                            &(col('member.dteatt').cast(T.IntegerType()) ==  (col('max_member.max_dteatt')))\
                                            &(col('member.tranno').cast(T.IntegerType()) == (col('max_member.max_tranno')))\
                                            &(col('member.mbrno').cast(T.IntegerType()) == (col('max_member.max_mbrno')))\
                                            , 'inner'
                                           )\
                                          
                                          
                                          
    df_policy_hist_max_effdate = df_policy_hist_all.groupBy('chdrnum')\
                                     .agg(F.max(df_policy_hist_all['effdate']).alias('max_eff'),F.count('chdrnum').alias('pol_year'))\
                                     .withColumnRenamed('chdrnum', 'max_chdrnum')


    df_policy_hist_all = df_policy_hist_all.join(df_policy_hist_max_effdate\
                                            ,(df_policy_hist_all['chdrnum'] == df_policy_hist_max_effdate['max_chdrnum'])\
                                            &(df_policy_hist_all['effdate'] == df_policy_hist_max_effdate['max_eff'])\
                                            , 'inner'
                                           )

    df_plan_max_planno_benefit = df_plan_benefit_all.groupBy('chdrnum')\
                                     .agg(F.max(df_plan_benefit_all['planno']).alias('max_planno'))\
                                    
                                    
    df_policy_owner_all = df_policy_owner_all.filter(df_policy_owner_all['validflag'] == '1')

    df_policy_owner_info_all = df_policy_owner_info_all.filter(df_policy_owner_info_all['validflag'] == '1')

    df_clnt_all = df_clnt_all.alias('clnt')\
                  .join(df_member_grp_all.alias('mem'), \
                                      (col('clnt.chdrnum') == col('mem.chdrnum')) &\
                                      (col('clnt.mbrno') == col('mem.mbrno')) &\
                                      (col('clnt.dpntno') == col('mem.dpntno')) &\
                                      (col('clnt.clntnum') == col('mem.clntnum')) \
                                       , "leftsemi")

    print(f'-- policy count : {df_policy_grp_all.count()}')

    print(f'-- member max count : {df_member_grp_all_max_dtetrm.count()}')
    print(f'-- member count : {df_member_grp_all.count()}')

    print(f'-- policy history max count : {df_policy_hist_max_effdate.count()}')
    print(f'-- policy history count : {df_policy_hist_all.count()}')

    print(f'-- owner count : {df_policy_owner_all.count()}')
    print(f'-- owner info count : {df_policy_owner_info_all.count()}')
    print(f'-- client count : {df_clnt_all.count()}')

    print(f'-- plan benefit ref count : {df_plan_max_planno_benefit.count()}')




    print('Reading ref tables...')
    df_ref_policy_benefit_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_benefit')
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
    df_ref_group_pol_sub_type_lserff = spark_session.table(
        'processing_ibs.ibs_ref_group_pol_sub_type_lserff')
    df_ref_policy_group_pol_name = spark_session.table(
        'processing_ibs.ibs_ref_policy_group_pol_name')
    df_ref_policy_group_product_definition = spark_session.table(
        'processing_ibs.ibs_ref_policy_group_product_definition')
    df_ref_policy_group_product_definition_description = spark_session.table(
        'processing_ibs.ibs_ref_policy_group_product_definition_description')
    df_ref_policy_group_benefit_definition = spark_session.table(
        'processing_ibs.ibs_ref_policy_group_benefit_definition')
    df_ref_policy_group_benefit_definition_description = spark_session.table(
        'processing_ibs.ibs_ref_policy_group_benefit_definition_description')


    ibs_ref_location_code = spark_session.table(
        'processing_ibs.ibs_ref_location_code')
    ibs_ref_province_location_code = spark_session.table(
        'processing_ibs.ibs_ref_province_location_code')
    ibs_ref_district_location_code = spark_session.table(
        'processing_ibs.ibs_ref_district_location_code')
    ibs_ref_sub_district_location_code = spark_session.table(
        'processing_ibs.ibs_ref_sub_district_location_code')
    ibs_ref_pa_policy_location_map = spark_session.table(
        'processing_ibs.ibs_ref_pa_policy_location_map')

    print('Declaring Default Params Value...')
    pol_refer_code_of_company_prefix = "POL_"
    pol_transaction_status = "N"
    pol_company_id = "1022"
    pol_type = "PT-01"
    cust_country_code = "THA"
    insrd_pol_sub_id = 'A'
    insrd_seq = '1'
    cov_plan_seq = '1'

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = '0000-00-00T00:00:00+07:00'

    NON_REQ_NULL_STR = 'null'
    NON_REQ_NULL_NUM = 'null'
    NON_REQ_NULL_DATE = 'null'



    print('Declaring Dimproduct Basic by map with ref table...')
    df_ref_policy_service_md_ref_policy_type = df_ref_policy_service_md_ref_source\
                                                    .filter(F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_type')\
                                                    .withColumnRenamed('ktaxa_code', 'policy_type_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'policy_type_oic_code')

    print('### Declaring Product Type by map with ref table...')
    df_ref_policy_service_md_ref_policy_product_type = df_ref_policy_service_md_ref_source\
                                                    .filter(F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_product_type')\
                                                    .withColumnRenamed('ktaxa_code', 'policy_product_type_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'policy_product_type_oic_code')



    print('Declaring Policy Status by map with ref table...')
    df_ref_policy_service_md_ref_policy_status = df_ref_policy_service_md_ref_source\
                                                    .filter(F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_status')\
                                                    .withColumnRenamed('ktaxa_code', 'policy_status_ktaxa_code')\
                                                    .withColumnRenamed('id_code', 'policy_status_oic_code')


    print('### Declaring Product Definination by map with ref table...')
    df_ref_policy_group_product_definition = df_ref_policy_group_product_definition.alias('product_def')\
                                             .join(df_ref_policy_group_product_definition_description.alias('product_def_desc')\
                                                   ,(F.trim(col('product_def.oic_code')) ==  F.trim(col('product_def_desc.oic_desc_code'))) \
                                                   , 'inner')


    print('### Declaring Benefit Defination by map with ref table...')
    df_ref_policy_group_benefit_definition = df_ref_policy_group_benefit_definition.alias('benefit_def')\
                                             .join(df_ref_policy_group_benefit_definition_description.alias('benefit_def_desc')\
                                                   ,(F.trim(col('benefit_def.oic_code')) ==  F.trim(col('benefit_def_desc.oic_desc_code'))) \
                                                   , 'inner')


    print('### Declaring Benefit Defination by map with ref table...')

    df_policy_grp_all = df_policy_grp.unionAll(df_policy_ilcl.drop('yyyy', 'mm'))



    print('### Preparing Location code dataframe for address columns...')

    print('##### build client location...')
    ibs_ref_district_location_code = ibs_ref_district_location_code\
                                .filter(F.trim(ibs_ref_district_location_code['district_th']) != '')

    ibs_ref_sub_district_location_code = ibs_ref_sub_district_location_code\
                                 .filter(F.trim(ibs_ref_sub_district_location_code['sub_district_th']) != '')

    df_clnt_all_loc = df_clnt_all.alias('a').select('clntnum', 'cltaddr04', 'cltaddr03')\
                                                 .join(ibs_ref_province_location_code.alias('b'),\
                                                      F.trim(col('a.cltaddr04'))\
                                                       .contains(F.trim(col('b.province_th')))\
                                                      ,'left')\
                                                 .join(ibs_ref_pa_policy_location_map.alias('c'),\
                                                      F.trim(col('a.cltaddr04')) == F.trim(col('c.pa_location_incorrect_th'))\
                                                      ,'left')\
                                                 .join(ibs_ref_district_location_code.alias('c'),\
                                                      F.trim(col('a.cltaddr03'))\
                                                       .contains(F.concat(lit('อำเภอ'),F.trim(col('c.district_th'))))\
                                                      ,'left')\
                                                 .join(ibs_ref_sub_district_location_code.alias('d'),\
                                                      F.trim(col('a.cltaddr03'))\
                                                       .contains(F.concat(lit('ตำบล'),F.trim(col('d.sub_district_th'))))\
                                                      ,'left')\
                                                      
    df_clnt_all_loc                                 = df_clnt_all_loc.withColumn('output_province',\
                                                                   F.when(df_clnt_all_loc['oic_province_code'].isNotNull()\
                                                                            ,df_clnt_all_loc['province_th'])\
                                                                    .when(df_clnt_all_loc['pa_oic_province_code'].isNotNull()\
                                                                            ,df_clnt_all_loc['pa_location_correct_th']\
                                                                         )\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                                  )\
                                                        .withColumn('output_province_code',\
                                                                   F.when(df_clnt_all_loc['oic_province_code'].isNotNull()\
                                                                            ,df_clnt_all_loc['oic_province_code'])\
                                                                    .when(df_clnt_all_loc['pa_oic_province_code'].isNotNull()\
                                                                            ,df_clnt_all_loc['pa_oic_province_code']\
                                                                         )\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                                  )


    df_clnt_all_loc                                  = df_clnt_all_loc.filter(\
                                                               ((df_clnt_all_loc['output_province_code'] == substring(df_clnt_all_loc['oic_district_code'],1,2)) &\
                                                               (df_clnt_all_loc['output_province_code'] == substring(df_clnt_all_loc['oic_sub_district_code'],1,2)) &\
                                                               (df_clnt_all_loc['oic_district_code'] == substring(df_clnt_all_loc['oic_sub_district_code'],1,4))
                                                               )
                                                              )

    df_clnt_all_loc                                  =     df_clnt_all_loc.withColumn('output_district_code', substring(df_clnt_all_loc['oic_district_code'],3,2))\
                                                       .withColumn('output_sub_district_code', substring(df_clnt_all_loc['oic_sub_district_code'],5,2))\
                                                       .withColumn("leven", F.levenshtein(col("cltaddr03"), col("sub_district_th")) )\
                                                      
                                                      
    df_min_levenshtein                           =  df_clnt_all_loc.groupBy('clntnum')\
                                                 .agg(F.min("leven").alias("min_leven"))\
                                                 .withColumnRenamed('clntnum', 'leven_clntnum')


    df_clnt_all_loc = df_clnt_all_loc.join(df_min_levenshtein, \
                                                         (df_clnt_all_loc['clntnum'] == df_min_levenshtein['leven_clntnum']) & \
                                                         (df_clnt_all_loc['leven'] == df_min_levenshtein['min_leven']), 'inner')\
                                                         .withColumnRenamed('clntnum', 'loc_clntnum')\
                                                         .withColumnRenamed('chdrnum', 'loc_chdrnum')\
                                                         .select(\
                                                                 'loc_clntnum',\
                                                                 df_clnt_all_loc['cltaddr04'],\
                                                                 df_clnt_all_loc['cltaddr03'],\
                                                                 'sub_district_th',\
                                                                 'output_province_code',\
                                                                 'output_district_code',\
                                                                 'output_sub_district_code')


    print('##### build Owner location...')
    ibs_ref_district_location_code = ibs_ref_district_location_code\
                                .filter(F.trim(ibs_ref_district_location_code['district_th']) != '')

    ibs_ref_sub_district_location_code = ibs_ref_sub_district_location_code\
                                 .filter(F.trim(ibs_ref_sub_district_location_code['sub_district_th']) != '')

    df_policy_owner_all_loc = df_policy_owner_all.alias('a').select('clntnum', 'cltaddr04', 'cltaddr03')\
                                                 .join(ibs_ref_province_location_code.alias('b'),\
                                                      F.trim(col('a.cltaddr04'))\
                                                       .contains(F.trim(col('b.province_th')))\
                                                      ,'left')\
                                                 .join(ibs_ref_pa_policy_location_map.alias('c'),\
                                                      F.trim(col('a.cltaddr04')) == F.trim(col('c.pa_location_incorrect_th'))\
                                                      ,'left')\
                                                 .join(ibs_ref_district_location_code.alias('c'),\
                                                      F.trim(col('a.cltaddr03'))\
                                                       .contains(F.concat(lit('อำเภอ'),F.trim(col('c.district_th'))))\
                                                      ,'left')\
                                                 .join(ibs_ref_sub_district_location_code.alias('d'),\
                                                      F.trim(col('a.cltaddr03'))\
                                                       .contains(F.concat(lit('ตำบล'),F.trim(col('d.sub_district_th'))))\
                                                      ,'left')\
                                                      
    df_policy_owner_all_loc                                 = df_policy_owner_all_loc.withColumn('output_province',\
                                                                   F.when(df_policy_owner_all_loc['oic_province_code'].isNotNull()\
                                                                            ,df_policy_owner_all_loc['province_th'])\
                                                                    .when(df_policy_owner_all_loc['pa_oic_province_code'].isNotNull()\
                                                                            ,df_policy_owner_all_loc['pa_location_correct_th']\
                                                                         )\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                                  )\
                                                        .withColumn('output_province_code',\
                                                                   F.when(df_policy_owner_all_loc['oic_province_code'].isNotNull()\
                                                                            ,df_policy_owner_all_loc['oic_province_code'])\
                                                                    .when(df_policy_owner_all_loc['pa_oic_province_code'].isNotNull()\
                                                                            ,df_policy_owner_all_loc['pa_oic_province_code']\
                                                                         )\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                                  )


    df_policy_owner_all_loc                                  = df_policy_owner_all_loc.filter(\
                                                               ((df_policy_owner_all_loc['output_province_code'] == substring(df_policy_owner_all_loc['oic_district_code'],1,2)) &\
                                                               (df_policy_owner_all_loc['output_province_code'] == substring(df_policy_owner_all_loc['oic_sub_district_code'],1,2)) &\
                                                               (df_policy_owner_all_loc['oic_district_code'] == substring(df_policy_owner_all_loc['oic_sub_district_code'],1,4))
                                                               )
                                                              )

    df_policy_owner_all_loc                                  =     df_policy_owner_all_loc.withColumn('output_district_code', substring(df_policy_owner_all_loc['oic_district_code'],3,2))\
                                                       .withColumn('output_sub_district_code', substring(df_policy_owner_all_loc['oic_sub_district_code'],5,2))\
                                                       .withColumn("leven", F.levenshtein(col("cltaddr03"), col("sub_district_th")) )\
                                                      
                                                      
    df_min_levenshtein                           =  df_policy_owner_all_loc.groupBy('clntnum')\
                                                 .agg(F.min("leven").alias("min_leven"))\
                                                 .withColumnRenamed('clntnum', 'leven_clntnum')


    df_policy_owner_all_loc = df_policy_owner_all_loc.join(df_min_levenshtein, \
                                                         (df_policy_owner_all_loc['clntnum'] == df_min_levenshtein['leven_clntnum']) & \
                                                         (df_policy_owner_all_loc['leven'] == df_min_levenshtein['min_leven']), 'inner')\
                                                         .withColumnRenamed('clntnum', 'loc_owner_clntnum')\
                                                         .withColumnRenamed('chdrnum', 'loc_owner_chdrnum')\
                                                         .select(\
                                                                 'loc_owner_clntnum',\
                                                                 df_policy_owner_all_loc['cltaddr04'],\
                                                                 df_policy_owner_all_loc['cltaddr03'],\
                                                                 'sub_district_th',\
                                                                 'output_province_code',\
                                                                 'output_district_code',\
                                                                 'output_sub_district_code')
    
    df_policy_owner_all_loc = df_policy_owner_all_loc.dropDuplicates()
    df_clnt_all_loc = df_clnt_all_loc.dropDuplicates()

    print('### Preparing Plans and Coverages...')

    df_plan_coverage_all = df_plan_coverage_all.alias('plan_cov')\
                                               .join(df_member_grp_all.alias('member')\
                                                     ,(F.trim(col('plan_cov.chdrnum')) ==  F.trim(col('member.chdrnum'))) &\
                                                      (F.trim(col('plan_cov.mbrno')) ==  F.trim(col('member.mbrno'))) &\
                                                      (F.trim(col('plan_cov.dpntno')) ==  F.trim(col('member.dpntno')))\
                                                     ,'leftsemi')

    df_benefit_all = df_benefit_all.alias('benefit')\
                                               .join(df_plan_coverage_all.alias('plan_cov')\
                                                     ,(F.trim(col('benefit.chdrnum')) ==  F.trim(col('plan_cov.chdrnum'))) &\
                                                      (F.trim(col('benefit.planno')) ==  F.trim(col('plan_cov.planno'))) &\
                                                      (F.trim(col('benefit.prodtyp')) ==  F.trim(col('plan_cov.prodtyp')))\
                                                     ,'leftsemi')


    df_plan_coverage_max_all = df_plan_coverage_all.alias('plan_cov')\
                                                    .groupBy(F.trim(col('plan_cov.chdrnum')).alias('max_chdrnum'),\
                                                             F.trim(col('plan_cov.mbrno')).alias('max_mbrno'),\
                                                             F.trim(col('plan_cov.dpntno')).alias('max_dpntno'),\
                                                             F.trim(col('plan_cov.planno')).alias('max_planno'),\
                                                             F.trim(col('plan_cov.prodtyp')).alias('max_prodtyp')\
                                                           )\
                                                    .agg(F.max(F.trim(col('plan_cov.effdate'))).alias('max_effdate'))

    df_benefit_max_all = df_benefit_all.alias('benefit')\
                                                    .filter(F.trim(col('benefit.bencde')) != '')\
                                                    .groupBy(F.trim(col('benefit.chdrnum')).alias('max_chdrnum'),\
                                                             F.trim(col('benefit.prodtyp')).alias('max_prodtyp'),\
                                                             F.trim(col('benefit.planno')).alias('max_planno'),\
                                                             F.trim(col('benefit.bencde')).alias('max_bencde'),\
                                                             F.trim(col('benefit.amtday')).alias('max_amtday'),\
                                                             F.trim(col('benefit.amtvis')).alias('max_amtvis'),\
                                                             F.trim(col('benefit.amtpropr')).alias('max_amtpropr'),\
                                                             F.trim(col('benefit.amtyear')).alias('max_amtyear'),\
                                                             F.trim(col('benefit.daysvis')).alias('max_daysvis')\
                                                           )\
                                                    .agg(F.max(F.trim(col('benefit.effdate'))).alias('max_eff'))



    df_plan_coverage_all = df_plan_coverage_all.alias('plan_cov')\
                                                   .join(df_plan_coverage_max_all.alias('max_plan_cov'), \
                                                         (F.trim(col('plan_cov.chdrnum')) ==  F.trim(col('max_plan_cov.max_chdrnum'))) &\
                                                         (F.trim(col('plan_cov.mbrno')) ==  F.trim(col('max_plan_cov.max_mbrno'))) &\
                                                         (F.trim(col('plan_cov.prodtyp')) ==  F.trim(col('max_plan_cov.max_prodtyp'))) &\
                                                         (F.trim(col('plan_cov.dpntno')) ==  F.trim(col('max_plan_cov.max_dpntno'))) &\
                                                         (F.trim(col('plan_cov.planno')) ==  F.trim(col('max_plan_cov.max_planno'))) &\
                                                         (F.trim(col('plan_cov.effdate')) ==  F.trim(col('max_plan_cov.max_effdate')))\
                                                        ,'inner')\


    print('### Preparing Plans and Coverages...')

    df_coverages_under_plans = df_plan_coverage_all.alias('cov_under_plans')\
                                                   .join(df_benefit_max_all.alias('benefit'), \
                                                         (F.trim(col('cov_under_plans.chdrnum')) ==  F.trim(col('benefit.max_chdrnum'))) &\
                                                         (F.trim(col('cov_under_plans.planno')) ==  F.trim(col('benefit.max_planno'))) &\
                                                         (F.trim(col('cov_under_plans.prodtyp')) ==  F.trim(col('benefit.max_prodtyp')))\
                                                         ,'left')\
                                                   .join(df_ref_policy_group_product_definition.alias('product_def'),\
                                                        (F.trim(col('cov_under_plans.prodtyp')) ==  F.trim(col('product_def.ktaxa_code')))\
                                                         ,'left')\
                                                   .join(df_ref_policy_group_benefit_definition.alias('benefit_def'),\
                                                        (F.trim(col('benefit.max_bencde')) ==  F.trim(col('benefit_def.ktaxa_code')))\
                                                         ,'left')\
                                                   .groupBy(F.trim(col('cov_under_plans.chdrnum')).alias('cov_uplan_chdrnum'),\
                                                            F.trim(col('cov_under_plans.mbrno')).alias('cov_uplan_mbrno'),\
                                                            F.trim(col('cov_under_plans.dpntno')).alias('cov_uplan_dpntno'),\
                                                            F.trim(col('cov_under_plans.planno')).alias('cov_uplan_planno'),\
                                                            F.trim(col('cov_under_plans.prodtyp')).alias('cov_uplan_prodtyp')\
                                                           )\
                                                        .agg(F.collect_list(\
                                                                     F.struct(\
                                                                                F.when(F.trim(col('benefit.max_bencde')).isNotNull()\
                                                                                       ,F.trim(col('benefit_def.oic_code')))\
                                                                                 .otherwise(F.trim(col('product_def.oic_code'))).cast(T.StringType()).alias('cov_plan_cov_code')\
                                                                               ,F.when(F.trim(col('benefit.max_bencde')).isNotNull()\
                                                                                       ,F.trim(col('benefit_def.oic_desc_th')))\
                                                                                 .otherwise(F.trim(col('product_def.oic_desc_th'))).cast(T.StringType()).alias('cov_plan_cov_detail')\
                                                                               ,F.when(F.trim(col('cov_under_plans.suminsu')).isNotNull()\
                                                                                       ,F.trim(col('cov_under_plans.suminsu')))\
                                                                                 .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_cov_amt')\
                                                                              ,F.when(F.trim(col('benefit.max_amtyear')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_amtyear')).cast(T.DecimalType(18, 2)))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_plan_cov_sum_insured_per_coverage_per_year')\
                                                                              ,F.when(F.trim(col('benefit.max_amtday')) > 0\
                                                                                       ,F.trim(col('benefit.max_amtday')).cast(T.DecimalType(18, 2)))\
                                                                                .when(F.trim(col('benefit.max_amtday')) == 0\
                                                                                       ,F.trim(col('benefit.max_amtvis')).cast(T.DecimalType(18, 2)))\
                                                                                .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_plan_cov_sum_insured_per_coverage_per_time')\
                                                                              ,F.when(F.trim(col('benefit.max_amtpropr')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_amtpropr')).cast(T.DecimalType(18, 2)))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_plan_cov_co_payment')\
                                                                              ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cov_plan_cov_deductible_amount')\
                                                                              ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_plan_cov_deductible_detail')\
                                                                              ,F.when(F.trim(col('benefit.max_daysvis')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_daysvis')))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_plan_cov_number_of_day')\
                                                                              ,lit('1').cast(T.StringType()).alias('cov_plan_cov_cover_year')\
                                                                              ,lit('1').cast(T.StringType()).alias('cov_plan_cov_payment_year')\
                                                                              ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_plan_cov_claim_status')\
                                                                             )\
                                                                           ).alias('coverages_under_plans'),\
                                                             F.collect_list(\
                                                                     F.struct(\
                                                                                F.when(F.trim(col('benefit.max_bencde')).isNotNull()\
                                                                                       ,F.trim(col('benefit_def.oic_code')))\
                                                                                 .otherwise(F.trim(col('product_def.oic_code'))).cast(T.StringType()).alias('cov_code')\
                                                                               ,F.when(F.trim(col('benefit.max_bencde')).isNotNull()\
                                                                                       ,F.trim(col('benefit_def.oic_desc_th')))\
                                                                                 .otherwise(F.trim(col('product_def.oic_desc_th'))).cast(T.StringType()).alias('cov_detail')\
                                                                               ,F.when(F.trim(col('cov_under_plans.suminsu')).isNotNull()\
                                                                                       ,F.trim(col('cov_under_plans.suminsu')))\
                                                                                 .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_amt')\
                                                                              ,F.when(F.trim(col('benefit.max_amtyear')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_amtyear')).cast(T.DecimalType(18, 2)))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_sum_insured_per_coverage_per_year')\
                                                                              ,F.when(F.trim(col('benefit.max_amtday')) > 0\
                                                                                       ,F.trim(col('benefit.max_amtday')).cast(T.DecimalType(18, 2)))\
                                                                                .when(F.trim(col('benefit.max_amtday')) == 0\
                                                                                       ,F.trim(col('benefit.max_amtvis')).cast(T.DecimalType(18, 2)))\
                                                                                .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_sum_insured_per_coverage_per_time')\
                                                                              ,F.when(F.trim(col('benefit.max_amtpropr')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_amtpropr')).cast(T.DecimalType(18, 2)))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_co_payment')\
                                                                              ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cov_deductible_amount')\
                                                                              ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_deductible_detail')\
                                                                              ,F.when(F.trim(col('benefit.max_daysvis')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_daysvis')))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_number_of_day')\
                                                                              ,lit('1').cast(T.StringType()).alias('cov_cover_year')\
                                                                              ,lit('1').cast(T.StringType()).alias('cov_payment_year')\
                                                                              ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_claim_status')\
                                                                             )\
                                                                           ).alias('coverages')
                                                            )


    df_coverages = df_plan_coverage_all.alias('cov_under_plans')\
                                                   .join(df_benefit_max_all.alias('benefit'), \
                                                         (F.trim(col('cov_under_plans.chdrnum')) ==  F.trim(col('benefit.max_chdrnum'))) &\
                                                         (F.trim(col('cov_under_plans.planno')) ==  F.trim(col('benefit.max_planno'))) &\
                                                         (F.trim(col('cov_under_plans.prodtyp')) ==  F.trim(col('benefit.max_prodtyp')))\
                                                         ,'left')\
                                                   .join(df_ref_policy_group_product_definition.alias('product_def'),\
                                                        (F.trim(col('cov_under_plans.prodtyp')) ==  F.trim(col('product_def.ktaxa_code')))\
                                                         ,'left')\
                                                   .join(df_ref_policy_group_benefit_definition.alias('benefit_def'),\
                                                        (F.trim(col('benefit.max_bencde')) ==  F.trim(col('benefit_def.ktaxa_code')))\
                                                         ,'left')\
                                                   .groupBy(F.trim(col('cov_under_plans.chdrnum')).alias('chdrnum'),\
                                                            F.trim(col('cov_under_plans.mbrno')).alias('mbrno'),\
                                                            F.trim(col('cov_under_plans.dpntno')).alias('dpntno')
                                                           )\
                                                        .agg(
                                                             F.collect_list(\
                                                                     F.struct(\
                                                                                F.when(F.trim(col('benefit.max_bencde')).isNotNull()\
                                                                                       ,F.trim(col('benefit_def.oic_code')))\
                                                                                 .otherwise(F.trim(col('product_def.oic_code'))).cast(T.StringType()).alias('cov_code')\
                                                                               ,F.when(F.trim(col('benefit.max_bencde')).isNotNull()\
                                                                                       ,F.trim(col('benefit_def.oic_desc_th')))\
                                                                                 .otherwise(F.trim(col('product_def.oic_desc_th'))).cast(T.StringType()).alias('cov_detail')\
                                                                               ,F.when(F.trim(col('cov_under_plans.suminsu')).isNotNull()\
                                                                                       ,F.trim(col('cov_under_plans.suminsu')))\
                                                                                 .otherwise(lit(REQ_NULL_DEC)).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_amt')\
                                                                              ,F.when(F.trim(col('benefit.max_amtyear')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_amtyear')).cast(T.DecimalType(18, 2)))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_sum_insured_per_coverage_per_year')\
                                                                              ,F.when(F.trim(col('benefit.max_amtday')) > 0\
                                                                                       ,F.trim(col('benefit.max_amtday')).cast(T.DecimalType(18, 2)))\
                                                                                .when(F.trim(col('benefit.max_amtday')) == 0\
                                                                                       ,F.trim(col('benefit.max_amtvis')).cast(T.DecimalType(18, 2)))\
                                                                                .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_sum_insured_per_coverage_per_time')\
                                                                              ,F.when(F.trim(col('benefit.max_amtpropr')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_amtpropr')).cast(T.DecimalType(18, 2)))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_co_payment')\
                                                                              ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cov_deductible_amount')\
                                                                              ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_deductible_detail')\
                                                                              ,F.when(F.trim(col('benefit.max_daysvis')).isNotNull()\
                                                                                       ,F.trim(col('benefit.max_daysvis')))\
                                                                                 .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()).alias('cov_number_of_day')\
                                                                              ,lit('1').cast(T.StringType()).alias('cov_cover_year')\
                                                                              ,lit('1').cast(T.StringType()).alias('cov_payment_year')\
                                                                              ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cov_claim_status')\
                                                                             )\
                                                                           ).alias('coverages')
                                                            )



    print('### Preparing Coverages Plan Sequence...')

    df_coverage_plan_seq = df_plan_coverage_all.alias('cov_plan_seq')\
                                                .join(df_coverages_under_plans.alias('cov_under_plans'), \
                                                         (F.trim(col('cov_plan_seq.chdrnum')) ==  F.trim(col('cov_under_plans.cov_uplan_chdrnum'))) &\
                                                         (F.trim(col('cov_plan_seq.mbrno')) ==  F.trim(col('cov_under_plans.cov_uplan_mbrno'))) &\
                                                         (F.trim(col('cov_plan_seq.dpntno')) ==  F.trim(col('cov_under_plans.cov_uplan_dpntno'))) &\
                                                         (F.trim(col('cov_plan_seq.prodtyp')) ==  F.trim(col('cov_under_plans.cov_uplan_prodtyp'))) &\
                                                         (F.trim(col('cov_plan_seq.planno')) ==  F.trim(col('cov_under_plans.cov_uplan_planno')))
                                                        ,'left')\
                                                .withColumn("row_num"\
                                                            ,F.row_number()\
                                                            .over(Window.partitionBy('chdrnum', 'mbrno', 'dpntno')\
                                                            .orderBy(df_plan_coverage_all['mbrno'])\
                                                             ).cast(T.IntegerType()).cast(T.StringType())\
                                                          )\
                                                   .groupBy(F.trim(col('cov_plan_seq.chdrnum')).alias('chdrnum'),\
                                                            F.trim(col('cov_plan_seq.mbrno')).alias('mbrno'),\
                                                            F.trim(col('cov_plan_seq.dpntno')).alias('dpntno')\
                                                           )\
                                                        .agg(F.collect_list(\
                                                                     F.struct(\
                                                                              col('row_num').cast(T.IntegerType()).cast(T.StringType()).alias('cov_plan_seq')\
                                                                              ,F.concat(F.trim(col('cov_plan_seq.planno'))\
                                                                                        ,lit('_')\
                                                                                        ,F.trim(col('cov_plan_seq.prodtyp'))\
                                                                                       ).cast(T.StringType()).alias('cov_plan_name')\
                                                                              ,F.trim(col('cov_plan_seq.suminsu')).cast(T.DecimalType(18, 2)).cast(T.StringType()).alias('cov_plan_sum_insured')\
                                                                              ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).cast(T.StringType())\
                                                                               .alias('cov_plan_maturity_benefit')
                                                                              ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).cast(T.StringType())\
                                                                              .alias('cov_plan_survival_benefit_min')\
                                                                              ,lit(REQ_NULL_DEC).cast(T.DecimalType(18, 2)).cast(T.StringType())\
                                                                              .alias('cov_plan_survival_benefit_max')\
                                                                              ,col('cov_under_plans.coverages_under_plans')\
                                                                         )\
                                                                           ).alias('policy_cov_plan_seqs')
                                                            )


    print('### Aggregating Output Policy table...')

    df_output_policy_clnt = df_member_grp_all.alias('mem').join(df_policy_grp_all.alias('pol'),\
                                              col('mem.chdrnum') == col('pol.chdrnum'),\
                                              'inner')\
                                            .join(df_policy_hist_all.alias('pol_hist'),\
                                              col('mem.chdrnum') == col('pol_hist.chdrnum'),\
                                              'left')\
                                            .join(df_plan_max_planno_benefit.alias('plan_benefit'),\
                                              col('mem.chdrnum') == col('plan_benefit.chdrnum'),\
                                              'inner')\
                                            .join(df_policy_owner_all.alias('owner'),\
                                                  (F.trim(col('pol.cownnum')) ==  F.trim(col('owner.clntnum')))&\
                                                  (F.trim(col('pol.chdrnum')) ==  F.trim(col('owner.chdrnum')))\
                                                  ,"inner")\
                                            .join(df_policy_owner_all_loc.alias('owner_loc'),\
                                                  (F.trim(col('pol.cownnum')) ==  F.trim(col('owner_loc.loc_owner_clntnum')))\
                                                  ,"left")\
                                            .join(df_clnt_all.alias('client'),\
                                                  (F.trim(col('mem.clntnum')) ==  F.trim(col('client.clntnum')))&\
                                                  (F.trim(col('mem.chdrnum')) ==  F.trim(col('client.chdrnum')))\
                                                  ,"inner")\
                                            .join(df_clnt_all_loc.alias('client_loc'),\
                                                  (F.trim(col('mem.clntnum')) ==  F.trim(col('client_loc.loc_clntnum')))\
                                                  ,"left")\
                                            .join(df_coverage_plan_seq.alias('cov_plan_seq'), \
                                                         (F.trim(col('mem.chdrnum')) ==  F.trim(col('cov_plan_seq.chdrnum'))) &\
                                                         (F.trim(col('mem.mbrno')) ==  F.trim(col('cov_plan_seq.mbrno'))) &\
                                                         (F.trim(col('mem.dpntno')) ==  F.trim(col('cov_plan_seq.dpntno')))\
                                                        ,'left')\
                                            .join(df_coverages.alias('sub_coverages'), \
                                                         (F.trim(col('mem.chdrnum')) ==  F.trim(col('sub_coverages.chdrnum'))) &\
                                                         (F.trim(col('mem.mbrno')) ==  F.trim(col('sub_coverages.mbrno'))) &\
                                                         (F.trim(col('mem.dpntno')) ==  F.trim(col('sub_coverages.dpntno')))\
                                                        ,'left')\
                                            .join(df_ref_policy_service_md_ref_policy_type.alias('ref_policy_type'),\
                                                  F.trim(col('pol.cnttype')) == F.trim(col('ref_policy_type.policy_type_ktaxa_code')), "inner")\
                                            .join(df_ref_policy_service_md_ref_policy_product_type.alias('ref_policy_product_type'),\
                                                  F.trim(col('pol.cnttype')) == F.trim(col('ref_policy_product_type.policy_product_type_ktaxa_code')), "inner")\
                                            .join(df_ref_group_pol_sub_type_lserff.alias('ref_lserff'),\
                                                  F.trim(col('pol.cnttype')) == F.trim(col('ref_lserff.plan_code')), "inner")\
                                            .join(df_ref_policy_group_pol_name.alias('ref_pol_name'),\
                                                  F.trim(col('pol.cnttype')) == F.trim(col('ref_pol_name.cnttype')), "inner")\
                                            .join(df_ref_policy_service_md_ref_policy_status.alias('ref_policy_status'),\
                                                  F.trim(col('pol.statcode')) == F.trim(col('ref_policy_status.policy_status_ktaxa_code')), "inner")\
                                            .join(df_policy_owner_info_all.alias('owner_info'),\
                                                  (F.trim(col('pol.cownnum')) ==  F.trim(col('owner_info.clntnum'))) &\
                                                  (F.trim(col('pol.chdrnum')) ==  F.trim(col('owner_info.chdrnum')))
                                                  ,"left")\
                                            .withColumn('pol_refer_code_of_company',concat(lit(pol_refer_code_of_company_prefix), F.trim(df_member_grp_all['chdrnum']),\
                                                                                           lit('_'), F.trim(df_member_grp_all['clntnum'])).cast(T.StringType())) \
                                            .withColumn('pol_transaction_status',lit(pol_transaction_status).cast(T.StringType())) \
                                            .withColumn('pol_company_id',lit(pol_company_id).cast(T.StringType())) \
                                            .withColumn('pol_id', F.trim(df_member_grp_all['chdrnum']).cast(T.StringType())) \
                                            .withColumn('pol_type', F.trim(df_ref_policy_service_md_ref_policy_type['policy_type_oic_code']).cast(T.StringType())) \
                                            .withColumn('pol_type_gli_name', F.when(F.trim(df_policy_hist_all['nofmbr']).isNotNull()\
                                                                                            , F.trim(df_policy_hist_all['nofmbr']))\
                                                                            .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()))\
                                            .withColumn('pol_type_gli_plan', F.trim(df_plan_max_planno_benefit['max_planno']).cast(T.IntegerType()).cast(T.StringType())) \
                                            .withColumn('pol_product_type', F.trim(df_ref_policy_service_md_ref_policy_product_type['policy_product_type_oic_code']).cast(T.StringType())) \
                                            .withColumn('pol_product_type_other_detail', lit(REQ_NULL_STR).cast(T.StringType()))\
                                            .withColumn('pol_type_2', lit('NP').cast(T.StringType()))\
                                            .withColumn('pol_sub_type', F.when(F.trim(df_ref_group_pol_sub_type_lserff['plan_code']).isNotNull()\
                                                                                            , F.trim(df_ref_group_pol_sub_type_lserff['lserff_code']))\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()))\
                                            .withColumn('pol_name', F.trim(df_ref_policy_group_pol_name['longdesc']).cast(T.StringType())) \
                                            .withColumn('pol_name_oic', F.trim(df_ref_policy_group_pol_name['longdesc']).cast(T.StringType()))\
                                            .withColumn('pol_start_date', concat(substring(F.trim(df_member_grp_all['dteatt']), 1, 4)\
                                                                                , lit('-')\
                                                                                , substring(F.trim(df_member_grp_all['dteatt']), 5, 2)\
                                                                                , lit('-')\
                                                                                ,substring(F.trim(df_member_grp_all['dteatt']), 7, 2)\
                                                                                , lit('T00:00:00+07:00')).cast(T.StringType())) \
                                            .withColumn('pol_end_date',   F.when(df_member_grp_all['dtetrm'] != 99999999,\
                                                                                 concat(substring(F.trim(df_member_grp_all['dtetrm']), 1, 4)\
                                                                                , lit('-')\
                                                                                , substring(F.trim(df_member_grp_all['dtetrm']), 5, 2)\
                                                                                , lit('-')\
                                                                                ,substring(F.trim(df_member_grp_all['dtetrm']), 7, 2)\
                                                                                , lit('T00:00:00+07:00')))\
                                                                          .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType())) \
                                            .withColumn('pol_claim_document', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                                            .withColumn('pol_payment_term', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                                            .withColumn('pol_payment_amount', lit(NON_REQ_NULL_NUM).cast(T.StringType())) \
                                            .withColumn("pol_payment_period", F.when(F.trim(df_policy_hist_all['billfreq']) == '00', lit('05'))\
                                                                               .when(F.trim(df_policy_hist_all['billfreq']) == '01', lit('04'))\
                                                                               .when(F.trim(df_policy_hist_all['billfreq']) == '02', lit('03'))\
                                                                               .when(F.trim(df_policy_hist_all['billfreq']) == '04', lit('02'))\
                                                                               .when(F.trim(df_policy_hist_all['billfreq']) == '12', lit('01'))\
                                                                              .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())\
                                                       )\
                                            .withColumn('pol_info_url', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                                            .withColumn('pol_claim_url',lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                                            .withColumn('pol_document_urls',F.array()) \
                                            .withColumn('pol_year', F.when(F.trim(df_policy_hist_all['pol_year']).isNotNull()\
                                                                                            , F.trim(df_policy_hist_all['pol_year']))\
                                                                   .otherwise(lit(NON_REQ_NULL_NUM)).cast(T.StringType()))\
                                            .withColumn('pol_status', F.trim(df_ref_policy_service_md_ref_policy_status['policy_status_oic_code']).cast(T.StringType()))\
                                            .withColumn('pol_status_note', lit(NON_REQ_NULL_STR).cast(T.StringType())) \
                                            .withColumn('pol_status_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_expropriation_value', lit(NON_REQ_NULL_NUM).cast(T.StringType())) \
                                            .withColumn('pol_immediate_refund', lit(NON_REQ_NULL_NUM).cast(T.StringType())) \
                                            .withColumn('pol_sale_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_transaction_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_approved_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_issue_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_account_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_agree_date',   lit(NON_REQ_NULL_DATE).cast(T.StringType())) \
                                            .withColumn('pol_distribution', lit('03').cast(T.StringType()))\
                                            .withColumn('pol_distribution_other_detail', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                            .withColumn('pol_license', F.when(F.trim(df_policy_hist_all['agntnum']).isNotNull()\
                                                                             ,F.trim(df_policy_hist_all['agntnum']))\
                                                                        .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType())\
                                                                        .alias('pol_license'))\
                                            .withColumn('pol_re_insured_contact_number', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                            .withColumn('pol_life_detail', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                            .withColumn("customer",\
                                                F.struct(\
                                                       F.when((F.trim(col('owner.clttype')) == 'P') &\
                                                              (F.trim(col('owner.natlty')) == 'THA') \
                                                              , lit('P')
                                                             )\
                                                         .when((F.trim(col('owner.clttype')) == 'P') &\
                                                              (F.trim(col('owner.natlty')) != 'THA') \
                                                              , lit('F')
                                                             )
                                                         .otherwise(lit('O'))\
                                                         .cast(T.StringType()).alias('cust_type')\
                                                       ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_type_other_detail')\
                                                       ,F.when((F.trim(col('owner.clttype')) == 'P') &\
                                                              (F.trim(col('owner.natlty')) == 'THA') \
                                                              , lit('01')
                                                             )\
                                                         .when((F.trim(col('owner.clttype')) == 'P') &\
                                                              (F.trim(col('owner.natlty')) != 'THA') \
                                                              , lit('02')
                                                             )
                                                         .otherwise(lit('99'))\
                                                         .cast(T.StringType()).alias('cust_id_type')\
                                                       ,F.when(F.trim(df_policy_owner_info_all['zwhidnum']).isNotNull() ,\
                                                                     F.trim(df_policy_owner_info_all['zwhidnum'])\
                                                              )\
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('cust_id')\
                                                       ,F.when(F.trim(col('owner.clttype')) == 'C'\
                                                                 , F.regexp_replace(F.trim(col('owner.lsurname')),\
                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                        )\
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('cust_company_name')\
                                                       ,F.when(F.trim(col('owner.clttype')) == 'P'\
                                                                 , F.regexp_replace(F.trim(col('owner.lgivname')),\
                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                        )\
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('cust_first_name')\
                                                       ,F.when(F.trim(col('owner.clttype')) == 'P'\
                                                                 , F.regexp_replace(F.trim(col('owner.lsurname')),\
                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                        )\
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('cust_last_name')\
                                                       ,F.when(F.trim(col('owner.clttype')) == 'C'\
                                                                 , lit(NON_REQ_NULL_NUM)
                                                              )
                                                         .when(F.trim(col('owner.clttype')) == 'P'\
                                                                 , F.floor((F.date_format(F.current_date(),'yyyyMMdd')-F.trim(col('owner.cltdob')))/10000)
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('cust_age')\
                                                       ,F.when(F.trim(col('owner.clttype')) == 'P'\
                                                                 , concat(substring(F.trim(col('owner.cltdob')), 1, 4)\
                                                                                , lit('-')\
                                                                                , substring(F.trim(col('owner.cltdob')), 5, 2)\
                                                                                , lit('-')\
                                                                                ,substring(F.trim(col('owner.cltdob')), 7, 2)\
                                                                                , lit('T00:00:00+07:00'))
                                                              )
                                                             .otherwise(lit(NON_REQ_NULL_DATE))
                                                             .cast(T.StringType()).alias('cust_birthday')\
                                                       ,F.when(F.trim(col('owner.clttype')) == 'C'\
                                                                 , lit('99')
                                                              )
                                                         .when((F.trim(col('owner.clttype')) == 'P') & (F.trim(col('owner.cltsex')) == 'M')\
                                                                 , lit('01')
                                                              )
                                                         .when((F.trim(col('owner.clttype')) == 'P') & (F.trim(col('owner.cltsex')) == 'F')\
                                                                 , lit('02')
                                                              )
                                                             .otherwise(lit('99'))
                                                             .cast(T.StringType()).alias('cust_gender')\
                                                        ,F.trim(F.regexp_replace(concat(F.trim(col('owner.cltaddr01')),\
                                                                                 lit(' '),\
                                                                                 F.trim(col('owner.cltaddr02')),\
                                                                                 lit(' '),\
                                                                                 F.trim(col('owner.cltaddr03')),\
                                                                                 lit(' '),\
                                                                                 F.trim(col('owner.cltaddr04'))),\
                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                        ))\
                                                                        .cast(T.StringType()).alias('cust_address')\
                                                        ,F.when(F.trim(col('owner_loc.output_sub_district_code')).isNotNull() ,\
                                                                 F.trim(col('owner_loc.output_sub_district_code'))\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_sub_district')\
                                                       ,F.when(F.trim(col('owner_loc.output_district_code')).isNotNull() ,\
                                                                 F.trim(col('owner_loc.output_district_code'))\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_district')\
                                                       ,F.when(F.trim(col('owner_loc.output_province_code')).isNotNull() ,\
                                                                 F.trim(col('owner_loc.output_province_code'))\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('cust_province')\
                                                         ,F.when(F.trim(col('owner.cltpcode')) != '' ,\
                                                                 F.trim(col('owner.cltpcode'))\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_zip_code')\
                                                         ,F.when(F.trim(col('owner.ctrycode')) != '' ,\
                                                                 F.trim(col('owner.ctrycode'))\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_country_code')\
                                                         ,F.when(F.trim(col('owner.cltphone01')) != '' ,\
                                                                 F.trim(col('owner.cltphone01'))\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_phone_number')\
                                                         ,F.when(F.trim(col('owner.cltphone02')) != '' ,\
                                                                 F.trim(col('owner.cltphone02'))\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_mobile_number')\
                                                         ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_email')\
                                                         ,F.when(F.trim(col('owner.clttype')) == 'C'\
                                                                 , lit(NON_REQ_NULL_NUM)
                                                              )
                                                         .when(F.trim(col('owner.clttype')) == 'P'\
                                                                 , F.floor((F.trim(df_policy_hist_all['effdate'])-F.trim(col('owner.cltdob')))/10000)
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('cust_issued_age')\
                                                         ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_race')\
                                                         ,F.when(F.trim(col('owner.natlty')) != '' ,\
                                                                 F.trim(col('owner.natlty'))\
                                                              )\
                                                         .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('cust_nationality')\
                                                         ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_religion')\
                                                         ,lit('99').cast(T.StringType()).alias('cust_occupation_level')\
                                                         ,lit(NON_REQ_NULL_STR).alias('cust_occupation_level_other_detail')\
                                                         ,(lit(REQ_NULL_STR)).cast(T.StringType()).cast(T.StringType()).alias('cust_occupation')\
                                                         ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_job')\
                                                         ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_position')\
                                                         ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('cust_business_type')\
                                                         ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('cust_income_per_year')\
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
                                                  F.struct( lit('1').cast(T.IntegerType()).cast(T.StringType()).alias('insrd_cov_plan_seq')\
                                                           ,lit('1').cast(T.IntegerType()).cast(T.StringType()).alias('insrd_seq')\
                                                           ,F.when((F.trim(col('client.clttype')) == 'P') &\
                                                              (F.trim(col('client.natlty')) == 'THA') \
                                                              , lit('P')
                                                             )\
                                                             .when((F.trim(col('client.clttype')) == 'P') &\
                                                              (F.trim(col('client.natlty')) != 'THA') \
                                                              , lit('F')
                                                             )
                                                             .when((F.trim(col('client.natlty')) == 'C')\
                                                              , lit('C')
                                                             )
                                                             .otherwise(lit(REQ_NULL_STR))\
                                                             .cast(T.StringType()).alias('insrd_type')\
                                                           ,F.when((F.trim(col('client.clttype')) == 'P') &\
                                                              (F.trim(col('client.natlty')) == 'THA') \
                                                              , lit('01')
                                                             )\
                                                             .when((F.trim(col('client.clttype')) == 'P') &\
                                                              (F.trim(col('client.natlty')) != 'THA') \
                                                              , lit('02')
                                                             )
                                                             .when((F.trim(col('client.clttype')) == 'C')\
                                                              , lit('99')
                                                             )
                                                             .otherwise(lit('99'))\
                                                             .cast(T.StringType()).alias('insrd_id_type')\
                                                           ,F.when((F.trim(col('client.clttype')) == 'P')
                                                              , F.trim(col('client.secuityno'))
                                                             )\
                                                             .when((F.trim(col('client.clttype')) == 'C')
                                                              , F.regexp_replace(F.trim(substring(df_policy_owner_info_all['zwhidnum'],1,100)), 
                                                                "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", "")\
                                                             )
                                                             .otherwise(lit('99'))\
                                                             .cast(T.StringType()).alias('insrd_id')\
                                                           ,F.when(F.trim(col('client.clttype')) == 'P'\
                                                                 , F.regexp_replace(F.trim(substring(col('client.lgivname'),1,100)),\
                                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                                   )\
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('insrd_first_name')\
                                                           ,F.when(F.trim(col('client.clttype')) == 'P'\
                                                                 , F.regexp_replace(F.trim(substring(col('client.lsurname'),1,100)),\
                                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                                   )\
                                                                  )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('insrd_last_name')\
                                                           ,F.when(F.trim(col('client.clttype')) == 'C'\
                                                                 , lit(NON_REQ_NULL_NUM)
                                                              )
                                                             .when(F.trim(col('client.clttype')) == 'P'\
                                                                 , F.trim(df_member_grp_all['age'])
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('insrd_age')\
                                                           ,F.when(F.trim(col('client.clttype')) == 'C'\
                                                                 , lit(NON_REQ_NULL_NUM)
                                                              )
                                                             .when(F.trim(col('client.clttype')) == 'P'\
                                                                 , concat(substring(F.trim(col('client.cltdob')), 1, 4)\
                                                                                , lit('-')\
                                                                                , substring(F.trim(col('client.cltdob')), 5, 2)\
                                                                                , lit('-')\
                                                                                ,substring(F.trim(col('client.cltdob')), 7, 2)\
                                                                                , lit('T00:00:00+07:00'))
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('insrd_birthday')\
                                                           ,F.when(F.trim(col('client.clttype')) == 'C'\
                                                                     , lit('99')
                                                                  )
                                                             .when((F.trim(col('client.clttype')) == 'P') & (F.trim(col('client.cltsex')) == 'M')\
                                                                 , lit('01')
                                                                  )
                                                             .when((F.trim(col('client.clttype')) == 'P') & (F.trim(col('client.cltsex')) == 'F')\
                                                                 , lit('02')
                                                                  )
                                                             .otherwise(lit('99'))
                                                             .cast(T.StringType()).alias('insrd_gender')\
                                                           ,F.trim(F.regexp_replace(concat(F.trim(col('client.cltaddr01')),\
                                                                                 lit(' '),\
                                                                                 F.trim(col('client.cltaddr02')),\
                                                                                 lit(' '),\
                                                                                 F.trim(col('client.cltaddr03')),\
                                                                                 lit(' '),\
                                                                                 F.trim(col('client.cltaddr04'))),\
                                                                        "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""\
                                                                        )\
                                                                   )\
                                                                        .cast(T.StringType()).alias('insrd_address')\
                                                        ,F.when(F.trim(col('client_loc.output_sub_district_code')).isNotNull() ,\
                                                                 F.trim(col('client_loc.output_sub_district_code'))\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('insrd_sub_district')\
                                                       ,F.when(F.trim(col('client_loc.output_district_code')).isNotNull() ,\
                                                                 F.trim(col('client_loc.output_district_code'))\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('insrd_district')\
                                                       ,F.when(F.trim(col('client_loc.output_province_code')).isNotNull() ,\
                                                                 F.trim(col('client_loc.output_province_code'))\
                                                              )\
                                                         .otherwise(lit('00')).cast(T.StringType()).alias('insrd_province')\
                                                           ,F.when(F.trim(col('owner.cltpcode')) != '' ,\
                                                                 F.trim(col('owner.cltpcode'))\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_zip_code')\
                                                           ,F.when(F.trim(col('client.ctrycode')) != '' ,\
                                                                 F.trim(col('client.ctrycode'))\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_country_code')\
                                                           ,F.when(F.trim(col('client.cltphone01')) != '' ,\
                                                                 F.trim(col('client.cltphone01'))\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_phone_number')\
                                                           ,F.when(F.trim(col('client.cltphone02')) != '' ,\
                                                                 F.trim(col('client.cltphone02'))\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_mobile_number')\
                                                            
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_email')\
                                                          ,F.when(F.trim(col('client.clttype')) == 'C'\
                                                                 , lit(NON_REQ_NULL_NUM)
                                                              )
                                                            .when(F.trim(col('client.clttype')) == 'P'\
                                                                 , F.floor((F.trim(df_policy_hist_all['effdate'])-F.trim(col('client.cltdob')))/10000)
                                                              )
                                                             .otherwise(lit(REQ_NULL_STR))
                                                             .cast(T.StringType()).alias('insrd_issued_age')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_race')\
                                                           ,F.when(F.trim(col('client.natlty')) != '' ,\
                                                                 F.trim(col('client.natlty'))\
                                                              )\
                                                             .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()).alias('insrd_nationality')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_religion')\
                                                           ,lit('99').cast(T.StringType()).alias('insrd_occupation_level')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation_level_other_detail')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_occupation')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_job')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_position')\
                                                           ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('insrd_business_type')\
                                                           ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('insrd_income_per_year')\
                                                           ,F.array(lit('{}').cast(T.StringType())).alias('insrd_pol_sub_ids')\
                                                           ,F.array().alias('insrd_beneficiaries')\
                                                      )\
                                               )\
                                            )\
                                            .withColumn('sub_policies', \
                                               F.array(\
                                                 F.struct(\
                                                      F.concat(\
                                                                F.trim(df_policy_grp_all['chdrnum']),lit('_00001')\
                                                              ).cast(T.StringType()).alias('sub_pol_id')\
                                                      ,lit('01').cast(T.StringType()).alias('sub_pol_type')\
                                                      ,F.when(F.trim(df_ref_group_pol_sub_type_lserff['plan_code']).isNotNull()\
                                                                                            , F.trim(df_ref_group_pol_sub_type_lserff['lserff_code']))\
                                                                    .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())\
                                                      .alias('sub_pol_ppt')
                                                      ,lit(NON_REQ_NULL_DATE).cast(T.StringType()).cast(T.StringType()).alias('sub_pol_start_date')\
                                                      ,lit(NON_REQ_NULL_DATE).cast(T.StringType()).cast(T.StringType()).alias('sub_pol_end_date')\
                                                      ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('sub_pol_sum_insured_per_year_exclude')\
                                                      ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('sub_pol_sum_insured_per_year_include')\
                                                      ,col('cov_plan_seq.policy_cov_plan_seqs')\
                                                      ,col('sub_coverages.coverages')\
                                                      )\
                                                      )\
                                                    )\
                                            .withColumn('beneficiaries', \
                                                         F.array(\
                                                           F.struct(\
                                                                        lit(REQ_NULL_INT).cast(T.IntegerType()).cast(T.StringType()).alias('beneficiary_seq_no')\
                                                                        ,lit(REQ_NULL_STR).cast(T.StringType()).alias('beneficiary_name')\
                                                                        ,lit('99').cast(T.StringType()).alias('beneficiary_relation')\
                                                                        ,lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('beneficiary_relation_other_detail')\
                                                                        ,lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('benefit_ratio')\
                                                                      )\
                                                                 )\
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


                                                    

    output_policy_df_final = df_output_policy_clnt\
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
    ,'customer'
    ,'insureds'
    ,'sub_policies'
    ,'beneficiaries'
    ,'endorsements'
    ,'loans'
    ,'loan_payments'
    ,'investments'        
    ).dropDuplicates()


    return output_policy_df_final