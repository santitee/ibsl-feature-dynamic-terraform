'''
job name     : IBS Ordinary Payment Utilities module.
Created by   : Kiatipoom Poopanitpun
Created date : 6 Mar 2022
version      : 0.1
Modified by   : Santi Teeragul
Modified date : 9 Feb 2026
version      : 0.2
Modifiedby   : Santi Teeragul
Modified date : 23 Mar 2026
version      : 0.3 Payment Premium Aggregation Logic Update - Add Join with Premium Table to get Premium Amount for Both Basic and Rider, Add New Column for Premium Sequence, Add Payment Channel Detail, Add Payment Type Riders, Add Payment Type Endorsements, Add Join with Ref Table to get Payment Type, Payment Direct Premium and Payment Period, Update Logic for Refer Code of Company and System Data Adjustment
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


def ordinary_payment_aggregation(run_params, spark_session, start_date, end_date):
    print('Reading data source transactional tables...')
    if(run_params.workflow_type == 'Daily') :
        print('Reading data source transactional tables...')
        df_t_payment_transaction_source = spark_session.table(
            'processing_ibs.ibs_t_policy_payment_transaction')\
                    .withColumn('date_cleaned', F.date_format(F.to_date(F.col('trx_dt'), 'yyyy-MM-dd'), 'yyyyMMdd'))\
                    .filter((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date))\
                    .drop('date_cleaned')
    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')\
                .filter((F.col('pstu').isin('1','2','5','6','B','F')))

        df_t_payment_transaction_source = spark_session.table("processing_ibs.ibs_t_policy_payment_transaction")
        
        # TODO: Add Others Payment Transaction      
        #1 Add Others Payment Transaction
        df_t_payment_others_transaction_source = spark_session.table("processing_ibs.ibs_t_policy_payment_others_transaction")
    
        #2 Add Others Payment Transaction - Union with Main Payment Transaction
        df_t_payment_transaction_source = df_t_payment_transaction_source.union(df_t_payment_others_transaction_source)

        df_t_payment_transaction_source = df_t_payment_transaction_source\
                .join(df_lfkludta_lfppml_source, df_t_payment_transaction_source['pol_no'] == df_lfkludta_lfppml_source['pno'], 'inner') \
                    .withColumn('date_cleaned', F.date_format(F.to_date(F.col('trx_dt'), 'yyyy-MM-dd'), 'yyyyMMdd'))\
                    .filter(((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date)))\
                    .drop('date_cleaned')\
                .select(df_t_payment_transaction_source['*'])
    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_lfkludta_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkludta_lfppml')\
                .filter((~F.col('pstu').isin('1','2','5','6','B','F')))
        
        df_t_payment_transaction_source = spark_session.table("processing_ibs.ibs_t_policy_payment_transaction")
        
        # TODO: Add Others Payment Transaction      
        #1 Add Others Payment Transaction
        df_t_payment_others_transaction_source = spark_session.table("processing_ibs.ibs_t_policy_payment_others_transaction")
    
        #2 Add Others Payment Transaction - Union with Main Payment Transaction
        df_t_payment_transaction_source = df_t_payment_transaction_source.union(df_t_payment_others_transaction_source)
        
        df_t_payment_transaction_source = df_t_payment_transaction_source\
                .join(df_lfkludta_lfppml_source, df_t_payment_transaction_source['pol_no'] == df_lfkludta_lfppml_source['pno'], 'inner') \
                    .withColumn('date_cleaned', F.date_format(F.to_date(F.col('trx_dt'), 'yyyy-MM-dd'), 'yyyyMMdd'))\
                    .filter(((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date)))\
                    .drop('date_cleaned')\
                .select(df_t_payment_transaction_source['*'])

    print('Reading ref tables...')
    df_ref_policy_service_md_ref_source = spark_session.table(
        'processing_ibs.ibs_ref_policy_service_md_ref')

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
    
    SYSTEM_DATA_ADJ = 'System Data Adjustment'

    df_t_policy_premium_source = spark_session.table(
        'processing_ibs.ibs_t_policy_premium')
    
    # Step 1: สร้าง window สำหรับ row_number ต่อ policy_number
    sub_policy_window = Window.partitionBy("due_date").orderBy("due_date")

    # Step 2: เพิ่ม row_number และ concat เป็น pmt_premium_rid_number
    latest_premium_rider = df_t_policy_premium_source \
    .filter(F.col("premium_level") == "Rider") \
    .withColumn("row_num", F.row_number().over(sub_policy_window)) \
    .withColumn("pmt_premium_rid_number", 
    F.concat_ws("_", 
    F.col("policy_number").cast(T.StringType()), 
    F.col("row_num").cast(T.StringType())
    )) \
    .withColumn("pmt_premium_rid_amount", 
    F.col("regular_premium").cast(T.DecimalType(18, 2))) \
    .groupBy('policy_number','due_date')\
    .agg(F.sort_array(F.collect_list(F.struct(F.col('pmt_premium_rid_number').alias('pmt_premium_rid_number'), 
                                            F.col('pmt_premium_rid_amount').alias('pmt_premium_rid_amount')))).alias('paymentrider'))
    # Removed:
    # latest_premium_with_row = df_t_policy_premium_source \
    # .filter(F.col("premium_level") == "Rider") \
    # .withColumn("row_num", F.row_number().over(sub_policy_window)) \
    # .withColumn("pmt_premium_rid_number", 
    # F.concat_ws("_", 
    # F.col("policy_number").cast(T.StringType()), 
    # F.col("row_num").cast(T.StringType())
    # )) \
    # .withColumn("pmt_premium_rid_amount", 
    # F.col("regular_premium").cast(T.DecimalType(18, 2))) \
    # .select("sequence", "payment_header_seq","due_date", "plan_code","policy_number","premium_level", "pmt_premium_rid_number", "pmt_premium_rid_amount")

    # latest_premium_rider = latest_premium_with_row.groupby('payment_header_seq')\
    # .agg(F.sort_array(F.collect_list(F.struct(F.col('pmt_premium_rid_number').alias('pmt_premium_rid_number'), 
    #                                         F.col('pmt_premium_rid_amount').alias('pmt_premium_rid_amount')))).alias('paymentrider'))
    
    ## Sum Basic + Rider and Only Basic
    latest_premium_sum_basic_rider = df_t_policy_premium_source \
    .groupBy('policy_number','due_date')\
    .agg(F.sum("regular_premium").alias('pmt_prd_premium_amount'))


    latest_premium_basic = df_t_policy_premium_source \
    .filter(F.col("premium_level") == "Basic") \
    .withColumn("pmt_prd_premium_amount_life",  F.col("regular_premium").cast(T.DecimalType(18, 2))) \
    .select('policy_number','due_date', 'pmt_prd_premium_amount_life')
    
    # df_t_payment_transaction_source = df_t_payment_transaction_source.join(
    #     latest_premium_rider,
    #     df_t_payment_transaction_source["pmt_seq"] == latest_premium_rider["payment_header_seq"],
    #     "left"
    # )
    
    ## Add join new logic to get premium amount for both basic and rider
    df_t_payment_transaction_source = df_t_payment_transaction_source\
    .join(
        latest_premium_rider,
        (
            (df_t_payment_transaction_source["pol_no"] == latest_premium_rider["policy_number"]) &
            (df_t_payment_transaction_source["due_dt"] == latest_premium_rider["due_date"])
        ),
        "left"
    )
    df_t_payment_transaction_source = df_t_payment_transaction_source\
    .join(
        latest_premium_sum_basic_rider,
        (
            (df_t_payment_transaction_source["pol_no"] == latest_premium_sum_basic_rider["policy_number"]) &
            (df_t_payment_transaction_source["due_dt"] == latest_premium_sum_basic_rider["due_date"])
        ),
        "left"
    )
    df_t_payment_transaction_source = df_t_payment_transaction_source\
    .join(
        latest_premium_basic,
        (
            (df_t_payment_transaction_source["pol_no"] == latest_premium_basic["policy_number"]) &
            (df_t_payment_transaction_source["due_dt"] == latest_premium_basic["due_date"])
        ),
        "left"
    )

    print('Adding new column for pmt_prd_seq...')
    df_t_payment_transaction = df_t_payment_transaction_source\
                                .withColumn('pmt_prd_premium_seq', F.row_number().over(Window.partitionBy("pmt_seq"
                                                                                                        , "pol_no"
                                                                                                        , "pol_year"
                                                                                                        , "pmt_premium_type"
                                                                                                        , "pmt_type"
                                                                                                        , "prm_mode"
                                                                                                        , "pmt_prm_term_year").orderBy("pmt_prd_seq"
                                                                                                                                    ,"trx_dt"
                                                                                                                                    ,"next_due_dt"
                                                                                                                                    ,"cbr_dt"
                                                                                                                                    ,"sub_dt"
                                                                                                                                    ,"receipt_no")))


    print('Declaring Ref table...')
    df_ref_policy_service_md_ref_payment_type = df_ref_policy_service_md_ref_source\
                                                                .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'payment_type') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                                .withColumnRenamed('ktaxa_code', 'payment_type_ktaxa_code')\
                                                                .withColumnRenamed('id_code', 'payment_type_oic_code')

    print('Declaring Payment Direct Premium by map with Ref table...')
    df_ref_policy_service_md_ref_payment_direct_premium = df_ref_policy_service_md_ref_source\
                                                                .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'payment_direct_premium') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                                .withColumnRenamed('ktaxa_code', 'payment_direct_premium_ktaxa_code')\
                                                                .withColumnRenamed('id_code', 'payment_direct_premium_oic_code')

    print('Declaring Payment Period by map with Ref table...')
    df_ref_policy_service_md_ref_payment_period = df_ref_policy_service_md_ref_source\
                                                                .filter((F.trim(df_ref_policy_service_md_ref_source['md_name']) == 'policy_payment_period') & (F.trim(df_ref_policy_service_md_ref_source['ktaxa_code']) != ''))\
                                                                .withColumnRenamed('ktaxa_code', 'payment_period_ktaxa_code')\
                                                                .withColumnRenamed('id_code', 'payment_period_oic_code')

    print('Aggregating Output Policy Payment table...')
    df_payment = df_t_payment_transaction\
                                .groupBy(\
                                        df_t_payment_transaction['pmt_seq']\
                                        ,df_t_payment_transaction['pol_no']\
                                        ,df_t_payment_transaction['pol_year']\
                                        ,df_t_payment_transaction['pmt_premium_type']\
                                        ,df_t_payment_transaction['pmt_type']\
                                        ,df_t_payment_transaction['prm_mode']\
                                        ,df_t_payment_transaction['pmt_prm_term_year']\
                                        )\
                                            .agg(\
                                                    F.sort_array(\
                                                                F.collect_list(\
                                                                        F.struct(\
                                                                            F.when(\
                                                                                    df_t_payment_transaction['pmt_prd_premium_seq'].isNotNull()\
                                                                                    ,df_t_payment_transaction['pmt_prd_premium_seq'].cast(T.IntegerType())
                                                                                    )\
                                                                                .otherwise(REQ_NULL_INT).cast(T.IntegerType()).alias('pmt_prd_premium_seq')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['pmt_prd_outstanding'].isNotNull()\
                                                                                    ,df_t_payment_transaction['pmt_prd_outstanding'].cast(T.IntegerType())
                                                                                    )\
                                                                                .otherwise(REQ_NULL_INT).cast(T.IntegerType()).alias('pmt_prd_premium_outstanding_payment')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['pmt_prd_premium_amount'].isNotNull()\
                                                                                    ,df_t_payment_transaction['pmt_prd_premium_amount'].cast(T.DecimalType(18,2))
                                                                                    )\
                                                                                .otherwise(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amount')\
                                                                            ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amount_tax')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['pmt_prd_premium_amount_life'].isNotNull()\
                                                                                    ,df_t_payment_transaction['pmt_prd_premium_amount_life'].cast(T.DecimalType(18,2))
                                                                                    )\
                                                                                .otherwise(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amount_life')\
                                                                            ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amt_saving')\
                                                                            ,F.when((df_t_payment_transaction['il_sp_prm'].isNotNull())&(df_t_payment_transaction['tpl_sp_prm'].isNotNull()), 
                                                                                F.lit(df_t_payment_transaction['il_sp_prm'] + df_t_payment_transaction['tpl_sp_prm']))\
                                                                            .when((df_t_payment_transaction['il_sp_prm'].isNotNull())&(df_t_payment_transaction['tpl_sp_prm'].isNull()),
                                                                                F.lit(df_t_payment_transaction['il_sp_prm']))\
                                                                            .when((df_t_payment_transaction['il_sp_prm'].isNull())&(df_t_payment_transaction['tpl_sp_prm'].isNotNull()),
                                                                                F.lit(df_t_payment_transaction['tpl_sp_prm']))\
                                                                            .otherwise(REQ_NULL_DEC)\
                                                                                .cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amt_investment')\
                                                                            ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amount_other')\
                                                                            ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amount_com')\
                                                                            ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amt_interest')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['trx_dt'].isNotNull()\
                                                                                    ,F.concat(df_t_payment_transaction['trx_dt'],lit('T00:00:00+07:00'))\
                                                                                    )\
                                                                                .otherwise(REQ_NULL_DATE).cast(T.StringType()).alias('pmt_prd_premium_date')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['due_dt'].isNotNull()\
                                                                                    ,F.concat(df_t_payment_transaction['due_dt'],lit('T00:00:00+07:00'))\
                                                                                    )\
                                                                                .otherwise(F.concat(df_t_payment_transaction['trx_dt'],lit('T00:00:00+07:00'))).cast(T.StringType()).alias('pmt_prd_premium_due_date')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['trx_dt'].isNotNull()\
                                                                                    ,F.concat(df_t_payment_transaction['trx_dt'],lit('T00:00:00+07:00'))\
                                                                                    )\
                                                                                .otherwise(REQ_NULL_DATE).cast(T.StringType()).alias('pmt_prd_premium_temp_receipt_date')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['trx_dt'].isNotNull()\
                                                                                    ,F.concat(df_t_payment_transaction['trx_dt'],lit('T00:00:00+07:00'))\
                                                                                    )\
                                                                                .otherwise(REQ_NULL_DATE).cast(T.StringType()).alias('pmt_prd_premium_receipt_date')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['receipt_no'].isNotNull()\
                                                                                    ,df_t_payment_transaction['receipt_no']
                                                                                    )\
                                                                                .otherwise(REQ_NULL_STR).cast(T.StringType()).alias('pmt_prd_premium_temp_receipt_number')\
                                                                            ,F.when(\
                                                                                    df_t_payment_transaction['receipt_no'].isNotNull()\
                                                                                    ,df_t_payment_transaction['receipt_no']
                                                                                    )\
                                                                                .otherwise(REQ_NULL_STR).cast(T.StringType()).alias('pmt_prd_premium_receipt_number')\
                                                                            ,F.when((F.trim(df_t_payment_transaction['pmt_method']) == 'Cash') &\
                                                                                    (~F.trim(df_t_payment_transaction['pmt_channel']).isin('Counter Service', 'AIS', 'Big C', 'EDC', 'LINE PAY', 'LinePay', 'mPOS', 'Quickpay', 'Tesco Lotus', 'True Money'))\
                                                                                    ,'02'\
                                                                                    )\
                                                                            .when((F.trim(df_t_payment_transaction['pmt_method']) == 'Cash') &\
                                                                                    (F.trim(df_t_payment_transaction['pmt_channel']).isin('Counter Service', 'AIS', 'Big C', 'EDC', 'LINE PAY', 'LinePay', 'mPOS', 'Quickpay', 'Tesco Lotus', 'True Money'))\
                                                                                    ,'07'\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Pay-in & Bank Transfer', 'Card Payment')
                                                                                    ,'03'\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Cheque')
                                                                                    ,'04'\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Debit Authorization (DA)')
                                                                                    ,'05'\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Credit Card Payment Auth (CCPA)')
                                                                                    ,'06'\
                                                                                )\
                                                                            .otherwise('99').cast(T.StringType()).alias('pmt_prd_premium_channel')\
                                                                            
                                                                            # TODO: Add SYSTEM_DATA_ADJ = 'System Data Adjustment' on pmt_prd_premium_channel = '99'
                                                                                
                                                                            ,F.when((F.trim(df_t_payment_transaction['pmt_method']) == 'Cash') &\
                                                                                    (~F.trim(df_t_payment_transaction['pmt_channel']).isin('Counter Service', 'AIS', 'Big C', 'EDC', 'LINE PAY', 'LinePay', 'mPOS', 'Quickpay', 'Tesco Lotus', 'True Money'))\
                                                                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType())\
                                                                                    )\
                                                                            .when((F.trim(df_t_payment_transaction['pmt_method']) == 'Cash') &\
                                                                                    (F.trim(df_t_payment_transaction['pmt_channel']).isin('Counter Service', 'AIS', 'Big C', 'EDC', 'LINE PAY', 'LinePay', 'mPOS', 'Quickpay', 'Tesco Lotus', 'True Money'))\
                                                                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType())\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Pay-in & Bank Transfer', 'Card Payment')
                                                                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType())\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Cheque')
                                                                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType())\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Debit Authorization (DA)')
                                                                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType())\
                                                                                )\
                                                                            .when(F.trim(df_t_payment_transaction['pmt_method']).isin('Credit Card Payment Auth (CCPA)')
                                                                                    ,lit(NON_REQ_NULL_STR).cast(T.StringType())\
                                                                                )\
                                                                            .otherwise('SYSTEM_DATA_ADJ').cast(T.StringType()).alias('pmt_prd_premium_channel_detail')\
                                                                            ,F.col('paymentrider').alias('payment_premium_type_riders')\
                                                                            ,F.array(\
                                                                                    F.struct(\
                                                                                                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('pmt_premium_edm_number')
                                                                                                ,lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_premium_edm_amount')
                                                                                                )\
                                                                                    ).alias('payment_premium_type_endorsements')\
                                                                            )\
                                                                )\
                                                                ).alias('payment_period_seqs')\
                                                )

    df_payment = df_payment\
                    .join(df_ref_policy_service_md_ref_payment_type, df_t_payment_transaction['pmt_type'] == df_ref_policy_service_md_ref_payment_type['payment_type_ktaxa_code']\
                    ,'left')\
                    .join(df_ref_policy_service_md_ref_payment_direct_premium, df_t_payment_transaction['pmt_premium_type'] == df_ref_policy_service_md_ref_payment_direct_premium['payment_direct_premium_ktaxa_code']\
                    ,'left')\
                    .join(df_ref_policy_service_md_ref_payment_period, df_t_payment_transaction['prm_mode'] == df_ref_policy_service_md_ref_payment_period['payment_period_ktaxa_code']\
                    ,'left')\
                    .withColumn('pmt_refer_code_of_company', F.concat(lit('PMT_KAL_'), df_payment['pmt_seq'], lit('_'), F.row_number().over(Window.partitionBy("pmt_seq").orderBy("pol_no"
                                                                                                                                                                ,"pmt_type"
                                                                                                                                                                ,"pmt_premium_type"
                                                                                                                                                                ,"pol_year"
                                                                                                                                                                ,"prm_mode"))).cast(T.StringType()))\
                    .withColumn('pmt_transaction_status', lit(pol_transaction_status).cast(T.StringType()))\
                    .withColumn('pmt_company_id', lit(pol_company_id).cast(T.StringType()))\
                    .withColumn('pmt_pol_id', df_payment['pol_no'].cast(T.StringType()))\
                    .withColumn('pmt_pol_refer_code_of_company', F.concat(lit('POL_KAL_'), df_payment['pol_no']).cast(T.StringType()))\
                    .withColumn('pmt_id', df_payment['pmt_seq'].cast(T.StringType()))\
                    .withColumn('pmt_type', df_ref_policy_service_md_ref_payment_type['payment_type_oic_code'].cast(T.StringType()))\
                    .withColumn('pmt_direct_premium', df_ref_policy_service_md_ref_payment_direct_premium['payment_direct_premium_oic_code'].cast(T.StringType()))\
                    .withColumn('pmt_premium_payment_period_year', \
                                F.when((df_payment['pmt_prm_term_year'].isNotNull()) &\
                                    (F.trim(df_payment['pmt_prm_term_year']) != '')\
                                        ,df_payment['pmt_prm_term_year'].cast(T.IntegerType())
                                    )\
                                .otherwise(lit(REQ_NULL_INT).cast(T.IntegerType()))
                                )\
                    .withColumn('pmt_premium_payment_year', \
                                F.when((df_payment['pol_year'].isNotNull()) &\
                                    (F.trim(df_payment['pol_year']) != '')\
                                        ,df_payment['pol_year'].cast(T.IntegerType())
                                    )\
                                .otherwise(lit(REQ_NULL_INT).cast(T.IntegerType()))
                                )\
                    .withColumn('pmt_payment_period', F.when(df_ref_policy_service_md_ref_payment_period['payment_period_oic_code'] != '',
                                                                df_ref_policy_service_md_ref_payment_period['payment_period_oic_code'].cast(T.StringType()))\
                                                                .otherwise(lit('05').cast(T.StringType())))

    print(
        '--Consolidating every elements to final Ordinary Payment Dataframe...'
    )

    output_payment_final = df_payment.select('pmt_refer_code_of_company'\
                                                ,'pmt_transaction_status'\
                                                ,'pmt_company_id'\
                                                ,'pmt_pol_id'\
                                                ,'pmt_pol_refer_code_of_company'\
                                                ,'pmt_id'\
                                                ,'pmt_type'\
                                                ,'pmt_direct_premium'\
                                                ,'pmt_premium_payment_period_year'\
                                                ,'pmt_premium_payment_year'\
                                                ,'pmt_payment_period'\
                                                ,'payment_period_seqs'\
                                                )
    return output_payment_final