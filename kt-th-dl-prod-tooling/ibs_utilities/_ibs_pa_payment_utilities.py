'''
job name     : IBS PA Payment Utilities module.
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
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.window import *
import ibs_utilities as ibs_utils


def pa_payment_aggregation(run_params, spark_session, start_date, end_date):
    print('--Reading data...')
    if(run_params.workflow_type == 'Daily') :
        df_ibs_lfkpa_lfppml_prep = spark_session.table(
        'processing_ibs.ibs_lfkpa_lfppml')\
            .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
            .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
            .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                    ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date)))
                   )\
            .drop('lstdate_cleaned', 'pdoi_cleaned')
    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_ibs_lfkpa_lfppml_prep = spark_session.table(
        'processing_ibs.ibs_lfkpa_lfppml')\
            .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
            .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
            .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                    ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date))) &
                    (F.col('pstu').isin('1'))
                   )\
            .drop('lstdate_cleaned', 'pdoi_cleaned')
    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_ibs_lfkpa_lfppml_prep = spark_session.table(
        'processing_ibs.ibs_lfkpa_lfppml')\
            .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
            .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
            .filter((((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                    ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date))) &
                    (~F.col('pstu').isin('1'))
                   )\
            .drop('lstdate_cleaned', 'pdoi_cleaned')



    df_ibs_lfkpa_dcrdetail_prep = spark_session.table('processing_ibs.ibs_lfkpa_dcrdetail')

    df_ibs_lfkpa_dcrdetail_prep = df_ibs_lfkpa_dcrdetail_prep.withColumnRenamed('lstdate', 'dcrdetail_lstdate')

    df_ibs_lfkpa_dcrdetail_prep = df_ibs_lfkpa_dcrdetail_prep \
        .join(df_ibs_lfkpa_lfppml_prep, df_ibs_lfkpa_lfppml_prep['pno']==df_ibs_lfkpa_dcrdetail_prep['dcpno'], 'left')\
        .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
        .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
        .filter(((F.col('lstdate_cleaned') >= start_date) & (F.col('lstdate_cleaned') < end_date)) |
                ((F.col('pdoi_cleaned') >= start_date) & (F.col('pdoi_cleaned') < end_date)))\
        .select(df_ibs_lfkpa_dcrdetail_prep['*'])

    col_ibs_lfkpa_lfppml = df_ibs_lfkpa_lfppml_prep.select(
        'pno', 'yearly', 'paymode', 'pmprem', 'ppytd')
    col_ibs_lfkpa_dcrdetail = df_ibs_lfkpa_dcrdetail_prep.select(
        'dcpno', 'rcno', 'paytype', 'dcdot', 'cbrdate', 'rcdate', 'paymod', 'dcrseq', 'revcode', 'dcryearly', 'amount')
    
    group_lfkpa_dcrdetail = col_ibs_lfkpa_dcrdetail.groupBy('dcpno', 'dcrseq')\
                            .agg(F.sum("amount").alias("sum_amount"))\
                            .filter("sum_amount > 0")

    group_lfkpa_dcrdetail = group_lfkpa_dcrdetail \
                                .withColumnRenamed('dcpno','group_dcpno')\
                                .withColumnRenamed('dcrseq','group_dcrseq')

    col_ibs_lfkpa_dcrdetail = group_lfkpa_dcrdetail.join(col_ibs_lfkpa_dcrdetail, 
                                       (group_lfkpa_dcrdetail['group_dcpno']==col_ibs_lfkpa_dcrdetail['dcpno']) &
                                       (group_lfkpa_dcrdetail['group_dcrseq']==col_ibs_lfkpa_dcrdetail['dcrseq']),
                                       'left').drop('group_dcpno','group_dcrseq')
    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = '0000-00-00T00:00:00+07:00'

    NON_REQ_NULL_STR = 'null'
    NON_REQ_NULL_NUM = 'null'
    NON_REQ_NULL_DATE = 'null'

    print('--Transforming data...')
    df_pa_payment = col_ibs_lfkpa_lfppml.join(col_ibs_lfkpa_dcrdetail,
                                             col_ibs_lfkpa_lfppml['pno']==col_ibs_lfkpa_dcrdetail['dcpno'],
                                             'left') \
                                        .withColumn('pmt_refer_code_of_company', concat(F.trim(F.col('pno')), lit('-01'), lit('_'), F.trim(F.col('dcrseq'))).cast(T.StringType())) \
                                        .withColumn('pmt_transaction_status', lit('N').cast(T.StringType())) \
                                        .withColumn('pmt_company_id', lit('1022').cast(T.StringType())) \
                                        .withColumn('pmt_pol_id',
                                                        F.when(F.col('pno').isNotNull(), F.col('pno').cast(T.StringType()))
                                                        .otherwise(lit(REQ_NULL_STR).cast(T.StringType()))) \
                                        .withColumn('pmt_pol_refer_code_of_company', concat(lit('POL_'), F.trim(F.col('pno'))).cast(T.StringType())) \
                                        .withColumn('pmt_id',
                                                        F.when(F.col('rcno').isNotNull(), F.col('rcno').cast(T.StringType()))
                                                        .otherwise(lit(REQ_NULL_STR).cast(T.StringType())))\
                                        .withColumn('pmt_type', lit('01').cast(T.StringType()))\
                                        .withColumn('pmt_direct_premium',
                                                    F.when(F.col('paytype')=='1', lit('01').cast(T.StringType()))
                                                    .when(F.col('paytype')!='1', lit('02').cast(T.StringType()))
                                                    .otherwise(lit(REQ_NULL_STR).cast(T.StringType()))
                                                   ) \
                                        .withColumn('pmt_premium_payment_period_year', lit(1).cast(T.IntegerType()).cast(T.StringType())) \
                                        .withColumn('pmt_premium_payment_year', lit(1).cast(T.IntegerType()).cast(T.StringType())) \
                                        .withColumn('pmt_payment_period', lit('04').cast(T.StringType())) \
                                        .withColumn('payment_period_seqs',
                                                    F.array(\
                                                           F.struct(\
                                                                    lit(1).cast(T.IntegerType()).cast(T.StringType()).alias('pmt_prd_premium_seq'),
                                                                    lit(0).cast(T.IntegerType()).cast(T.StringType()).alias('pmt_prd_premium_outstanding_payment'),
                                                                    F.when(F.col('pmprem').isNotNull(), F.col('pmprem').cast(T.DecimalType(18,2)).cast(T.StringType()))\
                                                                    .otherwise(lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).cast(T.StringType()))\
                                                                    .alias('pmt_prd_premium_amount'),
                                                                    lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).cast(T.StringType()).alias('pmt_prd_premium_amount_tax'),
                                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('pmt_prd_premium_amount_life'),
                                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('pmt_prd_premium_amt_saving'),
                                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('pmt_prd_premium_amt_investment'),
                                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('pmt_prd_premium_amount_other'),
                                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('pmt_prd_premium_amount_com'),
                                                                    lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('pmt_prd_premium_amt_interest'),
                                                                    F.when(F.col('dcdot').isNotNull(), F.from_unixtime(F.col('dcdot')/1000,"yyyy-MM-dd'T'HH:mm:ss'+07:00'"))\
                                                                    .otherwise(REQ_NULL_DATE)\
                                                                    .cast(T.StringType()).alias('pmt_prd_premium_date'),
                                                                    F.when(F.col('dcdot').isNotNull(), F.from_unixtime(F.col('ppytd')/1000,"yyyy-MM-dd'T'HH:mm:ss'+07:00'"))\
                                                                    .otherwise(lit(NON_REQ_NULL_DATE))\
                                                                    .cast(T.StringType()).alias('pmt_prd_premium_due_date'),
                                                                    F.when(F.col('dcdot').isNotNull(), F.from_unixtime(F.col('cbrdate')/1000,"yyyy-MM-dd'T'HH:mm:ss'+07:00'"))\
                                                                    .otherwise(lit(NON_REQ_NULL_DATE))\
                                                                    .cast(T.StringType()).alias('pmt_prd_premium_temp_receipt_date'),
                                                                    F.when(F.col('dcdot').isNotNull(), F.from_unixtime(F.col('rcdate')/1000,"yyyy-MM-dd'T'HH:mm:ss'+07:00'"))\
                                                                    .otherwise(REQ_NULL_DATE)\
                                                                    .cast(T.StringType()).alias('pmt_prd_premium_receipt_date'),
                                                                    F.when(F.col('rcno').isNotNull(), F.col('rcno').cast(T.StringType()))\
                                                                    .otherwise(lit(NON_REQ_NULL_DATE))\
                                                                    .cast(T.StringType()).alias('pmt_prd_premium_temp_receipt_number'),
                                                                    F.col('rcno').cast(T.StringType()).alias('pmt_prd_premium_receipt_number'),
                                                                    F.when(F.col('paymod')=='01', lit('01').cast(T.StringType()))
                                                                    .when(F.col('paymod')=='02', lit('06').cast(T.StringType()))
                                                                    .when((F.col('paymod')=='03')|(F.col('paymod')=='04'), lit('04').cast(T.StringType()))
                                                                    .when(F.col('paymod')=='05', lit('02').cast(T.StringType()))
                                                                    .otherwise(REQ_NULL_STR)\
                                                                    .cast(T.StringType()).alias('pmt_prd_premium_channel'),
                                                                    lit('02').cast(T.StringType()).alias('pmt_prd_premium_channel_detail'),
                                                                    F.array(\
                                                                       F.struct(\
                                                                                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('pmt_premium_rid_number'),
                                                                                lit(REQ_NULL_DEC).cast(T.StringType()).alias('pmt_premium_rid_amount'),
                                                                               )
                                                                       ).alias('payment_premium_type_riders'),
                                                                    F.array(\
                                                                       F.struct(\
                                                                                lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('pmt_premium_edm_number'),
                                                                                lit(REQ_NULL_DEC).cast(T.StringType()).alias('pmt_premium_edm_amount'),
                                                                               )
                                                                       ).alias('payment_premium_type_endorsements')
                                                                   )
                                                           )
                                                   )

    # df_pa_payment = ibs_utils.build_sequence_partition(
    #     params, df_pa_payment,
    #     params.args['RECORDS_AMOUNT_PERCENTAGE'],
    #     params.args['PARTITION_POL_ID_KEY'])

    # df_pa_payment = df_pa_payment.withColumn("id",F.sha2(concat(df_pa_payment['DataDate']\
    #                                                              ,df_pa_payment['TransactionDate']\
    #                                                              ,df_pa_payment['pmt_id']\
    #                                                              ,df_pa_payment['dcrseq']\
    #                                                              ,df_pa_payment['revcode']\
    #                                                              ,df_pa_payment['dcryearly']\
    #                                                              ),256))
    df_pa_payment = df_pa_payment.select(
        'pmt_refer_code_of_company', 'pmt_transaction_status',
        'pmt_company_id', 'pmt_pol_id', 'pmt_pol_refer_code_of_company',
        'pmt_id', 'pmt_type', 'pmt_direct_premium',
        'pmt_premium_payment_period_year', 'pmt_premium_payment_year',
        'pmt_payment_period', 'payment_period_seqs')
    return df_pa_payment
