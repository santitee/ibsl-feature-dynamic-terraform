'''
job name     : IBS Ordinary Payment Aggregation.
Created by   : Kiatipoom Poopanitpun
Created date : 6 Mar 2022
version      : 0.1
'''

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.window import *
import json
import boto3

import ibs_utilities as ibs_utils
import ibs_ordinary_payment_utilities as ibs_ordinary_payment_utils

run_params = ibs_utils.get_run_parameters(sys.argv)

print('--Declaring Spark Session...')
sc = SparkContext()
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session
# spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# spark_session.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")

job = Job(glueContext)
job.init(run_params.args['JOB_NAME'], run_params.args)

origin_col_list = ['pmt_refer_code_of_company','pmt_transaction_status','pmt_company_id'
                    ,'pmt_pol_id','pmt_pol_refer_code_of_company','pmt_id'
                    ,'pmt_type','pmt_direct_premium','pmt_premium_payment_period_year'
                    ,'pmt_premium_payment_year','pmt_payment_period','payment_period_seqs'
                    ,'DataDate','TransactionDate','PartitionNo']

s3_bucket = run_params.args['BUCKET_NAME']

print('--Checking Workflow Type...')
if run_params.workflow_type in ['One-Time-Inforce', 'One-Time-Non-Inforce']:
    print('Define One-time Start date - End date')
    start_date = run_params.workflow_onetime_start_date.replace("-", "")
    end_date = run_params.workflow_onetime_end_date.replace("-", "")

    print('--Starting Ordinary Payment Aggregation...')
    output_payment_df_final = ibs_ordinary_payment_utils.ordinary_payment_aggregation(
        run_params, spark_session, start_date, end_date)
    output_payment_df_final = ibs_utils.build_sequence_partition(
        run_params, output_payment_df_final,
        run_params.args['RECORDS_AMOUNT_PERCENTAGE'],
        run_params.args['PARTITION_POL_ID_KEY'])
elif run_params.workflow_type == 'Daily':
    print("Remove Missing Partitions")
    # Specify the S3 bucket and prefix where the partitioned data is located
    s3 = boto3.client("s3")
    s3_prefix = "data/aggregation/output/ibs_ordinary_payment/"
    database = "processing_ibs"
    table = "ibs_ordinary_payment"
    
    # Extract the partitions
    df = spark_session.sql(f"SHOW PARTITIONS {database}.{table}")
    partitions = df.rdd.map(lambda x: x[0]).collect()
    partitions_to_drop = []
    
    for partition in partitions:
        partition_list = partition.split("/")
        transactiondate, partitionno = partition_list[0].split("=")[1], partition_list[1].split("=")[1]
        try:
            df_parition_check = spark_session.sql(f"SELECT * FROM {database}.{table} WHERE transactiondate='{transactiondate}' AND partitionno='{partitionno}' LIMIT 1")
            if df_parition_check.rdd.isEmpty():
                partitions_to_drop.append((transactiondate, partitionno))
        except Exception as e:
            print(f"Partition not found for transactiondate='{transactiondate}', partitionno='{partitionno}': {e}")
            partitions_to_drop.append((transactiondate, partitionno))
            
    for transactiondate, partitionno in partitions_to_drop:
        print(f"Droping Missing s3 partiion: transactiondate='{transactiondate}', partitionno='{partitionno}'")
        spark_session.sql(f"ALTER TABLE {database}.{table} DROP IF EXISTS PARTITION (transactiondate='{transactiondate}', partitionno='{partitionno}')")
        
    print('Done processing remove missing parition.')
    
    print('Detect failed new status method process')
    fail_new_txn_df = ibs_utils.detect_err_new_status('payment')
    
    df_pol = fail_new_txn_df.select("TransactionDate","PartitionNo",F.posexplode("list_pol_id").alias("index","pmt_pol_id"))
    df_ref = fail_new_txn_df.select("TransactionDate","PartitionNo",F.posexplode("list_ref_code_of_company").alias("index","pmt_refer_code_of_company"))
    df_to_remove = df_pol.join(df_ref, on=["TransactionDate","PartitionNo","index"])
    print(f"--Debug df_to_remove on TransactionDate = '{run_params.position_date}'")
    df_to_remove.select("*").filter(f"TransactionDate = '{run_params.position_date}'").show()

    # df_origin_table = spark_session.table('processing_ibs.ibs_ordinary_payment')
    df_origin_table = spark_session.read.json(f"s3://{s3_bucket}/data/aggregation/output/ibs_ordinary_payment")
    df_origin_table = df_origin_table \
        .withColumn('TransactionDate', lit(F.col('TransactionDate')).cast(T.StringType()))\
        .withColumn('PartitionNo', lit(F.col('PartitionNo')).cast(T.StringType()))
    df_origin_table = df_origin_table.select(origin_col_list)
    df_overwrite = df_origin_table
    df_overwrite = df_origin_table.join(
                                    df_to_remove,
                                    on=["TransactionDate", "PartitionNo", "pmt_pol_id", "pmt_refer_code_of_company"],
                                    how="left_anti")
                                    
    df_overwrite = df_overwrite.select(origin_col_list)

    if df_overwrite.count() == 0:
        print("DataFrame is empty, skipping write")
    else:
        print(f"Filtering dataframe for removing failed new transaction status...")
        df_overwrite.coalesce(1).write.format('json') \
            .option("ignoreNullFields", "false") \
            .mode('overwrite')\
            .partitionBy("TransactionDate", "PartitionNo")\
            .save(f's3://{s3_bucket}/data/aggregation/filtered/ibs_ordinary_payment')
            
        filtered_output = spark_session.read.json(f"s3://{s3_bucket}/data/aggregation/filtered/ibs_ordinary_payment")
        filtered_output = filtered_output \
            .withColumn('TransactionDate', lit(F.col('TransactionDate')).cast(T.StringType()))\
            .withColumn('PartitionNo', lit(F.col('PartitionNo')).cast(T.StringType()))
        filtered_output = filtered_output.select(origin_col_list)
        
        filtered_output.coalesce(1).write.format('json')\
            .option("ignoreNullFields", "false")\
            .mode('overwrite')\
            .partitionBy("TransactionDate", "PartitionNo")\
            .save(f's3://{s3_bucket}/data/aggregation/output/ibs_ordinary_payment')
            
        print(f"Writing filtered data Successful!")
        print('----------------------------------------------------------')

    print('Define Daily Start date - End date')
    if (run_params.workflow_onetime_start_date == '-') and (run_params.workflow_onetime_end_date == '-') :
        start_date = run_params.position_year + run_params.position_month + run_params.position_day
        end_date = run_params.system_date
        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "") 
    else :
        start_date = run_params.workflow_onetime_start_date.replace("-", "")
        end_date = run_params.workflow_onetime_end_date.replace("-", "")  
    print(f"start date : {start_date}")
    print(f"end date : {end_date}")   

    print('--Starting Ordinary Payment Aggregation...')
    output_payment_df_final = ibs_ordinary_payment_utils.ordinary_payment_aggregation(
        run_params, spark_session, start_date, end_date)
    output_payment_df_final = ibs_utils.build_sequence_partition(
        run_params, output_payment_df_final,
        run_params.args['RECORDS_AMOUNT_PERCENTAGE'],
        run_params.args['PARTITION_POL_ID_KEY'])

    print('--Starting Change Data Capture Process...')
    print('--Get the previous data')
    prev_df = filtered_output
    prev_df = prev_df \
                .withColumn('TransactionDate', lit(F.col('TransactionDate')).cast(T.StringType()))\
                .withColumn('PartitionNo', lit(F.col('PartitionNo')).cast(T.StringType()))
    print('--Get the new data')
    new_df = output_payment_df_final
    print('--Debug new_df:')
    new_df.select('pmt_pol_id', 'pmt_refer_code_of_company','DataDate', 'TransactionDate', 'PartitionNo')
    print('--Debug old_df:')
    prev_df.select('pmt_pol_id', 'pmt_refer_code_of_company','DataDate', 'TransactionDate', 'PartitionNo')

    print('--Calling CDC Method...')
    output_payment_df_final = ibs_utils.cdc(prev_df, new_df, 'pmt_pol_id', 'pmt_refer_code_of_company', 'pmt_transaction_status')
    output_payment_df_final.select('pmt_pol_id', 'pmt_refer_code_of_company', 'pmt_transaction_status','DataDate', 'TransactionDate', 'PartitionNo')

print('--Writing Output Ordinary payment to S3...')
# output_payment_df_final = output_payment_df_final.withColumn("id",F.sha2(concat(output_payment_df_final['DataDate']\
#                                                               ,output_payment_df_final['TransactionDate']\
#                                                               ,output_payment_df_final['pmt_id']\
#                                                               ,output_payment_df_final['payment_period_seqs'].cast(T.StringType())\
#                                                               ),256))
                                                              
output_payment_df_final = output_payment_df_final.select(origin_col_list)

# spark_session.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# spark_session.conf.set("spark.sql.broadcastTimeout", "3600")
# def read_json_with_retries(spark_session, s3_path, max_retries=5):
#     retries = 0
#     while retries < max_retries:
#         try:
#             df = spark_session.read.json(s3_path)
#             return df
#         except AnalysisException as e:
#             if 'Slow Down' in str(e):
#                 retries += 1
#                 wait_time = 2 ** retries  # Exponential backoff
#                 print(f"Slow Down error encountered. Retrying in {wait_time} seconds...")
#                 time.sleep(wait_time)
#             else:
#                 raise e
#     raise Exception("Max retries exceeded")
    
if(output_payment_df_final.rdd.isEmpty()):
    print(f"There is no data for Ordinary Payment on TransactionDate = {run_params.position_date}!!!!!")
else:
    output_payment_df_final.coalesce(1).write.format('json')\
        .option("ignoreNullFields", "false")\
        .mode('overwrite')\
        .partitionBy("PartitionNo")\
        .save(f's3://{s3_bucket}/data/aggregation/staging/ibs_ordinary_payment/TransactionDate={run_params.position_date}')
    
    # staging_output = spark_session.read.json(f"s3://{s3_bucket}/data/aggregation/staging/ibs_ordinary_payment")
    staging_output = spark_session.read.json(f"s3://{s3_bucket}/data/aggregation/staging/ibs_ordinary_payment/TransactionDate={run_params.position_date}")
    staging_output = staging_output.withColumn('PartitionNo', lit(F.col('PartitionNo')).cast(T.StringType()))
    staging_output = staging_output.select(origin_col_list).filter(f"TransactionDate == '{run_params.position_date}'")
    print('--Debug staging_output:')
    staging_output.select('pmt_pol_id', 'pmt_refer_code_of_company', 'pmt_transaction_status','DataDate', 'TransactionDate', 'PartitionNo')
    
    print('--Writing staging data to final destination...')
    staging_output.drop('TransactionDate').coalesce(1).write.format('json')\
        .option("ignoreNullFields", "false")\
        .mode('overwrite')\
        .partitionBy("PartitionNo")\
        .save(f's3://{s3_bucket}/data/aggregation/output/ibs_ordinary_payment/TransactionDate={run_params.position_date}')
    
print('Aggregation was successful, commiting now.')
job.commit()
