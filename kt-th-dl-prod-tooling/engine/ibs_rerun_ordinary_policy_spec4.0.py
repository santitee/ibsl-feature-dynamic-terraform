'''
job name     : IBS Ordinary Policy Aggregation 4.0.
Created by   : Natchanon Thepyasuwan
Created date : 2 Jul 2025
version      : 4.0
'''

import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.functions import to_json, from_json
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_json, struct, length
from awsglue.dynamicframe import DynamicFrame
import json
import time

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

import ibs_utilities as ibs_utils
import ibs_ordinary_policy_utilities as ibs_ordinary_policy_utils

run_params = ibs_utils.get_run_parameters(sys.argv)

print('--Declaring Spark Session...')
sc = SparkContext()
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session

job = Job(glueContext)
job.init(run_params.args['JOB_NAME'], run_params.args)

origin_col_list = [
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
    # คีย์ใหม่ที่มีใน schema แต่ไม่มีใน origin_col_list เดิม
    'renewal', 'renewal_pol_id',
    'customer', 'insureds', 'sub_policies', 'beneficiaries',
    'endorsements', 'loans', 'loan_payments', 'investments',
    'TransactionDate'
]

    
def log_message(level, message):
    """
    Utility function for structured logging.
    Levels: INFO ✅, WARN ⚠️, ERROR ❌,
    """
    print(f"[{level}] {message}")


spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Paramaters
BUCKET_NAME = run_params.args['BUCKET_NAME']
TABLE_NAME = run_params.args['TABLE_NAME']
POSITION_DATE = run_params.position_date

# Output Path
OUTPUT_PATH = f"s3://{BUCKET_NAME}/data/spec4/aggregation/output/{TABLE_NAME}/"

log_message("INFO ✅", f"Using Output Path: {OUTPUT_PATH}")    
log_message("INFO ✅", f"Workflow Type: {run_params.workflow_type}")    

try:
    if run_params.workflow_type == 'Daily':
        print('Define Daily Start date - End date')
        start_date = run_params.position_year + run_params.position_month + run_params.position_day
        end_date = run_params.system_date
        end_date = end_date.replace("-", "")
 
        log_message("INFO ✅", f"Running Aggregation from {start_date} to {end_date}")    
        
        prev_df = spark_session.table('processing_ibs.ibs_ordinary_policy')
        df_error_all = spark_session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun2/*.csv")
        unique_refs = df_error_all.select('รหัสอ้างอิง').distinct() \
            .withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        policy_df = prev_df
        rerun_df = policy_df.join(
            unique_refs,
            policy_df['pol_refer_code_of_company'] == unique_refs['error_refs'],
            how='inner'
        ).drop(unique_refs['error_refs']) 
    
        new_df = rerun_df
    
        output_policy_df_final = ibs_utils.cdc(prev_df, new_df, 'pol_id', 'pol_refer_code_of_company', 'pol_transaction_status')
        
    elif run_params.workflow_type in ['One-Time-Inforce', 'One-Time-Non-Inforce']:
        print('Define One-time Start date - End date')
        # prev_df = spark_session.table('processing_ibs.ibs_ordinary_policy').filter(F.col('TransactionDate')=='2025-09-05')
        # print(f'prev_df: {prev_df.count()}')
        start_date = run_params.workflow_onetime_start_date.replace("-", "")
        end_date = run_params.workflow_onetime_end_date.replace("-", "")
        log_message("INFO ✅", f"Running Aggregation from {start_date} to {end_date}")     
        
        error_batch_all = spark_session.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun3/IBS_Fail_Report_251122_Part1_of_1.csv")
        error_batch_all = error_batch_all.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        print(f'print error_batch_all count: {error_batch_all.count()}')
        df = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, start_date, end_date)
        rerun_df = df.join(error_batch_all, df['pol_refer_code_of_company'] == error_batch_all['error_refs'], how='inner') \
                    .drop(error_batch_all['error_refs'])
        print(f'print rerun_df count: {rerun_df.count()}')
        
        # error_batch1 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250806_Part1_of_1.csv")
        # error_batch2 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250810_Part1_of_1.csv")
        # error_batch2 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250816_Part1_of_1.csv")
        # error_batch3 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250817_Part1_of_1.csv")
        # error_batch4 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250820_Part1_of_1.csv")
        # error_batch5 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250821_Part1_of_1.csv")
        # error_batch6 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250823_Part1_of_1.csv")
        # error_batch7 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250824_Part1_of_1.csv")
        # error_batch8 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250829_Part1_of_1.csv")
        # error_batch9 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250826_Part1_of_1.csv")
        # error_batch10 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250827_Part1_of_1.csv")
        # error_batch11 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250830_Part1_of_1.csv")
        # error_batch12 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250901_Part1_of_1.csv")
        # error_batch13 = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_250902_Part1_of_1.csv")

        
        # unique_refs1 = error_batch1.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs2 = error_batch2.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs3 = error_batch3.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs4 = error_batch4.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs5 = error_batch5.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs6 = error_batch6.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs7 = error_batch7.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs8 = error_batch8.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs9 = error_batch9.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs10 = error_batch10.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs11 = error_batch11.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs12 = error_batch12.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        # unique_refs13 = error_batch13.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        
        # policy_df_batch1 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '19980101', '20130101')
        # policy_df_batch2 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20130101', '20150101')
        # policy_df_batch3 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20150101', '20160901')
        # policy_df_batch4 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20160901', '20180301')
        # policy_df_batch5 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20180301', '20190901')
        # policy_df_batch6 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20190901', '20210201')
        # policy_df_batch7 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20210201', '20211214')
        # policy_df_batch8 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20211214', '20221007')
        # policy_df_batch9 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20221007', '20230715')
        # policy_df_batch10 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20230715', '20240410')
        # policy_df_batch11 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20240410', '20241122')
        # policy_df_batch12 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20241122', '20250522')
        # policy_df_batch13 = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, '20250522', '20250831')
        
        # rerun_batch1 = policy_df_batch1.join(unique_refs1,policy_df_batch1['pol_refer_code_of_company'] == unique_refs1['error_refs'],how='inner').drop(unique_refs1['error_refs'])
        # rerun_batch2 = policy_df_batch2.join(unique_refs2,policy_df_batch2['pol_refer_code_of_company'] == unique_refs2['error_refs'],how='inner').drop(unique_refs2['error_refs'])
        # rerun_batch3 = policy_df_batch3.join(unique_refs3,policy_df_batch3['pol_refer_code_of_company'] == unique_refs3['error_refs'],how='inner').drop(unique_refs3['error_refs'])
        # rerun_batch4 = policy_df_batch4.join(unique_refs4,policy_df_batch4['pol_refer_code_of_company'] == unique_refs4['error_refs'],how='inner').drop(unique_refs4['error_refs']) 
        # rerun_batch5 = policy_df_batch5.join(unique_refs5,policy_df_batch5['pol_refer_code_of_company'] == unique_refs5['error_refs'],how='inner').drop(unique_refs5['error_refs']) 
        # rerun_batch6 = policy_df_batch6.join(unique_refs6,policy_df_batch6['pol_refer_code_of_company'] == unique_refs6['error_refs'],how='inner').drop(unique_refs6['error_refs']) 
        # rerun_batch7 = policy_df_batch7.join(unique_refs7,policy_df_batch7['pol_refer_code_of_company'] == unique_refs7['error_refs'],how='inner').drop(unique_refs7['error_refs']) 
        # rerun_batch8 = policy_df_batch8.join(unique_refs8,policy_df_batch8['pol_refer_code_of_company'] == unique_refs8['error_refs'],how='inner').drop(unique_refs8['error_refs']) 
        # rerun_batch9 = policy_df_batch9.join(unique_refs9,policy_df_batch9['pol_refer_code_of_company'] == unique_refs9['error_refs'],how='inner').drop(unique_refs9['error_refs']) 
        # rerun_batch10 = policy_df_batch10.join(unique_refs10,policy_df_batch10['pol_refer_code_of_company'] == unique_refs10['error_refs'],how='inner').drop(unique_refs10['error_refs']) 
        # rerun_batch11 = policy_df_batch11.join(unique_refs11,policy_df_batch11['pol_refer_code_of_company'] == unique_refs11['error_refs'],how='inner').drop(unique_refs11['error_refs']) 
        # rerun_batch12 = policy_df_batch12.join(unique_refs12,policy_df_batch12['pol_refer_code_of_company'] == unique_refs12['error_refs'],how='inner').drop(unique_refs12['error_refs']) 
        # rerun_batch13 = policy_df_batch13.join(unique_refs13,policy_df_batch13['pol_refer_code_of_company'] == unique_refs13['error_refs'],how='inner').drop(unique_refs13['error_refs']) 
        
        # print(f'error_batch1_count: {rerun_batch1.count()}')
        # print(f'error_batch2_count: {rerun_batch2.count()}')
        # print(f'error_batch3_count: {rerun_batch3.count()}')
        # print(f'error_batch4_count: {rerun_batch4.count()}')
        # print(f'error_batch5_count: {rerun_batch5.count()}')
        # print(f'error_batch6_count: {rerun_batch6.count()}')
        # print(f'error_batch7_count: {rerun_batch7.count()}')
        # print(f'error_batch8_count: {rerun_batch8.count()}')
        # print(f'error_batch9_count: {rerun_batch9.count()}')
        # print(f'error_batch10_count: {rerun_batch10.count()}')
        # print(f'error_batch11_count: {rerun_batch11.count()}')
        # print(f'error_batch12_count: {rerun_batch12.count()}')
        # print(f'error_batch13_count: {rerun_batch13.count()}')
    
        # print('finish prepare rerun batch...')
        # print('Union process')
        # # prev_df = prev_df.drop('TransactionDate')
        # # rerun_df = prev_df \
        # # .unionByName(rerun_batch2)
        
        # rerun_df = rerun_batch1 \
        # .unionByName(rerun_batch2) \
        # .unionByName(rerun_batch3) \
        # .unionByName(rerun_batch4) \
        # .unionByName(rerun_batch5) \
        # .unionByName(rerun_batch6) \
        # .unionByName(rerun_batch7) \
        # .unionByName(rerun_batch8) \
        # .unionByName(rerun_batch9) \
        # .unionByName(rerun_batch10) \
        # .unionByName(rerun_batch11) \
        # .unionByName(rerun_batch12) \
        # .unionByName(rerun_batch13) 
        
        # # รายชื่อไฟล์ CSV และช่วงเวลา policy
        # error_files = [
        #     "250806", "250816", "250817", "250820", "250821", "250823", "250824",
        #     "250826", "250827", "250829", "250830", "250901", "250902"
        # ]
        
        # policy_ranges = [
        #     ("19980101", "20130101"), ("20130101", "20150101"), ("20150101", "20160901"),
        #     ("20160901", "20180301"), ("20180301", "20190901"), ("20190901", "20210201"),
        #     ("20210201", "20211214"), ("20211214", "20221007"), ("20221007", "20230715"),
        #     ("20230715", "20240410"), ("20240410", "20241122"), ("20241122", "20250522"),
        #     ("20250522", "20250831")
        # ]
        
        # # # โหลด error batches และ extract รหัสอ้างอิง
        # unique_refs_list = []
        # for date_code in error_files:
        #     path = f"s3://kt-th-dl-prod-ibs/data/spec4/error_rerun/IBS_Fail_Report_{date_code}_Part1_of_1.csv"
        #     df = spark_session.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv(path)
        #     unique_refs = df.select('รหัสอ้างอิง').distinct().withColumnRenamed('รหัสอ้างอิง', 'error_refs')
        #     unique_refs_list.append(unique_refs)
        
        # # โหลด policy batches
        # policy_df_list = []
        # for start_date, end_date in policy_ranges:
        #     df = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, start_date, end_date)
        #     policy_df_list.append(df)
        
        # # Join แต่ละ policy กับ error refs ที่ตรงกัน
        # rerun_df_list = []
        # for policy_df, unique_refs in zip(policy_df_list, unique_refs_list):
        #     joined_df = policy_df.join(unique_refs, policy_df['pol_refer_code_of_company'] == unique_refs['error_refs'], how='inner') \
        #                          .drop(unique_refs['error_refs'])
        #     rerun_df_list.append(joined_df)
            
        # # Union ทุก rerun_df เข้าด้วยกัน
        # final_rerun_df = reduce(DataFrame.unionByName, rerun_df_list)

        
        output_policy_df_final = rerun_df
    output_policy_df_final = output_policy_df_final \
    .withColumn('TransactionDate', lit(POSITION_DATE).cast(T.StringType()))\
    .withColumn('pol_transaction_status', lit('').cast(T.StringType()))
    
    output_policy_df_final = output_policy_df_final.select(origin_col_list)
    
    df_policy_union = output_policy_df_final.withColumn("id", 
    F.sha2(concat(output_policy_df_final['TransactionDate'],output_policy_df_final['pol_refer_code_of_company']),256))
    
    df_with_sized = df_policy_union \
    .withColumn('json_str', F.to_json(F.struct(*[F.col(c) for c in df.columns])))\
    .withColumn('json_size', F.length(F.encode(F.col('json_str'), 'UTF-8')))\
    .drop('json_str')
    
    df_ok = df_with_sized.filter(F.col('json_size') <= 390*1024)
    df_ok = df_ok.drop('json_size')
    print(f'df with sized 400KB dynamoDB allowed (390KB buffered) count : {df_ok.count()}')
    df_ok = df_ok.repartition(1)
    
    print("--Writing df to DynamoDB")
    df_ok = DynamicFrame.fromDF(df_ok, glueContext, "test")
    
    glueContext.write_dynamic_frame_from_options(
        frame=df_ok,
        connection_type="dynamodb",
        connection_options={
            "dynamodb.region": "ap-southeast-1",
            "dynamodb.output.tableName": "w-kt-th-ibs-pr-as1-policy-data",
            "dynamodb.sts.roleArn": "arn:aws:iam::906981732340:role/w-kt-th-ibs-pr-as1-dynamodb-role01",
            "compression": "lzo",
            "dynamodb.throughput.write.percent": "1.0"
        }
    )
    print("--Wrote data to dynamoDB successfully!")
    
    print("--Writing df to s3")

    df_policy_union.repartition(1).write.format("json") \
    .option("ignoreNullField", "false") \
    .mode("append") \
    .partitionBy("TransactionDate") \
    .save(f's3://{BUCKET_NAME}/data/spec4/export/ibs_policy_final_output/')
    
    print("--Wrote data to S3 successfully!")

    
except Exception as e:
    log_message("ERROR ❌", f"aggregation failed: {str(e)}")
    raise
    
recort_count = output_policy_df_final.count()
log_message("INFO ✅", f"Total records processed: {recort_count}")

if recort_count == 0:
    log_message("WARN ⚠️", f"No records to process for TransactionDate = {POSITION_DATE}. Skipping S3 write.")
else:
    start_time = time.time()
    try:
        output_policy_df_final.write.format("json") \
        .option("ignoreNullField", "false") \
        .mode("append") \
        .partitionBy("TransactionDate") \
        .save(OUTPUT_PATH)
    
        log_message("INFO ✅", f"Successfully wrote {recort_count} records with partition by TransactionDate to {OUTPUT_PATH}TransactionDate={POSITION_DATE} in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        log_message("ERROR ❌", f"Failed to write output to S3: {str(e)}")
        raise


print('Aggregation was successful, commiting now.')
job.commit()
