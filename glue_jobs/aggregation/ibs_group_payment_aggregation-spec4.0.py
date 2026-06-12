'''
job name     : IBS Grpou Payment Aggregation 4.0.
Created by   : Tara Thummanuthum
Created date : 27 FEB 2026
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
import json
import time

import ibs_utilities as ibs_utils
import ibs_group_payment_utilities as ibs_group_payment_utils

run_params = ibs_utils.get_run_parameters(sys.argv)

print('--Declaring Spark Session...')
sc = SparkContext()
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session

# ---- Spark Conf: ป้องกัน broadcast timeout / ใช้ AQE / กันพัง parser เวลาจัดการเวลา ----
spark_session.conf.set("spark.sql.broadcastTimeout", "1800")             # เพิ่มเวลา broadcast เป็น 30 นาที
spark_session.conf.set("spark.sql.adaptive.enabled", "true")             # เปิด AQE
spark_session.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark_session.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ปิด auto-broadcast join ทั้งระบบ (เราจะ broadcast เฉพาะตารางเล็กด้วยมือเท่านั้น)
spark_session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

job = Job(glueContext)
job.init(run_params.args['JOB_NAME'], run_params.args)
                    
origin_col_list = [
    'pmt_refer_code_of_company', 'pmt_transaction_status', 'pmt_company_id',
    'pmt_pol_id', 'pmt_pol_refer_code_of_company', 'pmt_id', 'pmt_type',
    'pmt_direct_premium', 'pmt_premium_payment_period_year', 'pmt_premium_payment_year',
    'pmt_payment_period', 'payment_period_seqs','TransactionDate','index'
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

# Add Index
INDEX = run_params.index
INDEX_STR = str(INDEX)
print("INDEX_STR : ", INDEX_STR)

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
        
        prev_df = spark_session.table('processing_ibs.ibs_group_payment')
    
        new_df = ibs_group_payment_utils.group_payment_aggregation(run_params, spark_session, start_date, end_date)
    
        output_payment_df_final = ibs_utils.cdc(prev_df, new_df, 'pmt_pol_id', 'pmt_refer_code_of_company','pmt_transaction_status')
    elif run_params.workflow_type in ['One-Time-Inforce', 'One-Time-Non-Inforce']:
        print('Define One-time Start date - End date')
        start_date = run_params.workflow_onetime_start_date.replace("-", "")
        end_date = run_params.workflow_onetime_end_date.replace("-", "")
        log_message("INFO ✅", f"Running Aggregation from {start_date} to {end_date}")     
        
        new_df = ibs_group_payment_utils.group_payment_aggregation(run_params, spark_session, start_date, end_date)
        output_payment_df_final = new_df
    output_payment_df_final = output_payment_df_final \
        .withColumn('TransactionDate', lit(POSITION_DATE).cast(T.StringType()))\
        .withColumn('pmt_transaction_status', lit('').cast(T.StringType()))\
        .withColumn('index', lit(INDEX_STR).cast(T.StringType()))
    
    output_payment_df_final = output_payment_df_final.select(origin_col_list)
except Exception as e:
    log_message("ERROR ❌", f"aggregation failed: {str(e)}")
    raise
    
recort_count = output_payment_df_final.count()
log_message("INFO ✅", f"Total records processed: {recort_count}")

if recort_count == 0:
    log_message("WARN ⚠️", f"No records to process for TransactionDate = {POSITION_DATE}. Skipping S3 write.")
else:
    start_time = time.time()
    try:
        output_payment_df_final.write.format("json") \
        .option("ignoreNullFields", "false") \
        .mode("append") \
        .partitionBy("TransactionDate") \
        .save(OUTPUT_PATH)
    
        log_message("INFO ✅", f"Successfully wrote {recort_count} records with partition by TransactionDate to {OUTPUT_PATH}/TransactionDate={POSITION_DATE} in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        log_message("ERROR ❌", f"Failed to write output to S3: {str(e)}")
        raise


print('Aggregation was successful, commiting now.')
job.commit()
