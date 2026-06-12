'''
job name     : IBS Ordinary Policy Aggregation 4.0.
Created by   : Natchanon Thepyasuwan
Created date : 2 Jul 2025
version      : 4.0
Modified by  : Santi Teeragul
Last update  : 23 Dec 2025
Modified by  : Santi Teeragul
Last update  : 23 Mar 2026
'''
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
    'TransactionDate','index'
]

def log_message(level, message):
    """
    Utility function for structured logging.
    Levels: INFO ✅, WARN ⚠️, ERROR ❌,
    """
    print(f"[{level}] {message}")

spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Parameters
BUCKET_NAME = run_params.args['BUCKET_NAME']
TABLE_NAME = run_params.args['TABLE_NAME']
POSITION_DATE = run_params.position_date

# Santi Teeragul: Add Index
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
        # start_date = run_params.position_year + run_params.position_month + run_params.position_day (Unused)
        
        # Santi Teeragul: Modified to get last successful run date from start_date_ref_table
        # For Daily, start_date is get last successful run date form start_date_ref_table
        # start_date_ref_table = s3://kt-th-dl-prod-ibs/data/spec4/aggregation/start_date_ref_table/
        start_date_ref_schema = T.StructType([
            T.StructField("workflow_run_date", T.StringType(), True),
            T.StructField("policy_transaction_date", T.StringType(), True),
        ])

        df_start_date_ref_table = spark_session.read.csv("s3a://kt-th-dl-prod-ibs/data/spec4/aggregation/start_date_ref_table/*.csv",header=False,schema=start_date_ref_schema) 
        #df_start_date_ref_table.show()

        # get policy_transaction_date
        df_policy = df_start_date_ref_table.select("policy_transaction_date")
        #df_policy.show(10, truncate=False)
        
        # TODO: Change start_date and end_date

        start_date = df_start_date_ref_table.agg(F.max("policy_transaction_date")).collect()[0][0]
        # print("Latest policy_transaction_date:", start_date)
        # print("Start Date : ", start_date)
        end_date = run_params.system_date
        end_date = end_date.replace("-", "")
        
        # start_date = '20251204'
        # end_date = '20251219'
        
        print("Start Date : ", start_date)
        print("End Date : ", end_date)
 
        log_message("INFO ✅", f"Running Aggregation from {start_date} to {end_date}")    
        
        prev_df = spark_session.table('processing_ibs.ibs_ordinary_policy')
    
        new_df = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, start_date, end_date)
    
        output_policy_df_final = ibs_utils.cdc(prev_df, new_df, 'pol_id', 'pol_refer_code_of_company', 'pol_transaction_status')
        
    elif run_params.workflow_type in ['One-Time-Inforce', 'One-Time-Non-Inforce']:
        print('Define One-time Start date - End date')
        start_date = run_params.workflow_onetime_start_date.replace("-", "")
        end_date = run_params.workflow_onetime_end_date.replace("-", "")
        log_message("INFO ✅", f"Running Aggregation from {start_date} to {end_date}")     
        
        new_df = ibs_ordinary_policy_utils.ordinary_policy_aggregation(run_params, spark_session, start_date, end_date)
        output_policy_df_final = new_df
    output_policy_df_final = output_policy_df_final \
    .withColumn('TransactionDate', lit(run_params.system_date).cast(T.StringType()))\
    .withColumn('pol_transaction_status', lit('').cast(T.StringType()))\
    .withColumn('index', lit(INDEX_STR).cast(T.StringType()))
    
    output_policy_df_final = output_policy_df_final.select(origin_col_list)
    output_policy_df_final.cache()
except Exception as e:
    log_message("ERROR ❌", f"aggregation failed: {str(e)}")
    raise
output_policy_df_final = output_policy_df_final.cache()    
record_count = output_policy_df_final.count()
log_message("INFO ✅", f"Total records processed: {record_count}")

if record_count == 0:
    log_message("WARN ⚠️", f"No records to process for TransactionDate = {POSITION_DATE}. Skipping S3 write.")
else:
    start_time = time.time()
    try:
        output_policy_df_final.write.format("json") \
        .option("ignoreNullField", "false") \
        .mode("append") \
        .partitionBy("TransactionDate") \
        .save(OUTPUT_PATH)
    
        log_message("INFO ✅", f"Successfully wrote {record_count} records with partition by TransactionDate to {OUTPUT_PATH}/TransactionDate={POSITION_DATE} in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        log_message("ERROR ❌", f"Failed to write output to S3: {str(e)}")
        raise


print('Aggregation was successful, commiting now.')
job.commit()
'''