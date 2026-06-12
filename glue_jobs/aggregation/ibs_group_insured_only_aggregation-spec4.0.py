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
import ibs_grp_std_insured_only_utilities as ibs_grp_std_insured_utils

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

# Output column list based on OIC JSON spec
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
    'renewal', 'renewal_pol_id',
    'customer', 'insureds', 'sub_policies', 'beneficiaries',
    'endorsements', 'loans', 'loan_payments', 'investments',
    'TransactionDate', 'index'
]


def log_message(level, message):
    """
    Utility function for structured logging.
    Levels: INFO, WARN, ERROR
    """
    print(f"[{level}] {message}")


spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Parameters
BUCKET_NAME = run_params.args['BUCKET_NAME']
TABLE_NAME = run_params.args['TABLE_NAME']
POSITION_DATE = run_params.position_date

#1 Santi Teeragul: Add Index
INDEX = run_params.index
INDEX_STR = str(INDEX)
print("INDEX_STR : ", INDEX_STR)

# Output Path
OUTPUT_PATH = f"s3://{BUCKET_NAME}/data/spec4/aggregation/output/{TABLE_NAME}/"

log_message("INFO", f"=== IBS Group Standard Insured Only Aggregation ===")
log_message("INFO", f"Using Output Path: {OUTPUT_PATH}")
log_message("INFO", f"Workflow Type: {run_params.workflow_type}")
log_message("INFO", f"Position Date: {POSITION_DATE}")

try:
    if run_params.workflow_type == 'Daily':
        print('Define Daily Start date - End date')
        start_date = run_params.position_year + run_params.position_month + run_params.position_day
        end_date = run_params.system_date
        end_date = end_date.replace("-", "")

        log_message("INFO", f"Running Aggregation from {start_date} to {end_date}")

        prev_df = spark_session.table('processing_ibs.ibs_group_policy')

        new_df = ibs_grp_std_insured_utils.grp_std_insured_only_aggregation(run_params, spark_session, start_date, end_date)

        output_policy_df_final = ibs_utils.cdc(prev_df, new_df, 'pol_id', 'pol_refer_code_of_company', 'pol_transaction_status')

    elif run_params.workflow_type in ['One-Time-Inforce', 'One-Time-Non-Inforce']:
        print('Define One-time Start date - End date')
        start_date = run_params.workflow_onetime_start_date.replace("-", "")
        end_date = run_params.workflow_onetime_end_date.replace("-", "")
        log_message("INFO", f"Running Aggregation from {start_date} to {end_date}")

        new_df = ibs_grp_std_insured_utils.grp_std_insured_only_aggregation(run_params, spark_session, start_date, end_date)
        output_policy_df_final = new_df
    output_policy_df_final = output_policy_df_final \
        .withColumn('TransactionDate', lit(POSITION_DATE).cast(T.StringType())) \
        .withColumn('index', lit(INDEX_STR).cast(T.StringType()))

    output_policy_df_final = output_policy_df_final.select(origin_col_list)

except Exception as e:
    log_message("ERROR", f"Aggregation failed: {str(e)}")
    raise

record_count = output_policy_df_final.count()
log_message("INFO", f"Total records processed: {record_count}")

if record_count == 0:
    log_message("WARN", f"No records to process for TransactionDate = {POSITION_DATE}. Skipping S3 write.")
else:
    start_time = time.time()
    try:
        output_policy_df_final.write.format("json") \
            .option("ignoreNullFields", "false") \
            .mode("append") \
            .partitionBy("TransactionDate") \
            .save(OUTPUT_PATH)

        log_message("INFO", f"Successfully wrote {record_count} records with partition by TransactionDate to {OUTPUT_PATH}/TransactionDate={POSITION_DATE} in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        log_message("ERROR", f"Failed to write output to S3: {str(e)}")
        raise

print('=== Group Standard Insured Only Aggregation was successful, committing now. ===')
job.commit()