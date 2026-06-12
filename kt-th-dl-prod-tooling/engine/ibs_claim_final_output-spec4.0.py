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

import sys
import math
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import re
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.functions import explode
from awsglue.dynamicframe import DynamicFrame

import ibs_utilities as ibs_utils

run_params = ibs_utils.get_run_parameters(sys.argv)
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def log_message(level, message):
    """
    Utility function for structured logging.
    Levels: INFO ✅, WARN ⚠️, ERROR ❌,
    """
    print(f"[{level}] {message}")
    
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3_bucket = run_params.args['BUCKET_NAME']
dataservice_env = run_params.args['DATASERVICE_ENV']
account_dataservice_no = run_params.args['ACCOUNT_DATASERVICE_NO']

BUCKET_NAME = run_params.args['BUCKET_NAME']
TABLE_NAME = run_params.args['TABLE_NAME']
POSITION_DATE = run_params.position_date

# Santi Teeragul: Add Index
INDEX = run_params.index
INDEX_STR = str(INDEX)
print("INDEX_STR : ", INDEX_STR)

print('Writing claim Domain to DynamoDB and S3!')
print('--Reading data...')
origin_col_list = [
    'claim_refer_code_of_company', 'claim_transaction_status', 'claim_company_id',
    'claim_pol_id', 'claim_pol_refer_code_of_company', 'claim_id', 'claim_no',
    'claim_type', 'claim_pol_type', 'claim_incurred_date', 'claim_report_date',
    'claim_notify_date', 'claim_discharge_date', 'claim_hospital_id', 'claim_hospital_name',
    'claim_doctor_name', 'claim_completion_date', 'claim_close_date', 'claim_status',
    'claim_reject_reason', 'claim_document_link', 'claim_causes_of_death',
    'claim_oic_case_id', 'claim_oic_case_detail', 'claim_request_channel',
    'claim_insrd_ids', 'claim_icd_10_codes', 'claim_icd_9_codes', 'claim_coverages',
    'estimates', 'claim_payments','transactiondate','index'
]

df_ibs_ordinary_claim = spark.table('processing_ibs.ibs_ordinary_claim').select(origin_col_list).filter(f"TransactionDate = '{run_params.system_date}'")

df_ibs_ordinary_claim = df_ibs_ordinary_claim.filter(f"index = '{run_params.index}'")

# Add the 'index' column (constant tag from run parameters) so we can partition on it later.
## TODO: add 'index' column to the dataframe for downstream partitioning/writes
df_ibs_ordinary_claim = df_ibs_ordinary_claim.withColumn("index", F.lit(INDEX_STR).cast(T.StringType()))

# df_ibs_ordinary_claim = df_ibs_ordinary_claim.filter(f"`TransactionDate` = '{run_params.system_date}' AND `index` = {run_params.index}")
# df_ibs_group_claim = spark.table('processing_ibs.ibs_group_claim').filter(f"TransactionDate = '{run_params.system_date}'")

print(df_ibs_ordinary_claim.count())
# print(df_ibs_group_claim.count())

print('--Preparating dataframe - Union')
df_claim_union = df_ibs_ordinary_claim

df_claim_union = df_claim_union.withColumnRenamed('transactiondate', 'TransactionDate')

df_claim_union = df_claim_union.withColumn("id",F.sha2(concat(df_claim_union['TransactionDate']\
                                                                ,df_claim_union['claim_refer_code_of_company']\
                                                                ,df_claim_union['index']\
                                                                 ),256))

print(df_ibs_ordinary_claim.count())
# print(df_ibs_group_claim.count())

print(df_claim_union.printSchema())
print(df_claim_union.count())

print("--Writing df to DynamoDB")
dyf_claim = DynamicFrame.fromDF(df_claim_union, glueContext, "test")

glueContext.write_dynamic_frame_from_options(
    frame=dyf_claim,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": "ap-southeast-1",
        "dynamodb.output.tableName": f"{dataservice_env}-claim-data",
        "dynamodb.sts.roleArn": f"arn:aws:iam::{account_dataservice_no}:role/{dataservice_env}-dynamodb-role01",
        "compression": "lzo",
        "dynamodb.throughput.write.percent": "1.0"
    }
)
print("--Wrote data to dynamoDB successfully!")


print("--Writing df to s3")

df_claim_union.repartition(1).write.format("json") \
        .option("ignoreNullField", "false") \
        .mode("append") \
        .partitionBy("TransactionDate") \
        .save(
        f's3://{s3_bucket}/data/spec4/export/ibs_claim_final_output/')

print("--Wrote data to S3 successfully!")
job.commit()
