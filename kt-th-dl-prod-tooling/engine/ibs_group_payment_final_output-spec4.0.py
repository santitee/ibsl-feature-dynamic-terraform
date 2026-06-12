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
from pyspark.sql.functions import to_json, struct, length

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

print('Writing payment Domain to DynamoDB and S3!')
print('--Reading data...')
origin_col_list = [
    'pmt_refer_code_of_company', 'pmt_transaction_status', 'pmt_company_id',
    'pmt_pol_id', 'pmt_pol_refer_code_of_company', 'pmt_id', 'pmt_type',
    'pmt_direct_premium', 'pmt_premium_payment_period_year', 'pmt_premium_payment_year',
    'pmt_payment_period', 'payment_period_seqs','transactiondate', 'index']

df_ibs_group_payment = spark.table('processing_ibs.ibs_group_payment').select(origin_col_list).filter(f"TransactionDate = '{run_params.system_date}'")

df_ibs_group_payment = df_ibs_group_payment.select(origin_col_list).filter(f"index = '{run_params.index}'")

# Add the 'index' column (constant tag from run parameters) so we can partition on it later.
## TODO: add 'index' column to the dataframe for downstream partitioning/writes
df_ibs_group_payment = df_ibs_group_payment.withColumn("index", F.lit(INDEX_STR).cast(T.StringType()))

# df_ibs_ordinary_payment = df_ibs_ordinary_payment.filter(f"`TransactionDate` = '{run_params.system_date}' AND `index` = {run_params.index}")

# df_ibs_pa_payment = spark.table('processing_ibs.ibs_pa_payment').filter(f"TransactionDate = '{run_params.system_date}'")
df_ibs_group_payment = spark.table('processing_ibs.ibs_group_payment').filter(f"TransactionDate = '{run_params.system_date}'")

# print(df_ibs_ordinary_payment.count())
# print(df_ibs_pa_payment.count())
print(df_ibs_group_payment.count())

print('--Preparating dataframe - Union')
df_payment_union = df_ibs_group_payment

df_payment_union = df_payment_union.withColumnRenamed('transactiondate', 'TransactionDate')

df_payment_union = df_payment_union.withColumn("id",F.sha2(concat(df_payment_union['TransactionDate']\
                                                                ,df_payment_union['pmt_refer_code_of_company']\
                                                                ,(df_payment_union['payment_period_seqs']).cast(T.StringType())\
                                                                ,(df_payment_union['index']).cast(T.StringType())\
                                                                 ),256))      

# add repartition
# df_payment_union = df_payment_union.repartition(1)
 
# Optional deduplicate
df_payment_union = df_payment_union.dropDuplicates(["id"])

print(df_payment_union.printSchema())
print(df_payment_union.count())


print("--Writing df to DynamoDB")
dyf_payment = DynamicFrame.fromDF(df_payment_union, glueContext, "test")

glueContext.write_dynamic_frame_from_options(
    frame=dyf_payment,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": "ap-southeast-1",
        "dynamodb.output.tableName": f"{dataservice_env}-payment-data",
        "dynamodb.sts.roleArn": f"arn:aws:iam::{account_dataservice_no}:role/{dataservice_env}-dynamodb-role01",
        "compression": "lzo",
        "dynamodb.throughput.write.percent": "1.0"
    }
)
print("--Wrote data to dynamoDB successfully!")


print("--Writing df to s3")

df_payment_union.repartition(1).write.format("json") \
        .option("ignoreNullField", "false") \
        .mode("append") \
        .partitionBy("TransactionDate") \
        .save(
        f's3://{s3_bucket}/data/spec4/export/ibs_group_payment_final_output/')

print("--Wrote data to S3 successfully!")
job.commit()
'''