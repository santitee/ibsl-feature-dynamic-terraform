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
from pyspark.sql.window import Window

import ibs_utilities as ibs_utils

run_params = ibs_utils.get_run_parameters(sys.argv)
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def log_message(level, message):
    """
    Utility function for structured logging.
    Levels: INFO ✅, WARN ⚠️, ERROR ❌,
    """
    print(f"[{level}] {message}")


spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3_bucket = run_params.args["BUCKET_NAME"]
dataservice_env = run_params.args["DATASERVICE_ENV"]
account_dataservice_no = run_params.args["ACCOUNT_DATASERVICE_NO"]

BUCKET_NAME = run_params.args["BUCKET_NAME"]
TABLE_NAME = run_params.args["TABLE_NAME"]
POSITION_DATE = run_params.position_date

# Santi Teeragul: Add Index
INDEX = run_params.index
INDEX_STR = str(INDEX)
print("INDEX_STR : ", INDEX_STR)

print("Writing Policy Domain to DynamoDB and S3!")
print("--Reading data...")
origin_col_list = [
    "pol_refer_code_of_company",
    "pol_transaction_status",
    "pol_company_id",
    "pol_id",
    "pol_type",
    "pol_type_gli_name",
    "pol_type_gli_plan",
    "pol_product_type",
    "pol_product_type_other_detail",
    "pol_type_2",
    "pol_sub_type",
    "pol_name",
    "pol_name_oic",
    "pol_start_date",
    "pol_end_date",
    "pol_claim_document",
    "pol_payment_term",
    "pol_payment_amount",
    "pol_payment_period",
    "pol_info_url",
    "pol_claim_url",
    "pol_document_urls",
    "pol_year",
    "pol_status",
    "pol_status_note",
    "pol_status_date",
    "pol_expropriation_value",
    "pol_immediate_refund",
    "pol_sale_date",
    "pol_transaction_date",
    "pol_approved_date",
    "pol_issue_date",
    "pol_account_date",
    "pol_agree_date",
    "pol_distribution",
    "pol_distribution_other_detail",
    "pol_license",
    "pol_re_insured_contact_number",
    "pol_life_detail",
    # คีย์ใหม่ที่มีใน schema แต่ไม่มีใน origin_col_list เดิม
    "renewal",
    "renewal_pol_id",
    "customer",
    "insureds",
    "sub_policies",
    "beneficiaries",
    "endorsements",
    "loans",
    "loan_payments",
    "investments",
    "transactiondate",
    "index",
]

## Unuse
# df_ibs_ordinary_policy = spark.table('processing_ibs.ibs_ordinary_policy').select(origin_col_list).filter(f"TransactionDate = '{run_params.system_date}'")
# df_ibs_ordinary_policy = df_ibs_ordinary_policy.select(origin_col_list).filter(f"index = '{run_params.index}'")

# Add the 'index' column (constant tag from run parameters) so we can partition on it later.
## TODO: add 'index' column to the dataframe for downstream partitioning/writes
# df_ibs_ordinary_policy = df_ibs_ordinary_policy.withColumn("index", F.lit(INDEX_STR))

# Apply Group Policy and add 'index'
df_ibs_group_policy = (
    spark.table("processing_ibs.ibs_group_insured")
    .select(origin_col_list)
    .filter(f"TransactionDate = '{run_params.system_date}'")
)

df_ibs_group_policy = df_ibs_group_policy.select(origin_col_list).filter(
    f"index = '{run_params.index}'"
)

## TODO: add 'index' column to the dataframe for downstream partitioning/writes
df_ibs_group_policy = df_ibs_group_policy.withColumn("index", F.lit(INDEX_STR))

# 2026-05-07 : added running number for inured
df_ibs_group_policy = df_ibs_group_policy.withColumn(
    "row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

# df_ibs_ordinary_policy.printSchema()
# print(df_ibs_ordinary_policy.count())

df_ibs_group_policy.printSchema()
print(df_ibs_group_policy.count())

print("--Preparating dataframe - Union")
# df_policy_union = df_ibs_ordinary_policy
df_policy_union = df_ibs_group_policy
# df_policy_union = df_ibs_ordinary_policy.union(df_ibs_group_policy)

df_policy_union = df_policy_union.withColumnRenamed(
    "transactiondate", "TransactionDate"
)

# df_policy_union = df_policy_union.withColumn("id",F.sha2(concat(df_policy_union['TransactionDate']\
#                                                                 ,df_policy_union['pol_refer_code_of_company']
#                                                                  ),256))

## TODO: Santi Teeragul Add column 'index' to generate id
# df_policy_union = df_policy_union.withColumn('index', lit(INDEX_STR).cast(T.StringType()))

# 2026-05-07 : for insured only force i to 1st index of id for optimize query in dynamodb
# df_policy_union = df_policy_union.withColumn("id", F.sha2(concat(df_policy_union['TransactionDate'],df_policy_union['pol_refer_code_of_company'],df_policy_union['index'],df_policy_union['row_num']  ),256) )
df_policy_union = df_policy_union.withColumn(
    "id",
    F.concat(
        F.lit("i"),
        F.sha2(
            concat(
                df_policy_union["TransactionDate"],
                df_policy_union["pol_refer_code_of_company"],
                df_policy_union["index"],
                df_policy_union["row_num"],
            ),
            256,
        ),
    ),
)

print("--Sample Data--")

df_policy_union.show()
print(df_policy_union.printSchema())
print(df_policy_union.count())
# df_policy_union = df_policy_union.repartition(1)

# Optional dedup
df_policy_union = df_policy_union.dropDuplicates(["id"])

print("--Writing df to DynamoDB")
dyf_policy = DynamicFrame.fromDF(df_policy_union, glueContext, "test")

glueContext.write_dynamic_frame_from_options(
    frame=dyf_policy,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": "ap-southeast-1",
        "dynamodb.output.tableName": f"{dataservice_env}-policy-data",
        "dynamodb.sts.roleArn": f"arn:aws:iam::{account_dataservice_no}:role/{dataservice_env}-dynamodb-role01",
        "compression": "lzo",
        "dynamodb.throughput.write.percent": "1.0",
    },
)
print("--Wrote data to dynamoDB successfully!")

print("--Writing df to s3")

# df_policy_union.repartition(1).write.format("json") \
df_policy_union.write.format("json").option("ignoreNullField", "false").mode(
    "append"
).partitionBy("TransactionDate").save(
    f"s3://{s3_bucket}/data/spec4/export/ibs_group_policy_final_output/"
)

print("--Wrote data to S3 successfully!")
job.commit()
