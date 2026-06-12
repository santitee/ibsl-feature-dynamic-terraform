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

# Use INDEX value from WORKFLOW
INDEX = run_params.index
INDEX_STR = str(INDEX)
print("INDEX_STR : ", INDEX_STR)

BUCKET_NAME = run_params.args['BUCKET_NAME']
TABLE_NAME = run_params.args['TABLE_NAME']
SYSTEM_DATE = run_params.system_date
SYSTEM_DATE_INDEX = SYSTEM_DATE + "_" + INDEX_STR

print("SYSTEM_DATE_INDEX : ", SYSTEM_DATE_INDEX)

# Define the schema for the DataFrame
schema = T.StructType([
    T.StructField("TransactionDate", T.StringType(), True),
    T.StructField("start_date_time", T.StringType(), True),
    T.StructField("finish_date_time", T.StringType(), True),
    T.StructField("status", T.StringType(), True)
])

# Create the sample data with the provided values
data = [
    (SYSTEM_DATE_INDEX, "", "", "Ready")
]

print("Print data: ",data)

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

print("--Writing df to DynamoDB")
dyf = DynamicFrame.fromDF(df, glueContext, "test")

glueContext.write_dynamic_frame_from_options(
    frame=dyf,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.region": "ap-southeast-1",
        "dynamodb.output.tableName": f"{dataservice_env}-complete-daily",
        "dynamodb.sts.roleArn": f"arn:aws:iam::{account_dataservice_no}:role/{dataservice_env}-dynamodb-role01",
        "compression": "lzo",
        "dynamodb.throughput.write.percent": "1.0"
    }
)
print("--Wrote data to dynamoDB successfully!")
job.commit()
