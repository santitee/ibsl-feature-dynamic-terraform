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

import boto3
import time
from itertools import islice
from typing import Iterator, List, Dict

run_params = ibs_utils.get_run_parameters(sys.argv)

print('--Declaring Spark Session...')
sc = SparkContext()
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session

job = Job(glueContext)
job.init(run_params.args['JOB_NAME'], run_params.args)

# Glue PySpark cell: Delete DynamoDB items by (TransactionDate, id)


# ========= Config =========
TABLE_NAME = "w-kt-th-ibs-np-as1-policy-data"
TARGET_DATE = "2025-09-22"       # วันที่ที่ต้องการลบทั้งหมด
REGION_NAME = "ap-southeast-1"   # เช่น "ap-southeast-1"; ถ้า None จะใช้ค่า default ของ environment

# DynamoDB constraints
BATCH_SIZE = 25                  # ข้อจำกัดของ DynamoDB: สูงสุด 25 ต่อ batch
MAX_RETRIES = 6                  # retry สำหรับ UnprocessedItems
BACKOFF_BASE_SEC = 1.0           # เริ่มต้น backoff 1,2,4,8,...

# Parallelism control (ช่วยคุม throttling)
MAX_CONCURRENCY = 16             # จำนวน partitions สูงสุดสำหรับการลบ (ปรับตาม WCU/On-Demand ของตารางคุณ)

# ชนิดของคีย์ TransactionDate ในตาราง (โดยปกติเป็นสตริง "S")
TXN_DATE_DDB_TYPE = "S"          # ใช้ "S" (string) | ถ้าตารางเก็บเป็นตัวเลข ให้เปลี่ยนเป็น "N"
# ==========================


# ---------- Utilities ----------
def _new_dynamo_client():
    session = boto3.session.Session(region_name=REGION_NAME) if REGION_NAME else boto3.session.Session()
    return session.client("dynamodb")

def _query_keys_for_date(client, table_name: str, date_value: str, date_type: str = "S") -> List[Dict]:
    """
    Query เฉพาะ partition key = TransactionDate (เท่ากับ TARGET_DATE) และดึงเฉพาะคีย์สำหรับลบ
    คืนค่าเป็นลิสต์ของ Key objects ในรูปแบบ DynamoDB JSON: 
      {"TransactionDate": {"S": ...}, "id": {"S": ...}}
    หมายเหตุ: ประเภทของ 'id' จะใช้ตามที่ DynamoDB ส่งกลับมา (S/N/...) เพื่อความถูกต้อง
    """
    keys: List[Dict] = []
    kwargs = {
        "TableName": table_name,
        "KeyConditionExpression": "TransactionDate = :val",
        "ExpressionAttributeValues": {":val": {date_type: date_value}},
        "ProjectionExpression": "id",   # ดึงเฉพาะคีย์ (id)
        # ถ้าชื่อฟิลด์เป็น reserved keyword ให้ใช้ ExpressionAttributeNames มาช่วย (กรณีนี้ไม่จำเป็น)
    }

    while True:
        resp = client.query(**kwargs)
        for item in resp.get("Items", []):
            # item["id"] มีชนิดตามจริง เช่น {"S": "abc"} หรือ {"N": "123"}
            keys.append({
                "TransactionDate": {date_type: date_value},
                "id": item["id"]
            })

        lek = resp.get("LastEvaluatedKey")
        if lek:
            kwargs["ExclusiveStartKey"] = lek
        else:
            break

    return keys

def _chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            break
        yield chunk

def _flush_batch_delete(client, table_name: str, delete_requests: List[Dict]) -> int:
    """
    ส่ง batch delete พร้อม retry UnprocessedItems แบบ exponential backoff
    delete_requests: รูปแบบ [{"DeleteRequest": {"Key": {...}}}, ...]
    คืนค่าจำนวนรายการใน batch นี้ (ที่ลบสำเร็จหลัง retry)
    """
    attempt = 0
    request_items = {table_name: delete_requests}

    while True:
        resp = client.batch_write_item(RequestItems=request_items)
        unproc = resp.get("UnprocessedItems", {}).get(table_name, [])
        if not unproc:
            return len(delete_requests)  # ทั้งหมดสำเร็จแล้ว

        if attempt >= MAX_RETRIES:
            # ยังเหลือ unprocessed หลัง retry หมด — คุณอาจเปลี่ยนเป็น log warning แทน raise ก็ได้
            raise RuntimeError(f"UnprocessedItems remain after {MAX_RETRIES} retries: {len(unproc)} items")

        sleep_sec = BACKOFF_BASE_SEC * (2 ** attempt)
        time.sleep(sleep_sec)
        request_items = {table_name: unproc}
        attempt += 1

def _delete_partition(iter_keys: Iterator[Dict]) -> Iterator[int]:
    """
    ทำงานในแต่ละ partition ของ Spark:
    - สร้าง boto3 client ใหม่ใน worker
    - batch เป็นกลุ่มละ 25
    - retry หากมี UnprocessedItems
    คืนค่าเป็น iterator ของจำนวนที่ลบได้ใน partition นี้
    """
    client = _new_dynamo_client()
    deleted = 0
    buffer: List[Dict] = []

    for key in iter_keys:
        buffer.append({"DeleteRequest": {"Key": key}})
        if len(buffer) == BATCH_SIZE:
            deleted += _flush_batch_delete(client, TABLE_NAME, buffer)
            buffer.clear()

    if buffer:
        deleted += _flush_batch_delete(client, TABLE_NAME, buffer)

    yield deleted
# ------------------------------


# 1) ดึงรายการคีย์ทั้งหมดของวันที่ที่ต้องการลบ (Query เฉพาะพาร์ติชันนั้น)
driver_client = _new_dynamo_client()
keys = _query_keys_for_date(driver_client, TABLE_NAME, TARGET_DATE, TXN_DATE_DDB_TYPE)
total_to_delete = len(keys)

print(f"[INFO] Table={TABLE_NAME} date={TARGET_DATE} -> {total_to_delete} items to delete")

if total_to_delete == 0:
    print("[INFO] ไม่พบข้อมูลสำหรับลบ (จบงาน)")
else:
    # 2) เตรียม parallelism (อย่าให้สูงเกินไป เสี่ยงเจอ throttling)
    #    heuristic: 1 slice ต่อ ~2,000 รายการ (ปรับได้), แต่ไม่เกิน MAX_CONCURRENCY
    slices = min(MAX_CONCURRENCY, max(1, total_to_delete // 2000 or 1))

    rdd = spark.sparkContext.parallelize(keys, slices)
    per_partition_deleted = rdd.mapPartitions(_delete_partition).collect()
    total_deleted = sum(per_partition_deleted)

    print(f"[DONE] Deleted {total_deleted}/{total_to_delete} items "
          f"in {len(per_partition_deleted)} partitions from {TABLE_NAME} for date {TARGET_DATE}")

job.commit()
