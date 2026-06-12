'''
job name     : IBS Utilities module.
Created by   : Kiatipoom Poopanitpun
Created date : 6 Mar 2022
version      : 0.1
'''

import sys
from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, max as max_
from pyspark.sql.window import Window
import math
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

@dataclass
class RunParameters:
    args: dict
    system_datetime: str
    system_date: str
    system_year: str
    system_month: str
    system_day: str
    position_date: str
    position_year: str
    position_month: str
    position_day: str
    workflow_type: str
    workflow_onetime_start_date: str
    workflow_onetime_end_date: str
    init: int
    index: int = 1



def is_running_in_workflow(sys_args):
    return '--WORKFLOW_NAME' in sys_args


def get_glue_params_with_options(sys_args, mandatory_args, optional_args):
    parameters = mandatory_args
    for arg in optional_args:
        if (f'--{arg}' in sys_args):
            parameters.append(arg)
    return getResolvedOptions(sys_args, parameters)

def validate_index(index_value, source_label):
    """ตรวจสอบค่า INDEX ให้เป็น int และ >= 1"""
    try:
        iv = int(index_value)
    except Exception:
        raise ValueError(f"[{source_label}] ค่า INDEX ไม่เป็นตัวเลข: {index_value!r}")
    if iv < 1:
        raise ValueError(f"[{source_label}] ค่า INDEX ต้องมากกว่าหรือเท่ากับ 1 แต่ได้: {iv}")
    return iv


def get_parameters_with_workflow(sys_args):
    print('Running within Workflow...')
    try:
        glue_client = boto3.client('glue')
        mandatory_args = [
            'JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'DATABASE_NAME',
            'TABLE_NAME', 'BUCKET_NAME', 'REGION_NAME',
            'RECORDS_AMOUNT_PERCENTAGE', 'PARTITION_POL_ID_KEY', 
            'DATASERVICE_ENV', 'ACCOUNT_DATASERVICE_NO',
            'INDEX'
        ]
        optional_args = ['SECRET_MANAGER_NAME', 'INIT']
        args = get_glue_params_with_options(sys_args, mandatory_args, optional_args)
        workflow_properties = glue_client.get_workflow_run_properties(Name=args['WORKFLOW_NAME'], RunId=args['WORKFLOW_RUN_ID'])
        
        #workflow_params = workflow_properties['RunProperties']
        
        #print("workflow_params : ",workflow_params)
        #print("args : ", args)
        #print("workflow_params['WORKFLOW_TYPE']", workflow_params['WORKFLOW_TYPE'])
        #print("index['INDEX']", workflow_params['INDEX'])
        
        ## TODO: New code to handle missing INDEX in workflow_params
        workflow_params = workflow_properties.get('RunProperties', {})

        # --- Logging ชัดเจนว่าค่า INDEX มาจาก Workflow ---
        print("===== WORKFLOW RUN PROPERTIES =====")
        print(f"WORKFLOW_NAME        : {args.get('WORKFLOW_NAME')}")
        print(f"WORKFLOW_RUN_ID      : {args.get('WORKFLOW_RUN_ID')}")
        print(f"WORKFLOW_TYPE        : {workflow_params.get('WORKFLOW_TYPE')}")
        print(f"INDEX (from workflow): {workflow_params.get('INDEX')}")
        print("===================================")
        
                # ตั้งค่า default และ validate
        init_value = int(workflow_params.get('INIT', args.get('INIT', '0')))
        index_raw = workflow_params.get('INDEX', '1')  # default 1 ถ้าไม่มี
        index_value = validate_index(index_raw, "WORKFLOW")

        run_parameters = RunParameters(
            args=args,
            system_datetime=workflow_params.get('system_datetime'),
            system_date=workflow_params.get('system_date'),
            system_year=workflow_params.get('system_year'),
            system_month=workflow_params.get('system_month'),
            system_day=workflow_params.get('system_day'),
            position_date=workflow_params.get('position_date'),
            position_year=workflow_params.get('position_year'),
            position_month=workflow_params.get('position_month'),
            position_day=workflow_params.get('position_day'),
            workflow_type=workflow_params.get('WORKFLOW_TYPE'),
            workflow_onetime_start_date=workflow_params.get('ONE_TIME_START_DATE'),
            workflow_onetime_end_date=workflow_params.get('ONE_TIME_END_DATE'),
            init=init_value,
            index=index_value
        )

        # --- สรุปค่า index ที่จะใช้จริง ---
        print(f"[WORKFLOW] INDEX ที่ใช้จริง: {run_parameters.index}")
        return run_parameters
    except Exception as e:
        print('Exception occurred while gathering parameters running within workflow:', e)
        raise
    """"
        ## TODO: comment old code

        run_parameters = RunParameters(
            args=args,
            system_datetime=workflow_params['system_datetime'],
            system_date=workflow_params['system_date'],
            system_year=workflow_params['system_year'],
            system_month=workflow_params['system_month'],
            system_day=workflow_params['system_day'],
            position_date=workflow_params['position_date'],
            position_year=workflow_params['position_year'],
            position_month=workflow_params['position_month'],
            position_day=workflow_params['position_day'],
            workflow_type=workflow_params['WORKFLOW_TYPE'],
            workflow_onetime_start_date=workflow_params['ONE_TIME_START_DATE'],
            workflow_onetime_end_date=workflow_params['ONE_TIME_END_DATE'],
            init=int(workflow_params['INIT']),
            index=int(workflow_params['INDEX'])
            )
        return run_parameters
    except Exception as e:
        print(
            'Exception ocurred while gathering parameters '
            'running within workflow:', e)
        raise
    """


def get_parameters_without_workflow(sys_args):
    print('Running as single job...')
    try:
        mandatory_args = [
            'JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'BUCKET_NAME',
            'POSITION_DATE', 'REGION_NAME', 'RECORDS_AMOUNT_PERCENTAGE',
            'PARTITION_POL_ID_KEY', 'WORKFLOW_TYPE', 'ONE_TIME_START_DATE',
            'ONE_TIME_END_DATE', 'DATASERVICE_ENV', 'ACCOUNT_DATASERVICE_NO',
            'INDEX'
        ]
        optional_args = ['SECRET_MANAGER_NAME', 'INIT']
        args = get_glue_params_with_options(sys_args, mandatory_args,
                                            optional_args)
        system_date = datetime.now() + timedelta(hours=7)
        position_date = datetime.strptime(args['POSITION_DATE'],
                                          '%Y-%m-%d') + timedelta(hours=7)
        
        index_raw = args['INDEX']  # default 1 ถ้าไม่มี
        index_value = validate_index(index_raw, "Job")
        
        run_parameters = RunParameters(
            args=args,
            system_datetime=system_date.strftime('%Y-%m-%d %H:%M:%S'),
            system_date=system_date.strftime('%Y-%m-%d'),
            system_year=system_date.strftime('%Y'),
            system_month=system_date.strftime('%m'),
            system_day=system_date.strftime('%d'),
            position_date=position_date.strftime('%Y-%m-%d'),
            position_year=position_date.strftime('%Y'),
            position_month=position_date.strftime('%m'),
            position_day=position_date.strftime('%d'),
            workflow_type=args.get('WORKFLOW_TYPE'),
            workflow_onetime_start_date=args.get('ONE_TIME_START_DATE'),
            workflow_onetime_end_date=args.get('ONE_TIME_END_DATE'),
            init=int(args.get('INIT', '0')),
            index=index_value
            )
        return run_parameters
    except Exception as e:
        print('Exception ocurred while gathering parameters running solo:', e)
        raise


def get_run_parameters(sys_args):
    print('Gathering run parameters...')
    if is_running_in_workflow(sys_args):
        run_parameters = get_parameters_with_workflow(sys_args)
    else:
        run_parameters = get_parameters_without_workflow(sys_args)
    print(f'Run parameters: {run_parameters}')
    return run_parameters


def build_sequence_partition(run_params, df, records_amount_percentage, partition_pol_id_key):
    print('Generating column PartitionNo')
    partition_amount = int(math.ceil(df.count() / int(20)))
    print('partition_amount : ', partition_amount)
    if(df.rdd.isEmpty()):
        print(f"There is no data that cannot repartitioning!")
    else :
        df = df.select('*').repartition(partition_amount, partition_pol_id_key)
    
    df = df.withColumn('PartitionNo', lit(spark_partition_id()).cast(T.StringType()))
    df = df.withColumn('DataDate', lit(run_params.position_date))
    df = df.withColumn('TransactionDate', lit(run_params.position_date))

    return df

def cdc(prev_df, new_df, str_id, str_refer_code_of_company, str_transaction_status):
    print(f'Get String of key field: {str_id}, {str_refer_code_of_company}')
    print(f'Get String of transaction_status field: {str_transaction_status}')
    print('Processing the exist data and patching "U" on status...')
    prev_df = prev_df.groupby(str_id, str_refer_code_of_company).agg(F.collect_list(str_transaction_status).alias('prev_status_stack'))
    df_exist = new_df \
        .join(prev_df, (new_df[str_id] == prev_df[str_id]) & (new_df[str_refer_code_of_company] == prev_df[str_refer_code_of_company]), 'leftsemi')\
            .withColumn('status_patched', lit('U'))\
                .drop(str_transaction_status)\
                    .withColumnRenamed('status_patched', str_transaction_status)
    print('Processing the non-exist data and patching "N" on status...')
    df_not_exist = new_df \
        .join(prev_df, (new_df[str_id] == prev_df[str_id]) & (new_df[str_refer_code_of_company] == prev_df[str_refer_code_of_company]), 'leftanti')\
            .withColumn('status_patched', lit('N'))\
                .drop(str_transaction_status)\
                    .withColumnRenamed('status_patched', str_transaction_status)
    print('Combining all dataframe...')
    # df = prev_df.union(df_exist).union(df_not_exist)
    df = df_exist.union(df_not_exist)
    print('--Done Change Data Capture process.')

    return df

def read_dynamodb(table_name):
    parameters = ['DATASERVICE_ENV', 'ACCOUNT_DATASERVICE_NO']
    args = getResolvedOptions(sys.argv, parameters)
    
    print(f'--Retriving DynamoDB table to Glue Dynamicframe: {table_name}')
    glue_context= GlueContext(SparkContext.getOrCreate())
    dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": f"{table_name}",
        "dynamodb.region": "ap-southeast-1",
        "dynamodb.sts.roleArn": f"arn:aws:iam::{args['ACCOUNT_DATASERVICE_NO']}:role/{args['DATASERVICE_ENV']}-dynamodb-role01",
        }
    )
    print(f'--Converting Dynamicframe: {table_name} to Spark Dataframe')
    df = dyf.toDF()
    print('--Done Retriving DynamoDB table process!')

    return df

def detect_err_new_status(domain_type):
    parameters = ['DATASERVICE_ENV', 'ACCOUNT_DATASERVICE_NO']
    args = getResolvedOptions(sys.argv, parameters)

    table_name = f"{args['DATASERVICE_ENV']}-transaction-logs"
    df = read_dynamodb(table_name)

    print('--Starting Rerun method for transaction logs...')
    list_field = ['TransactionDate', 'DomainType', 'pol_id', 'refer_code_of_company', 'transaction_status', 'Send_Status', 'PartitionNo']
    df = df.select(list_field).filter(df['DomainType']==domain_type)
    df = df.select('pol_id', 'refer_code_of_company', 'Send_Status', 'TransactionDate', 'transaction_status', 'PartitionNo') \
        .orderBy('pol_id', F.col('TransactionDate').desc())
    
    df = df.select('pol_id', 'refer_code_of_company', 'Send_Status', 'TransactionDate', 'transaction_status', 'PartitionNo', 
    F.row_number().over(Window.partitionBy("pol_id", "refer_code_of_company").orderBy(df['pol_id'])).alias("row_num"))
    df = df.filter(df['row_num'] ==1)
    df = df.select('pol_id', 'refer_code_of_company', 'Send_Status', 'TransactionDate', 'transaction_status', 'PartitionNo') \
        .filter((df['Send_Status']=='Fail') & (df['transaction_status']=='N')) 
    
    df = df.select('TransactionDate', 'transaction_status', 'PartitionNo', 'pol_id', 'refer_code_of_company')
    
    df.show()
    
    df = df.groupby('TransactionDate', 'PartitionNo').agg(F.collect_list(F.col('pol_id')).alias('list_pol_id'), 
                                                          F.collect_list(F.col('refer_code_of_company')).alias('list_ref_code_of_company')
                                                         )
    
    return df


## To build dataframe that will be contralize to filter each table for all tables should have data before transfer to IBS.
def build_delta_key(spark_session, df_delta_key, input_table, key_cols, start_date, end_date):
    df_delta_key = spark_session.table(
    f'processing_ibs.{input_table}')\
        .filter(((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd")) >= start_date) &
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd")) < end_date))\
        .groupBy(key_cols).count()\
        .select(key_cols)\
        .unionAll(df_delta_key)
    
    return df_delta_key


def build_policy_delta_key(spark_session, df_delta_key, df_input, key_cols, start_date, end_date):
    df_delta_key = df_input\
        .filter(((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd")) >= start_date) &
                ((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd")) < end_date))\
        .groupBy(key_cols).count()\
        .select(key_cols)\
        .unionAll(df_delta_key)
    
    return df_delta_key