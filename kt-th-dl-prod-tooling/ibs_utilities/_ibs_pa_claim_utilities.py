'''
job name     : IBS PA Claim Utilities module.
Created by   : Natchanon Thepyasuwan
Created date : 6 Mar 2022
version      : 0.1
'''

from datetime import datetime
from datetime import timedelta
from dataclasses import dataclass
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, concat, substring
from pyspark.sql.window import *


def pa_claim_aggregation(run_params, spark_session, start_date, end_date):
    if(run_params.workflow_type == 'Daily') :
        df_pa_claims_prep = spark_session.table(
            'processing_ibs.ibs_claims')\
                .withColumn('date_cleaned', F.from_unixtime(F.col('modifieddate')/1000,'yyyyMMdd'))\
                .filter((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date))\
                .drop('date_cleaned')
    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_lfkpa_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkpa_lfppml')\
                    .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
                    .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
                    .filter(
                            (F.col('pstu').isin('1'))\
                           )

        df_pa_claims_prep = spark_session.table(
            'processing_ibs.ibs_claims')
        df_pa_claims_prep = df_pa_claims_prep\
                .join(df_lfkpa_lfppml_source, df_lfkpa_lfppml_source['pno']==df_pa_claims_prep['policynumber'], 'inner')\
                .withColumn('date_cleaned', F.from_unixtime(F.col('modifieddate')/1000,'yyyyMMdd'))\
                .filter((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date))\
                .drop('date_cleaned')\
                .select(df_pa_claims_prep['*'])
    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_lfkpa_lfppml_source = spark_session.table(
            'processing_ibs.ibs_lfkpa_lfppml')\
                    .withColumn('lstdate_cleaned', F.from_unixtime(F.col('lstdate')/1000,'yyyyMMdd'))\
                    .withColumn('pdoi_cleaned', F.from_unixtime(F.col('pdoi')/1000,'yyyyMMdd'))\
                    .filter(
                            (~F.col('pstu').isin('1'))\
                           )
        
        df_pa_claims_prep = spark_session.table(
            'processing_ibs.ibs_claims')
        df_pa_claims_prep = df_pa_claims_prep\
                .join(df_lfkpa_lfppml_source, df_lfkpa_lfppml_source['pno']==df_pa_claims_prep['policynumber'], 'inner')\
                .withColumn('date_cleaned', F.from_unixtime(F.col('modifieddate')/1000,'yyyyMMdd'))\
                .filter((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date))\
                .drop('date_cleaned')\
                .select(df_pa_claims_prep['*'])
        


    df_pa_claimassessments_prep = spark_session.table(
        'processing_ibs.ibs_claimassessments')\
            .withColumn('date_cleaned', F.from_unixtime(F.col('modifieddate')/1000,'yyyyMMdd'))\
            .filter((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date))\
            .drop('date_cleaned')
    df_ref_hospital_code = spark_session.table(
        'processing_ibs.ibs_ref_hospital_code')
    df_ref_hospital_code = df_ref_hospital_code.select(
        'code', 'th_name').dropDuplicates(['th_name'])
    df_pa_oiccodes_prep = spark_session.table(
        'processing_ibs.ibs_oiccodes')
    df_ref_pa_coverage_code = spark_session.table(
        'processing_ibs.ibs_ref_pa_coverage_code')
    df_ref_oic_pa_coverage_code = spark_session.table(
        'processing_ibs.ibs_ref_oic_pa_coverage_code')

    df_pa_claims_col = df_pa_claims_prep.select(
        'claimid', 'InsuranceTypeID', 'PolicyNumber', 'ClaimNumber',
        'ClaimCategoryID', 'IncidentDate', 'ReceiveDate', 'DischargeDate',
        'Hospital', 'OICCodeID', 'PlanCode', 'citizenid',
        'SumInsuredBasic').filter(F.col('InsuranceTypeID') == '7')

    df_pa_claimassessments_col = df_pa_claimassessments_prep.select(
        'claimid', 'DecisionDate', 'DecisionID', 'AssessmentID',
        'DecisionRemark', 'ClaimAmt', 'CreatedDate', 'TotalPayment')

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = '1900-01-01T00:00:00+07:00'

    NON_REQ_NULL_STR = 'null'
    NON_REQ_NULL_NUM = 'null'
    NON_REQ_NULL_DATE = 'null'

    print('--Transforming data...')
    df_pa_clmast_max_assessmentID = df_pa_claimassessments_col.select('claimid','AssessmentID') \
                                    .groupby('claimid').agg(F.max('AssessmentID').alias('max_assessmentID')).sort('claimid')
    df_pa_claimassessments_temp = df_pa_claimassessments_col \
                                    .select('claimid','DecisionID','AssessmentID','DecisionRemark','ClaimAmt','CreatedDate','TotalPayment')\
                                    .withColumnRenamed('claimid', 'claimid_temp')\
                                    .withColumnRenamed('AssessmentID', 'AssessmentID_temp')
    df_pa_clmast_max_assessmentID = df_pa_clmast_max_assessmentID \
                                    .join(df_pa_claimassessments_temp,
                                         df_pa_clmast_max_assessmentID['max_assessmentID']==df_pa_claimassessments_temp['AssessmentID_temp'],
                                          'left'
                                         ).drop('claimid')
    df_pa_clmast_max_decisiondate = df_pa_claimassessments_col \
                                    .groupby('claimid').agg(F.max('DecisionDate').alias('maxdate')).sort('claimid')
    df_pa_clmast_max_decisiondate = df_pa_clmast_max_decisiondate \
                                    .withColumn('max_decisiondate', F.from_unixtime(F.col('maxdate')/1000,'yyyy-MM-dd'))
    df_pa_oiccodes_col = df_pa_oiccodes_prep.select('OICCodeID', 'OICCode', 'Description')\
                                            .withColumnRenamed('OICCodeID', 'OICCodeID_temp')\
                                            .withColumnRenamed('Description', 'oiccodes_description')
    df_ref_oic_pa_coverage_code = df_ref_oic_pa_coverage_code.withColumnRenamed(
        'oic_code', 'oic_code_temp')
    output_df = df_pa_claims_col.withColumn('claim_refer_code_of_company', F.when(F.col('InsuranceTypeID')=='7',
                                                                                 concat(lit('CLM_'), F.trim(F.col('claimid'))))
                                            .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())
                                           )\
                                .withColumn('claim_transaction_status', lit('N').cast(T.StringType()))\
                                .withColumn('claim_company_id', lit('1022').cast(T.StringType()))\
                                .withColumn('claim_pol_id',
                                                F.when(F.trim(F.col('PolicyNumber')).isNotNull(), F.trim(F.col('PolicyNumber')))
                                                .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()))\
                                .withColumn('claim_pol_refer_code_of_company',
                                                F.when(F.trim(F.col('PolicyNumber')).isNotNull(), concat(lit('POL_'), F.trim(F.col('PolicyNumber'))))
                                                .otherwise(lit(REQ_NULL_STR)).cast(T.StringType()))\
                                .withColumn('claim_id', F.trim(F.col('ClaimNumber')).cast(T.StringType()))\
                                .withColumn('claim_no', F.trim(F.col('claimid')).cast(T.StringType()))\
                                .withColumn('claim_type',
                                            F.when(F.trim(F.col('ClaimCategoryID'))=='1', lit('02'))
                                            .when(F.trim(F.col('ClaimCategoryID'))=='2', lit('01'))
                                            .otherwise(lit(REQ_NULL_STR)).cast(T.StringType())
                                           )\
                                .withColumn('claim_pol_type', lit('PT-09').cast(T.StringType()))\
                                .withColumn('claim_incurred_date',
                                            F.when(F.col('IncidentDate').isNull(), F.concat(F.from_unixtime(F.col('ReceiveDate')/1000,'yyyy-MM-dd'),lit('T00:00:00+07:00')))
                                            .otherwise(F.concat(F.from_unixtime(F.col('IncidentDate')/1000,'yyyy-MM-dd'),lit('T00:00:00+07:00'))).cast(T.StringType())
                                           )\
                                .withColumn('claim_report_date', F.when((F.col('ReceiveDate').isNotNull()) & (F.col('ReceiveDate')!='0'),
                                                                        F.concat(F.from_unixtime(F.col('ReceiveDate')/1000,'yyyy-MM-dd'),lit('T00:00:00+07:00')))
                                                                .otherwise(lit(REQ_NULL_DATE)).cast(T.StringType()))\
                                .withColumn('claim_notify_date', F.when((F.col('IncidentDate').isNotNull()) & (F.col('IncidentDate')!='0'),
                                                                        F.concat(F.from_unixtime(F.col('IncidentDate')/1000,'yyyy-MM-dd'),lit('T00:00:00+07:00')))
                                                                .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType()))\
                                .withColumn('claim_discharge_date', F.when((F.col('DischargeDate').isNotNull()) & (F.col('DischargeDate')!='0'),
                                                                        F.concat(F.from_unixtime(F.col('DischargeDate')/1000,'yyyy-MM-dd'),lit('T00:00:00+07:00')))
                                                                .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType()))\
                                .join(df_ref_hospital_code,
                                      df_pa_claims_col.Hospital==df_ref_hospital_code.th_name, "left")\
                                .withColumn('claim_hospital_id',
                                    F.when(F.col('code').isNotNull(), F.col('code'))
                                    .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()))\
                                .withColumn('claim_hospital_name',
                                    F.when(F.col('th_name').isNotNull(), F.regexp_replace(F.regexp_replace(F.trim(F.substring((F.col('th_name')), 1, 100)),
                                                                            "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""),"\\\\", ""))
                                    .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()))\
                                .withColumn('claim_doctor_name', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                .join(df_pa_clmast_max_decisiondate,
                                     df_pa_claims_col['claimid']==df_pa_clmast_max_decisiondate['claimid'], 'left')\
                                .withColumn('claim_completion_date', F.when((F.col('max_decisiondate').isNotNull()) & (F.col('max_decisiondate')!='0'),
                                                                            F.concat(F.col('max_decisiondate'),lit('T00:00:00+07:00')))
                                                                    .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType()))\
                                .withColumn('claim_close_date', F.when((F.col('max_decisiondate').isNotNull()) & (F.col('max_decisiondate')!='0'),
                                                                            F.concat(F.col('max_decisiondate'),lit('T00:00:00+07:00')))
                                                                    .otherwise(lit(NON_REQ_NULL_DATE)).cast(T.StringType()))\
                                .join(df_pa_clmast_max_assessmentID,
                                     df_pa_claims_col.claimid==df_pa_clmast_max_assessmentID.claimid_temp,
                                      'left'
                                     )\
                                .withColumn('claim_status', F.when(F.trim(F.col('DecisionID'))=='1', lit('01'))
                                                            .when(F.trim(F.col('DecisionID'))=='2', lit('05'))
                                                            .when(F.trim(F.col('DecisionID'))=='3', lit('05'))
                                                            .when(F.trim(F.col('DecisionID'))=='4', lit('05'))
                                                            .when(F.trim(F.col('DecisionID'))=='5', lit('05'))
                                                            .when(F.trim(F.col('DecisionID'))=='6', lit('03'))
                                                            .otherwise(lit('03'))\
                                                            .cast(T.StringType())
                                           )\
                                .withColumn('claim_reject_reason', F.when(F.trim(F.col('DecisionID')).isin(['2','3','4','5']),
                                                                        F.when(F.lower(F.trim(F.col('DecisionRemark'))).like('%void%'), lit('08'))\
                                                                        .when(F.lower(F.trim(F.col('DecisionRemark'))).like('%เวลารอคอย%'), lit('03'))\
                                                                        .when(F.lower(F.trim(F.col('DecisionRemark'))).like('%waiting%'), lit('03'))\
                                                                        .when(F.lower(F.trim(F.col('DecisionRemark'))).like('%ฆ่าตัวตาย%1%'), lit('09'))\
                                                                        .otherwise(lit('99'))
                                                                    )
                                                                   .otherwise(lit('99')).cast(T.StringType())
                                           )\
                                .withColumn('claim_document_link', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                .withColumn('claim_causes_of_death', lit('99').cast(T.StringType()))\
                                .join(df_pa_oiccodes_col,
                                     df_pa_claims_col.OICCodeID==df_pa_oiccodes_col.OICCodeID_temp,
                                     'left')\
                                .withColumn('claim_oic_case_id',
                                    F.when(F.trim(F.col('OICCode')).isNotNull(), F.trim(F.col('OICCode')))
                                    .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()))\
                                .withColumn('claim_oic_case_detail',
                                    F.when(F.trim(F.col('oiccodes_description')).isNotNull(), 
                                                                F.regexp_replace(F.regexp_replace(F.trim(F.substring(F.col('oiccodes_description'), 1, 100)),
                                                                "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""),"\\\\", "")\
                                    )\
                                    .otherwise(lit(NON_REQ_NULL_STR)).cast(T.StringType()))\
                                .withColumn('claim_request_channel', lit(NON_REQ_NULL_STR).cast(T.StringType()))\
                                .withColumn('claim_insrd_ids', 
                                    F.array(\
                                        F.when(F.col('citizenid').isNotNull(), F.col('citizenid'))
                                        .otherwise('').cast(T.StringType())
                                        )
                                    )\
                                .withColumn('claim_icd_10_codes', F.array())\
                                .withColumn('claim_icd_09_codes', F.array())\
                                .join(df_ref_pa_coverage_code,
                                     df_pa_claims_col.PlanCode==df_ref_pa_coverage_code.planc,
                                     'left')\
                                .join(df_ref_oic_pa_coverage_code,
                                     df_ref_pa_coverage_code.oic_code==df_ref_oic_pa_coverage_code.oic_code_temp
                                     )\
                                .withColumn('claim_coverages',
                                            F.array(\
                                                   F.struct(\
                                                            F.when(F.col('PlanCode').isNotNull(), F.col('oic_code'))\
                                                            .when((F.col('PlanCode').isNull())|(F.col('PlanCode')==''), lit('C20-016'))\
                                                            .otherwise(lit(REQ_NULL_STR))\
                                                            .cast(T.StringType()).alias('claim_coverage_code'),

                                                            F.when(F.col('PlanCode').isNotNull(), 
                                                                F.regexp_replace(F.regexp_replace(F.trim(F.substring(F.col('description_thai'), 1, 100)),
                                                                "[^\u0E00-\u0E7F0-9A-Za-z .,/-_@]", ""),"\\\\", "")\
                                                                  )\
                                                            .when((F.col('PlanCode').isNull())|(F.col('PlanCode')==''), lit('J0016 อื่น ๆ โปรดระบุ………'))\
                                                            .otherwise(lit(REQ_NULL_STR))\
                                                            .cast(T.StringType()).alias('claim_coverage_detail'),
                                                            
                                                            F.when((F.col('SumInsuredBasic')).isNull(), lit(NON_REQ_NULL_NUM))\
                                                            .otherwise(F.col('SumInsuredBasic').cast(T.DecimalType(18,2)))
                                                            .cast(T.StringType()).alias('claim_sum_insured_per_coverage_per_year'),
                                                            lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_sum_insured_per_coverage_per_time'),
                                                            F.when((F.col('SumInsuredBasic')).isNull(), lit(NON_REQ_NULL_NUM))\
                                                            .otherwise(F.col('SumInsuredBasic').cast(T.DecimalType(18,2)))\
                                                            .cast(T.StringType()).alias('claim_coverage_amt'),
                                                            lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_deductible_amt'),
                                                            F.when(F.col('ClaimAmt').isNull(), lit(NON_REQ_NULL_NUM))\
                                                            .otherwise(F.col('ClaimAmt').cast(T.DecimalType(18,2)))\
                                                            .cast(T.StringType()).alias('claim_amount'),
                                                            F.array(\
                                                                    F.struct(\
                                                                             lit('105090000').cast(T.StringType()).alias('claim_pol_coverage_detail_simb2'),
                                                                             lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_pol_coverage_detail_simb2_amt'),
                                                                            )
                                                                   ).alias('simb2s')
                                                           )
                                                   )
                                           )\
                                .withColumn('estimates', F.array(\
                                                F.struct(\
                                                    lit(REQ_NULL_INT).cast(T.IntegerType()).cast(T.StringType()).alias('claim_estimate_seq'),
                                                    lit(REQ_NULL_DATE).cast(T.StringType()).alias('claim_estimate_date'),
                                                    lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).cast(T.StringType()).alias('claim_estimate_amount')
                                                    )
                                                )
                                            )\
                                .withColumn('claim_payments',
                                            F.array(\
                                                    F.struct(\
                                                             F.when(F.trim(F.col('DecisionID'))=='1', F.col('claimid_temp'))
                                                             .when(F.trim(F.col('DecisionID'))!='1', lit(REQ_NULL_STR))
                                                             .otherwise(lit(REQ_NULL_STR))\
                                                             .cast(T.StringType()).alias('claim_payment_id'),
                                                             lit('99').cast(T.StringType()).alias('claim_payment_type'),
                                                             lit(REQ_NULL_INT).cast(T.IntegerType()).cast(T.StringType()).alias('claim_payment_term'),
                                                             lit(REQ_NULL_INT).cast(T.IntegerType()).cast(T.StringType()).alias('claim_payment_term_paid'),
                                                             lit('99').cast(T.StringType()).alias('claim_payment_channel'),
                                                             F.when(F.trim(F.col('DecisionID'))=='1', F.concat(F.from_unixtime(F.col('CreatedDate')/1000,'yyyy-MM-dd'),lit('T00:00:00+07:00')))
                                                             .otherwise(lit(REQ_NULL_DATE))\
                                                             .cast(T.StringType()).alias('claim_payment_date'),
                                                             lit(REQ_NULL_DATE).cast(T.StringType()).alias('claim_payment_account_date'),
                                                             F.when(F.col('TotalPayment').isNull(), lit(REQ_NULL_DEC))\
                                                             .otherwise(F.col('TotalPayment'))\
                                                             .cast(T.DecimalType(18,2)).cast(T.StringType()).alias('claim_payment_invoice_amt'),
                                                             F.when(F.col('TotalPayment').isNull(), lit(REQ_NULL_DEC))\
                                                             .otherwise(F.col('TotalPayment'))\
                                                             .cast(T.DecimalType(18,2)).cast(T.StringType()).alias('claim_payment_amt'),
                                                             lit(NON_REQ_NULL_NUM).cast(T.StringType()).alias('claim_payment_other_invoice_amt'),
                                                             lit(NON_REQ_NULL_STR).cast(T.StringType()).alias('claim_payment_beneficiary_name')
                                                            )
                                                   )
                                           )

    output_df = output_df.select(
        'claim_refer_code_of_company', 'claim_transaction_status',
        'claim_company_id', 'claim_pol_id', 'claim_pol_refer_code_of_company',
        'claim_id', 'claim_no', 'claim_type', 'claim_pol_type',
        'claim_incurred_date', 'claim_report_date', 'claim_notify_date',
        'claim_discharge_date', 'claim_hospital_id', 'claim_hospital_name',
        'claim_doctor_name', 'claim_completion_date', 'claim_close_date',
        'claim_status', 'claim_reject_reason', 'claim_document_link',
        'claim_causes_of_death', 'claim_oic_case_id', 'claim_oic_case_detail', 
        'claim_request_channel', 'claim_insrd_ids',
        'claim_icd_10_codes', 'claim_icd_09_codes', 'claim_coverages',
        'estimates', 'claim_payments')
    return output_df
