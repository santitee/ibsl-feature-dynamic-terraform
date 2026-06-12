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


# Zero Guard for Decimal Fields
_ZERO_GUARD_ENABLED = True
_ZERO_GUARD_EPSILON = 0.001

def _zero_guard(col_expr):
    if _ZERO_GUARD_ENABLED:
        return F.when(col_expr == 0.0, F.lit(_ZERO_GUARD_EPSILON)).otherwise(col_expr).cast(T.DoubleType())
    return col_expr.cast(T.DoubleType())

def _zero_guard_decimal(col_expr, precision=18, scale=2):
    dec = T.DecimalType(precision, scale)
    if 1:
        return F.when(col_expr.cast(dec) == F.lit(0).cast(dec),
                      F.lit(_ZERO_GUARD_EPSILON).cast(dec)).otherwise(col_expr.cast(dec))
    return col_expr.cast(dec)

def _to_iso_tz7_from_yyyymmdd(col_expr):
    c = col_expr.cast(T.StringType())
    return F.when(
        (c.isNotNull()) & (F.length(c) == 8),
        F.concat(
            F.substring(c,1,4), F.lit("-"),
            F.substring(c,5,2), F.lit("-"),
            F.substring(c,7,2), F.lit("T00:00:00+07:00")
        )
    ).otherwise(F.lit(None))

def group_payment_aggregation(run_params, spark_session, start_date, end_date):

    # =====================================================
    # SECTION 1: Read Source Tables
    # =====================================================
    print('Reading data source payment tables...')

    # filter ibs_atlldta_chdrpf ,_ilcl
    df_chdrpf_filter_source = spark_session.table(
            'processing_ibs.ibs_atlldta_chdrpf').select('chdrnum','statcode').distinct()\
                .filter((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date))
    df_chdrpf_ilcl_filter_source = spark_session.table(
            'processing_ibs.ibs_atlldta_chdrpf_ilcl').select('chdrnum','statcode').distinct()\
                .filter((F.date_format(F.to_date('datime', 'yyyy-MM-dd').cast(T.StringType()), "yyyyMMdd") < end_date))
    
    df_chdrpf_filter_source = df_chdrpf_filter_source.unionByName(df_chdrpf_ilcl_filter_source).dropDuplicates()

    if(run_params.workflow_type == 'Daily') :
        print('Reading data source transactional tables...')
        df_gbihpf_source = spark_session.table('processing_ibs.ibs_atlldta_gbihpf')\
                            .select('billno', 'chdrnum', 'nrflg', 'transaction_date').distinct()

        df_gbihpf_source = df_gbihpf_source\
                .join(df_chdrpf_filter_source, df_gbihpf_source['chdrnum'] == df_chdrpf_filter_source['chdrnum'], 'inner') \
                            .withColumn('date_cleaned', F.date_format(F.to_date(F.col('transaction_date'), 'yyMMdd'), 'yyyyMMdd'))\
                            .filter((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date))\
                            .drop('date_cleaned')

    elif(run_params.workflow_type == 'One-Time-Inforce'):
        df_chdrpf_filter_source = df_chdrpf_filter_source.filter(F.col('statcode').isin('IF', 'PN', 'PE', 'PR'))

        df_gbihpf_source = spark_session.table('processing_ibs.ibs_atlldta_gbihpf')\
                            .select('billno', 'chdrnum', 'nrflg', 'transaction_date').distinct()
        
        df_gbihpf_source = df_gbihpf_source\
                .join(df_chdrpf_filter_source, df_gbihpf_source['chdrnum'] == df_chdrpf_filter_source['chdrnum'], 'inner') \
                    .withColumn('date_cleaned', F.date_format(F.to_date(F.col('transaction_date'), 'yyMMdd'), 'yyyyMMdd'))\
                    .filter(((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date)))\
                    .drop('date_cleaned')\
                .select(df_gbihpf_source['*'])

    elif(run_params.workflow_type == 'One-Time-Non-Inforce'):
        df_chdrpf_filter_source = df_chdrpf_filter_source.filter((~F.col('statcode').isin('IF', 'PN', 'PE', 'PR')))

        df_gbihpf_source = spark_session.table('processing_ibs.ibs_atlldta_gbihpf')\
                            .select('billno', 'chdrnum', 'nrflg', 'transaction_date').distinct()
        
        df_gbihpf_source = df_gbihpf_source\
                .join(df_chdrpf_filter_source, df_gbihpf_source['chdrnum'] == df_chdrpf_filter_source['chdrnum'], 'inner') \
                    .withColumn('date_cleaned', F.date_format(F.to_date(F.col('transaction_date'), 'yyMMdd'), 'yyyyMMdd'))\
                    .filter(((F.col('date_cleaned') >= start_date) & (F.col('date_cleaned') < end_date)))\
                    .drop('date_cleaned')\
                .select(df_gbihpf_source['*'])
    
    df_gchipf_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf')\
                        .select('chdrnum', 'ccdate').distinct()
    # 2026/05/11 : union df_gchipf_ilcl_source
    df_gchipf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf_ilcl').select('chdrnum', 'ccdate').distinct()
    df_gchipf_source = df_gchipf_source.unionByName(df_gchipf_ilcl_source).dropDuplicates()

    
    df_gpmdpf_source = spark_session.table('processing_ibs.ibs_atlldta_gpmdpf')\
                        .select('chdrnum', 'mbrno', 'billno', 'pprem', 'prodtyp').distinct()

    # --- Rider premium (exclude life products) ---
    LIFE_PROD_LIST = ['GCL1','GCL2','GCL3','GCL4','GCL5','GLF1','GLTS','GPHS','GTL1','GTL2','GTL3','GTL4','MRTA']

    df_gpmdpf_rider_detail_base = (
        df_gpmdpf_source
          .filter(~F.col("prodtyp").isin(LIFE_PROD_LIST))
          .select("chdrnum", "billno", "prodtyp", "mbrno", "pprem")
    )

    # 2) Case A
    df_rider_case_A = (
        df_gpmdpf_rider_detail_base
          .filter((F.col("chdrnum").startswith("8")) | (F.col("chdrnum").startswith("CS")))
          .groupBy("chdrnum", "billno", "prodtyp")
          .agg(F.sum("pprem").alias("pmt_premium_rid_amount"))
          .withColumn(
              "pmt_premium_rid_number",
              F.concat(F.col("chdrnum"), F.lit("_"), F.col("prodtyp"))
          )
          .select("chdrnum", "billno", "pmt_premium_rid_number", "pmt_premium_rid_amount")
    )

    # 3) Case B
    df_rider_case_B = (
        df_gpmdpf_rider_detail_base
          .filter(~((F.col("chdrnum").startswith("8")) | (F.col("chdrnum").startswith("CS"))))
          .groupBy("chdrnum", "billno", "prodtyp", "mbrno")
          .agg(F.sum("pprem").alias("pmt_premium_rid_amount"))
          .withColumn(
              "pmt_premium_rid_number",
              F.concat(F.col("chdrnum"), F.lit("_"), F.col("mbrno"), F.lit("_"), F.col("prodtyp"))
          )
          .select("chdrnum", "billno", "pmt_premium_rid_number", "pmt_premium_rid_amount")
    )

    # 4) union
    df_gpmdpf_rider_detail = df_rider_case_A.unionByName(df_rider_case_B).dropDuplicates()

    # 5) collect array 
    df_gpmdpf_rider = (
        df_gpmdpf_rider_detail
          .groupBy("chdrnum", "billno")
          .agg(
              F.collect_list(
                  F.struct(
                      F.col("pmt_premium_rid_number"),
                      F.col("pmt_premium_rid_amount")
                  )
              ).alias("payment_premium_type_riders")
          )
    )
    # --- Rider premium (exclude life products) ---

    df_gpmdpf_1 = df_gpmdpf_source.groupBy('chdrnum', 'billno').agg(F.sum('pprem').alias('pmt_prd_premium_amount'))

    df_gpmdpf_2 = df_gpmdpf_source.filter("prodtyp in ('GCL1','GCL2','GCL3','GCL4','GCL5','GLF1','GLTS','GPHS','GTL1','GTL2','GTL3','GTL4','MRTA')")\
                                .groupBy('chdrnum', 'billno', 'prodtyp').agg(F.sum('pprem').alias('pmt_prd_premium_amount_life'))
    
    df_gpmdpf_life = df_gpmdpf_source \
                        .filter("prodtyp in ('GCL1','GCL2','GCL3','GCL4','GCL5','GLF1','GLTS','GPHS','GTL1','GTL2','GTL3','GTL4','MRTA')") \
                        .groupBy('chdrnum', 'billno') \
                        .agg(F.sum('pprem').alias('pmt_prd_premium_amount_life'))


    df_gpmdpf_agg = (df_gpmdpf_2.alias('gpm2').join(df_gpmdpf_1.alias('gpm1'),
                         (F.trim(F.col('gpm2.chdrnum')) == F.trim(F.col('gpm1.chdrnum')))&
                         (F.trim(F.col('gpm2.billno')) == F.trim(F.col('gpm1.billno')))
                         ,'left')
                  ).select('gpm2.chdrnum', 'gpm2.billno', 'prodtyp', 'pmt_prd_premium_amount','pmt_prd_premium_amount_life')


    df_chdrpf_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf').select('chdrnum').distinct()
    df_chdrpf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl').select('chdrnum').distinct()
    df_chdrpf_source = df_chdrpf_source.unionByName(df_chdrpf_ilcl_source).dropDuplicates()

    df_chdrpf_main = df_chdrpf_source.alias('chd').join(
                        df_gpmdpf_source.select('chdrnum','mbrno','prodtyp').distinct().alias('gpmm'),
                        F.trim(F.col('chd.chdrnum')) == F.trim(F.col('gpmm.chdrnum')),'inner').select('chd.chdrnum','mbrno','prodtyp')

    df_chdrpf_main = df_chdrpf_main.withColumn('pmt_premium_rid_number',          
                        F.when(
                                (F.col("chdrnum").startswith("8")) | 
                                (F.col("chdrnum").startswith("CS")),
                                F.concat(F.col("chdrnum"), F.lit("_"), F.col("prodtyp"))
                            ).otherwise(
                                F.concat(F.col("chdrnum"), F.lit("_"), F.col("mbrno"), F.lit("_"), F.col("prodtyp"))
                        )).select('chdrnum','pmt_premium_rid_number').distinct()

    # fix new
    df_gpmdpf_total = (
        df_gpmdpf_source
          .groupBy("chdrnum", "billno")
          .agg(F.sum("pprem").alias("pmt_prd_premium_amount"))
    )

    df_payment_main = (df_gbihpf_source.alias('gbih')
                   .join(df_gchipf_source.alias('gch'),
                         F.trim(F.col('gbih.chdrnum')) == F.trim(F.col('gch.chdrnum')),'inner')
                  ).distinct()

    
    df_payment_main = (df_payment_main
                    .join(df_gpmdpf_life.alias('life'),
                          (F.col('gbih.chdrnum') == F.col('life.chdrnum')) &
                          (F.col('gbih.billno') == F.col('life.billno')),
                          "inner")
                        #   "left")
                  )
    
    df_payment_main = (df_payment_main
                          .join(df_gpmdpf_total.alias("tot"),
                              (F.trim(F.col('gbih.chdrnum')) == F.trim(F.col('tot.chdrnum'))) &
                              (F.trim(F.col('gbih.billno')) == F.trim(F.col('tot.billno'))),
                              "left"
                          )
                    )
    df_payment_main = df_payment_main.drop("tot.chdrnum", "tot.billno")

    df_payment_main = (df_payment_main
                   .join(df_chdrpf_main.alias('chdm'),
                         (F.trim(F.col('gbih.chdrnum')) == F.trim(F.col('chdm.chdrnum')))
                         ,'left')
                  )

    # ---------- join rider array ----------
    df_payment_main = (
        df_payment_main
          .join(
              df_gpmdpf_rider.alias("rid"),
              (F.trim(F.col('gbih.chdrnum')) == F.trim(F.col('rid.chdrnum'))) &
              (F.trim(F.col('gbih.billno')) == F.trim(F.col('rid.billno'))),
              'left'
          )
    )
    df_payment_main = df_payment_main.drop("df_gpmdpf_rider.chdrnum", "df_gpmdpf_rider.billno").distinct()



    print('Declaring Default Params Value...')
    pol_refer_code_of_company_prefix = "POL_KAL_"
    pmt_refer_code_of_company_prefix = "PMT_KAL_"
    pmt_transaction_status = "" 
    pmt_company_id = "1022"
    pol_type = "PT-05"
    pmt_type = "01"
    cust_country_code = "THA"
    insrd_pol_sub_id = 'A'
    insrd_seq = '1'
    cov_plan_seq = '1'

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = None

    NON_REQ_NULL_STR = None
    NON_REQ_NULL_NUM = None
    NON_REQ_NULL_DATE = None


    df_payment_main = df_payment_main\
        .withColumn('pmt_refer_code_of_company',
                                F.concat_ws('',
                                    F.lit(pmt_refer_code_of_company_prefix),
                                    F.trim(F.col('gbih.billno'))
                                ).cast(T.StringType())
                            )\
        .withColumn('pmt_transaction_status', lit(pmt_transaction_status).cast(T.StringType()))\
        .withColumn('pmt_company_id', lit(pmt_company_id).cast(T.StringType()))\
        .withColumn('pmt_pol_id', F.trim(F.col('gbih.chdrnum')).cast(T.StringType()))\
        .withColumn('pmt_pol_refer_code_of_company',
                                F.concat_ws('',
                                    F.lit(pol_refer_code_of_company_prefix),
                                    F.trim(F.col('gbih.chdrnum')),
                                    F.lit('_'),
                                    F.date_format(F.to_date(F.trim(F.col('gch.ccdate')).cast('string'),'yyyyMMdd'),'yyyy')
                                ).cast(T.StringType())
                            )\
        .withColumn('pmt_id', F.trim(F.col('gbih.billno')).cast(T.StringType()))\
        .withColumn('pmt_type', F.lit('01').cast(T.StringType()))\
        .withColumn('pmt_direct_premium', 
                    F.when(F.trim(F.col("gbih.nrflg")) == 'N', F.lit("01"))
                    .when(F.trim(F.col("gbih.nrflg")) == 'R', F.lit("02"))
                    .when(F.trim(F.col("gbih.nrflg")) == 'S', F.lit("03"))
                    .otherwise(F.lit(NON_REQ_NULL_STR)))\
        .withColumn('pmt_premium_payment_period_year', F.lit(1).cast(T.IntegerType()))\
        .withColumn('pmt_premium_payment_year', F.lit(1).cast(T.IntegerType()))\
        .withColumn('pmt_payment_period', F.lit('04'))

    df_payment_main = df_payment_main.withColumn("pmt_prd_premium_date",
                            _to_iso_tz7_from_yyyymmdd(F.concat(F.lit('20'), F.col('gbih.transaction_date').cast(T.StringType())))
                        )
                        
    df_payment_main = df_payment_main.withColumn("pmt_prd_premium_due_date",
                            _to_iso_tz7_from_yyyymmdd(F.date_format(F.add_months(F.to_date(F.col('gbih.transaction_date').cast("string"), "yyMMdd"), 12),"yyyyMMdd"))
                        )

    df_payment_main = df_payment_main.withColumn('payment_period_seqs',
                    F.array(
                        F.struct(
                            lit(1).cast(T.IntegerType()).alias('pmt_prd_premium_seq'),
                            lit(0).cast(T.IntegerType()).alias('pmt_prd_premium_outstanding_payment'),
                            _zero_guard_decimal(F.col('tot.pmt_prd_premium_amount').cast(T.DoubleType())).alias('pmt_prd_premium_amount'),
                            _zero_guard_decimal(lit(0.00).cast(T.DoubleType())).alias('pmt_prd_premium_amount_tax'),
                            _zero_guard_decimal(F.col('life.pmt_prd_premium_amount_life').cast(T.DoubleType())).alias('pmt_prd_premium_amount_life'),
                            _zero_guard_decimal(lit(0.00).cast(T.DoubleType())).alias('pmt_prd_premium_amt_saving'),
                            _zero_guard_decimal(lit(0.00).cast(T.DoubleType())).alias('pmt_prd_premium_amt_investment'),
                            _zero_guard_decimal(lit(0.00).cast(T.DoubleType())).alias('pmt_prd_premium_amount_other'),
                            _zero_guard_decimal(lit(0.00).cast(T.DoubleType())).alias('pmt_prd_premium_amount_com'),
                            lit(NON_REQ_NULL_NUM).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amt_interest'),
                            # lit(REQ_NULL_DEC).cast(T.DecimalType(18,2)).alias('pmt_prd_premium_amt_interest'),
                            F.col("pmt_prd_premium_date").alias("pmt_prd_premium_date"),
                            F.col("pmt_prd_premium_due_date").alias("pmt_prd_premium_due_date"),
                            lit(REQ_NULL_DATE).cast(T.StringType()).alias('pmt_prd_premium_temp_receipt_date'),
                            # F.lit('1753-01-01T00:00:00+00:00').cast(T.StringType()).alias('pmt_prd_premium_temp_receipt_date'),
                            _to_iso_tz7_from_yyyymmdd(F.concat(F.lit('20'), F.col('transaction_date').cast(T.StringType()))).alias('pmt_prd_premium_receipt_date'),
                            F.lit('').cast(T.StringType()).alias("pmt_prd_premium_temp_receipt_number"),
                            F.col('gbih.billno').cast(T.StringType()).alias("pmt_prd_premium_receipt_number"),
                            F.lit('03').cast(T.StringType()).alias("pmt_prd_premium_channel"),
                            F.lit('').cast(T.StringType()).alias("pmt_prd_premium_channel_detail"),
                            F.coalesce(F.col("payment_premium_type_riders"), F.array()).alias("payment_premium_type_riders"),
                            F.array().alias("payment_premium_type_endorsements")
                        )
                    )
                )


    print(
        '--Consolidating every elements to final Ordinary Payment Dataframe...'
    )

    output_payment_final = df_payment_main.select('pmt_refer_code_of_company'\
                                                ,'pmt_transaction_status'\
                                                ,'pmt_company_id'\
                                                ,'pmt_pol_id'\
                                                ,'pmt_pol_refer_code_of_company'\
                                                ,'pmt_id'\
                                                ,'pmt_type'\
                                                ,'pmt_direct_premium'\
                                                ,'pmt_premium_payment_period_year'\
                                                ,'pmt_premium_payment_year'\
                                                ,'pmt_payment_period'\
                                                ,'payment_period_seqs'\
                                                ).distinct()
    return output_payment_final
