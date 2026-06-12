from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

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

from pyspark.sql.types import StringType

# =========================
# Helpers
# =========================
_ZERO_GUARD_ENABLED = True
_ZERO_GUARD_EPSILON = 0.001

def _zero_guard_decimal(col_expr, precision=18, scale=2):
    dec = T.DecimalType(precision, scale)
    return F.when(col_expr.cast(dec) == F.lit(0).cast(dec),
                  F.lit(_ZERO_GUARD_EPSILON).cast(dec)).otherwise(col_expr.cast(dec))

def _to_iso_tz7_from_yyyymmdd(col_expr):
    c = F.regexp_replace(col_expr.cast(T.StringType()), r'\D', '')
    return F.when(
        (c.isNotNull()) & (F.length(c) == 8) & (c != '00000000') & (c != '99999999'),
        F.concat_ws('', F.substring(c,1,4), F.lit('-'), F.substring(c,5,2), F.lit('-'),
                    F.substring(c,7,2), F.lit('T00:00:00+07:00'))
    ).otherwise(F.lit(None).cast(T.StringType()))

def _year_from_yyyymmdd(col_expr):
    c = col_expr.cast(T.StringType())
    return F.when((c.isNotNull()) & (F.length(c) == 8) & (c != '99999999') & (c != '00000000'),
                  F.substring(c,1,4)
          ).otherwise(F.lit(None).cast(T.StringType()))

@F.udf(StringType())
def map_policy_type(cnttype):
    if cnttype is None:
        return None
    m = {
        "CHS":"PT-03","CL1":"PT-03","CR1":"PT-03","CR2":"PT-03","CR3":"PT-03","CR4":"PT-03","CR5":"PT-03","CR6":"PT-03",
        "EB1":"PT-03","GCI":"PT-03","GHP":"PT-03","GHS":"PT-03","GHT":"PT-03","GLP":"PT-03","GLT":"PT-03",
        "GPA":"PT-10","GTL":"PT-03","MRT":"PT-03","PAG":"PT-10","PAS":"PT-10","PA1":"PT-10","PA2":"PT-10","PA3":"PT-10",
        "PHS":"PT-03","PH1":"PT-03","PH2":"PT-03"
    }
    return m.get(cnttype, None)

@F.udf(StringType())
def map_g400_claim_status(gcsts):
    if gcsts is None:
        return None
    if gcsts in ("CR","CW","CV"):
        return "05"
    if gcsts in ("CA","CC"):
        return "01"
    if gcsts == "CP":
        return "03"
    return None

@F.udf(StringType())
def map_claim_payment_channel(paid_type):
    if paid_type is None:
        return "99"
    if paid_type in ("Transfer"):
        return "01"
    if paid_type in ("Cheque"):
        return "03"
    return "99"

@F.udf(StringType())
def map_health_oic_from_group(group, code):
    if group:
        g = group.strip().lower()
        if g in ("approved", "paid"):
            return "01"
        if g == "pending":
            return "03"
        if g == "declined":
            return "05"
        if g == "cancelled":
            return "04"
    # fallback by code patterns 
    if code:
        c = code.strip().upper()
        if c.startswith("P"):
            return "01"
        if c.startswith("D"):
            return "05"
        if c in ("C1","LB3","Cancelled"):
            return "04"
    return None


def group_claim_aggregation(run_params, spark_session, start_date, end_date):
    """
    Build Claim output by union of:
      1) G400 major claims (ibs_atlldta_gcmhpf)
      2) G400 health claims (ibs_claim_header WHERE bustype='GRP')

    Filtered by claim_report_date between [start_date, end_date] (YYYY-MM-DD).
    Returns DataFrame with 31 fields required by OIC.
    """

    # =====================================================
    # SECTION 1: Read Source Tables (Only required columns)
    # =====================================================
    # ---------- G400 ----------
    df_g400_claim = spark_session.table("processing_ibs.ibs_atlldta_gcmhpf").select(
        "chdrnum","clamnum","gcoccno","gcclmseq","dteclam","gcfrpdte","dateauth",
        "gcsts","gcgrspy","gcdthclm","gccauscd","gcoprscd","apaidamt",'prodtyp','bencde'
    ).filter((F.col('dteclam') >= start_date) & (F.col('dteclam') < end_date))

    df_chdrpf_filter_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf').select('chdrnum').distinct()
    df_chdrpf_ilcl_filter_source = spark_session.table('processing_ibs.ibs_atlldta_chdrpf_ilcl').select('chdrnum').distinct()
    df_chdrpf_filter_source = df_chdrpf_filter_source.unionByName(df_chdrpf_ilcl_filter_source).dropDuplicates()

    df_g400_claim = df_g400_claim\
                    .join(df_chdrpf_filter_source, df_g400_claim['chdrnum'] == df_chdrpf_filter_source['chdrnum'], 'inner') \
                    .select(df_g400_claim['*'])

    df_g400_chdr = spark_session.table("processing_ibs.ibs_atlldta_chdrpf").select("chdrnum","cnttype").distinct()
    df_g400_chdr_ilcl = spark_session.table("processing_ibs.ibs_atlldta_chdrpf_ilcl").select("chdrnum","cnttype").distinct()
    df_g400_chdr = df_g400_chdr.unionByName(df_g400_chdr_ilcl).dropDuplicates()

    # df_g400_chip = spark_session.table("processing_ibs.ibs_atlldta_gchipf").select("chdrnum","ccdate")
    df_g400_chip = (spark_session.table("processing_ibs.ibs_atlldta_gchipf").select("chdrnum","ccdate"))
    # 2026/05/11 : union df_gchipf_ilcl_source
    df_gchipf_ilcl_source = spark_session.table('processing_ibs.ibs_atlldta_gchipf_ilcl').select("chdrnum","ccdate")
    df_g400_chip = df_g400_chip.unionByName(df_gchipf_ilcl_source).dropDuplicates()

    df_g400_chip = (spark_session.table("processing_ibs.ibs_atlldta_gchipf")
        .select(F.col("chdrnum").cast("string").alias("chip_chdrnum"), F.col("ccdate").cast("string").alias("chip_ccdate"))
        .withColumn("ccdate_valid",
            F.when(
                (F.length("chip_ccdate")==8) &
                (~F.col("chip_ccdate").isin("00000000", "99999999")),
                F.col("chip_ccdate")
            )
        )
        .groupBy("chip_chdrnum")
        .agg(F.max("ccdate_valid").alias("max_ccdate"))
    )

    # df_g400_bnfl = spark_session.table("processing_ibs.ibs_atlldta_bnflpf").select("chdrnum","prodtyp","bencde","amtyear","amtday","amtvis")
    df_g400_gcre = spark_session.table("processing_ibs.ibs_atlldta_gcrepf").select("clamnum","seqno","resvdate","cumpaid")
    df_g400_gcpy = spark_session.table("processing_ibs.ibs_atlldta_gcpypf").select("clamnum","gcoccno","pay_type","payclt")
    df_g400_gmhd = spark_session.table("processing_ibs.ibs_atlldta_gmhdpf").select("chdrnum","clntnum","dtetrm")
    # add _ilcl
    df_gmhdpf_ilcl = spark_session.table('processing_ibs.ibs_atlldta_gmhdpf_ilcl').select("chdrnum","clntnum","dtetrm")
    df_g400_gmhd = df_g400_gmhd.unionByName(df_gmhdpf_ilcl).dropDuplicates()

    df_g400_clnt = spark_session.table("processing_ibs.ibs_atlldta_clntpf").select("clntnum","secuityno","lgivname","lsurname","validflag")
    df_ref_g400_cov = spark_session.table("processing_ibs.ibs_ref_g400_product_benefit_code").select("prodtyp","bencde","oic_code","longdesc2")
    # NEW: death cause & reject reason refs
    df_ref_g400_death = spark_session.table("processing_ibs.ibs_ref_g400_claim_causes_of_death")\
        .select("gccauscd","causes_oic_code")
    df_ref_g400_reject = spark_session.table("processing_ibs.ibs_ref_g400_claim_reject_reason")\
        .select("desctabl","descitem","oic_code","validflag")\
        .where((F.col("desctabl")=="T9699") & (F.col("validflag")=='1'))
    
    df_ref_oic_claim_coverage = spark_session.table("processing_ibs.ibs_ref_oic_claim_coverage")

    # ---------- Health (GRP only) ----------
    # filter by date
    df_h_claim_header = spark_session.table("processing_ibs.ibs_claim_header").select(
        "notifyno","pol_no","notifydate","acc_date","admitdate","dischdate",
        "hosp_code","hosp_name","clm_cstatus","assess_date","ld_mainstatus",
        "bustype","actualbill", "submit_date", "cust_id"
    ).filter((F.col('notifydate') >= start_date) & (F.col('notifydate') < end_date))

    df_h_claim_status = spark_session.table("processing_ibs.ibs_claim_status").select("claimsts","claimstsgroup")

    # filter by date
    df_h_cdp = spark_session.table("processing_ibs.ibs_claim_details_payment")\
        .select("notifyno","benefitid","maxbendtmamount","actualamount","netapproveamount")\
        .filter((F.col('notifydate') >= start_date) & (F.col('notifydate') < end_date))

    df_h_benefit_mas = spark_session.table("processing_ibs.ibs_benefit_master")\
        .select("benefit_cd","benefit_th_name","benefit_group_name","limit_type_cd","benefit_limit")

    # filter by date
    df_h_payment = spark_session.table("processing_ibs.ibs_payment_details")\
        .select("notifyno","paymentno","paid_date","paid_type","payee_name")\
        .filter((F.col('notifydate') >= start_date) & (F.col('notifydate') < end_date))

    df_ref_health_cov = spark_session.table("processing_ibs.ibs_ref_coverage_health")\
        .select("map_benefit_th_name","map_benefit_group_name","map_benefit_oic_code")
    
    df_ref_health_reject = spark_session.table("processing_ibs.ibs_ref_health_claim_reject_reason").select("claimsts","oic_code")

    # =====================================================
    # SECTION 2: Transform & Merge
    # =====================================================

    REQ_NULL_STR = ''
    REQ_NULL_INT = 0
    REQ_NULL_DEC = 0.00
    REQ_NULL_DATE = None

    NON_REQ_NULL_STR = None
    NON_REQ_NULL_NUM = None
    NON_REQ_NULL_DATE = None

    # ---------- 2.1 G400 (major) : Header ----------
    g400_base = (
        df_g400_claim.alias("g")
        .join(df_g400_chdr.alias("c"), on="chdrnum", how="left")
        # .join(df_g400_chip.alias("p"), on="chdrnum", how="left")
        .join(df_g400_chip.alias("p"), on=F.col("g.chdrnum")==F.col("p.chip_chdrnum"),how="left")
        # join refs for mapping
        .join(df_ref_g400_death.alias("cd"), on=F.col("g.gccauscd")==F.col("cd.gccauscd"), how="left")
        .join(df_ref_g400_reject.alias("rr"), on=F.col("g.gcoprscd")==F.col("rr.descitem"), how="left")
    ).distinct()


    g400_base = g400_base.withColumn(
        "claim_pol_refer_code_of_company",
        F.when(_year_from_yyyymmdd(F.col("p.max_ccdate")).isNotNull(),
               F.concat(F.lit("POL_KAL_"), F.col("g.chdrnum").cast("string"), F.lit("_"),
                        _year_from_yyyymmdd(F.col("p.max_ccdate"))))
         .otherwise(F.concat(F.lit("POL_KAL_"), F.col("g.chdrnum").cast("string")))
    ).withColumn(
        "claim_refer_code_of_company",
        F.concat(F.lit("CLM_KAL_"),
                 F.col("clamnum").cast("string"), F.lit("_"),
                 F.col("gcoccno").cast("string"), F.lit("_"),
                 F.col("gcclmseq").cast("string"))
    ).withColumn("claim_incurred_date",  _to_iso_tz7_from_yyyymmdd(F.col("dteclam"))
    ).withColumn("claim_report_date",    _to_iso_tz7_from_yyyymmdd(F.col("gcfrpdte"))
    ).withColumn("claim_close_date",     _to_iso_tz7_from_yyyymmdd(F.col("dateauth"))
    ).withColumn("claim_completion_date", _to_iso_tz7_from_yyyymmdd(F.col("dteclam"))
    ).withColumn("claim_status", map_g400_claim_status(F.col("gcsts"))
    ).withColumn(
        "claim_causes_of_death",
        F.when(F.col("g.gcdthclm")=='Y', F.col("cd.causes_oic_code").cast("string"))
         .otherwise(F.lit("98"))
    )

    g400_header = (
        g400_base
        .withColumn("claim_transaction_status", F.lit(""))
        .withColumn("claim_company_id", F.lit("1022"))
        .withColumn("claim_pol_id", F.col("chdrnum").cast("string"))
        .withColumn("claim_id", F.col("clamnum").cast("string"))
        .withColumn("claim_no", F.col("clamnum").cast("string"))
        .withColumn("claim_type", F.lit("01"))
        .withColumn("claim_pol_type", map_policy_type(F.col("c.cnttype")))
        .withColumn("claim_notify_date", F.lit(None).cast("string"))
        .withColumn("claim_discharge_date", F.lit(None).cast("string"))
        .withColumn("claim_hospital_id", F.lit(None).cast("string"))
        .withColumn("claim_hospital_name", F.lit(None).cast("string"))
        .withColumn("claim_doctor_name", F.lit(None).cast("string"))
        # NEW: use mapped OIC reject code (rr.oic_code) when claim_status='05'
        .withColumn("claim_reject_reason",
            F.when(F.col("claim_status")=='05', F.col("rr.oic_code").cast("string"))
             .otherwise(F.lit(None).cast("string"))
        )
        .withColumn("claim_document_link", F.lit(None).cast("string"))
        .withColumn("claim_oic_case_id", F.lit(None).cast("string"))
        .withColumn("claim_oic_case_detail", F.lit(None).cast("string"))
        .withColumn("claim_request_channel", F.lit(None).cast("string"))
    )

    # ---------- 2.2 G400 : claim_insrd_ids ----------
    w_gmhd_latest = Window.partitionBy("gm.chdrnum","gm.clntnum").orderBy(F.col("gm.dtetrm").cast("string").desc())
    g400_insrd = (
        df_g400_gmhd.alias("gm")
        .join(df_g400_clnt.alias("cl"), on=F.col("gm.clntnum")==F.col("cl.clntnum"), how="left")
        .where(F.col("cl.validflag")=='1')
        .withColumn("rn", F.row_number().over(w_gmhd_latest))
        .where(F.col("rn")==1)
        .withColumn("insrd_id_tmp",
            F.when(F.col("cl.secuityno").isNotNull() & (F.col("cl.secuityno")!=''),
                   F.col("cl.secuityno").cast("string"))
             .otherwise(F.concat_ws(" ", F.col("cl.lgivname"), F.col("cl.lsurname")))
        )
        .groupBy("gm.chdrnum")
        .agg(F.array_distinct(F.collect_list(F.col("insrd_id_tmp"))).alias("claim_insrd_ids"))
    )


    g400_header = g400_header.join(g400_insrd.alias('g400_insrd'), on="chdrnum", how="inner")\
                             .withColumn("claim_insrd_ids", F.coalesce(F.col("g400_insrd.claim_insrd_ids"), F.array()))

    g400_cov = (
        df_g400_claim.alias("g")
        .join(df_ref_g400_cov.alias("m"),
              on=[F.col("g.prodtyp")==F.col("m.prodtyp"),F.col("g.bencde")==F.col("m.bencde")],how="left")
        .join(df_ref_oic_claim_coverage.alias("ccd"),
              on=[F.col("m.oic_code")== F.col("ccd.oic_code")],how="left")
        .withColumn("claim_coverage_code",
            F.when(F.col("m.oic_code").isNotNull(), F.col("m.oic_code"))
             .otherwise(F.concat_ws("-", F.col("g.prodtyp"), F.col("g.bencde")))
        )
        # .withColumn("claim_coverage_detail", F.col("m.longdesc2") )
        .withColumn("claim_coverage_detail", F.coalesce(F.col("ccd.description_thai"),F.col("m.longdesc2")) )
        .withColumn("claim_sum_insured_per_coverage_per_year",_zero_guard_decimal(F.col("g.gcgrspy").cast(T.DecimalType(18,2))))
        .withColumn("claim_sum_insured_per_coverage_per_time",_zero_guard_decimal(F.col("g.gcgrspy").cast(T.DecimalType(18,2))))
        .withColumn("claim_coverage_amt", _zero_guard_decimal(F.col("g.gcgrspy").cast(T.DecimalType(18,2))))
        .withColumn("claim_deductible_amt", F.lit(NON_REQ_NULL_NUM))
        .withColumn("claim_amount", _zero_guard_decimal(F.col("g.gcgrspy").cast(T.DecimalType(18,2))))
        .withColumn("simb2s", F.array())
        .groupBy("clamnum")
        .agg(F.collect_list(
            F.struct(
                "claim_coverage_code",
                "claim_coverage_detail",
                "claim_sum_insured_per_coverage_per_year",
                "claim_sum_insured_per_coverage_per_time",
                "claim_coverage_amt",
                "claim_deductible_amt",
                "claim_amount",
                "simb2s"
            )
        ).alias("claim_coverages"))
    )

    # g400_header = g400_header.join(g400_cov.select("clamnum","claim_coverages"), on="clamnum", how="left")\
    g400_header = g400_header.join(g400_cov.select("clamnum","claim_coverages"), on="clamnum", how="inner")\
                             .withColumn("claim_coverages", F.coalesce(F.col("claim_coverages"), F.array())).distinct()
    

    # ---------- 2.4 G400 : Estimates ----------
    w_gcre = Window.partitionBy("clamnum").orderBy(F.col("seqno").desc())
    g400_est = (
        df_g400_gcre
        .withColumn("rn", F.row_number().over(w_gcre))
        .where(F.col("rn")==1)
        .withColumn("claim_estimate_seq", F.lit(1).cast("int"))
        .withColumn("claim_estimate_date", _to_iso_tz7_from_yyyymmdd(F.col("resvdate")))
        .withColumn("claim_estimate_amount", _zero_guard_decimal(F.col("cumpaid").cast(T.DecimalType(18,2))))
        .groupBy("clamnum")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("claim_estimate_seq").alias("claim_estimate_seq"),
                    F.col("claim_estimate_date").alias("claim_estimate_date"),
                    F.col("claim_estimate_amount").alias("claim_estimate_amount"),
                )
            ).alias("estimates")
        )
    )
    g400_header = g400_header.join(g400_est.select("clamnum","estimates"), on="clamnum", how="left")\
                             .withColumn("estimates", F.coalesce(F.col("estimates"), F.array()))

    # ---------- 2.5 G400 : Payments ----------
    g400_pay_base = (
        df_g400_claim.alias("g")
        .join(df_g400_gcpy.alias("p"), on=F.col("g.clamnum")==F.col("p.clamnum"), how="left")
        .withColumn("has_payment", F.col("g.apaidamt").cast("double") != 0)
    )
    g400_benef_name = (
        df_g400_gcpy.alias("gp")
        .join(df_g400_clnt.alias("cl"), on=F.col("gp.payclt")==F.col("cl.clntnum"), how="left")
        .groupBy("gp.clamnum")
        .agg(F.array_join(F.collect_list(F.concat_ws(" ", F.col("cl.lgivname"), F.col("cl.lsurname"))), ", ").alias("benef_names"))
    )
    g400_pay = (
        g400_pay_base
        .join(g400_benef_name, on="clamnum", how="left")
        .where(F.col("has_payment"))
        .withColumn("claim_payment_id",
            F.concat_ws("_", F.col("g.clamnum").cast("string"), F.col("g.gcoccno").cast("string"), F.col("g.gcclmseq").cast("string")))
        .withColumn("claim_payment_type", F.lit("01"))
        .withColumn("claim_payment_term", F.lit(1).cast("int"))
        .withColumn("claim_payment_term_paid", F.lit(1).cast("int"))
        .withColumn("claim_payment_channel",
            F.when(F.col("p.pay_type").isNull(), F.lit("99"))
             .when(F.col("p.pay_type")=='C', F.lit("03"))
             .otherwise(F.lit("01"))
        )
        .withColumn("claim_payment_date", _to_iso_tz7_from_yyyymmdd(F.col("g.dateauth")))
        .withColumn("claim_payment_account_date", _to_iso_tz7_from_yyyymmdd(F.col("g.dateauth")))
        .withColumn("claim_payment_invoice_amt", _zero_guard_decimal(F.col("g.apaidamt").cast(T.DecimalType(18,2))))
        .withColumn("claim_payment_amt",         _zero_guard_decimal(F.col("g.apaidamt").cast(T.DecimalType(18,2))))
        .withColumn("claim_payment_other_invoice_amt", F.lit(None).cast(T.DecimalType(18,2)))
        .withColumn("claim_payment_beneficiary_name", F.col("benef_names"))
        .groupBy("g.clamnum")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("claim_payment_id").alias("claim_payment_id"),
                    F.col("claim_payment_type").alias("claim_payment_type"),
                    F.col("claim_payment_term").alias("claim_payment_term"),
                    F.col("claim_payment_term_paid").alias("claim_payment_term_paid"),
                    F.col("claim_payment_channel").alias("claim_payment_channel"),
                    F.col("claim_payment_date").alias("claim_payment_date"),
                    F.col("claim_payment_account_date").alias("claim_payment_account_date"),
                    F.col("claim_payment_invoice_amt").alias("claim_payment_invoice_amt"),
                    F.col("claim_payment_amt").alias("claim_payment_amt"),
                    F.col("claim_payment_other_invoice_amt").alias("claim_payment_other_invoice_amt"),
                    F.col("claim_payment_beneficiary_name").alias("claim_payment_beneficiary_name"),
                )
            ).alias("claim_payments")
        )
    )
    g400_header = g400_header.join(g400_pay.select("clamnum","claim_payments"), on="clamnum", how="left")\
                             .withColumn("claim_payments", F.coalesce(F.col("claim_payments"), F.array()))

    # ICD codes (OTHER99)
    g400_header = g400_header.withColumn("claim_icd_10_codes", F.array(F.lit("OTHER99")))\
                             .withColumn("claim_icd_9_codes",  F.array(F.lit("OTHER99")))

    # =====================================================
    # HEALTH (HCS – GRP only)
    # =====================================================
    h_base = (
        df_h_claim_header.alias("h")
        .where(F.col("h.bustype")=='GRP')
        .join(df_h_claim_status.alias("s"), on=F.col("h.clm_cstatus")==F.col("s.claimsts"), how="left")
        .join(df_g400_chdr.alias("c"), on=F.col("h.pol_no")==F.col("c.chdrnum"), how="left")
        .join(df_g400_chip.alias("py"), on=F.col("h.pol_no")==F.col("py.chip_chdrnum"), how="left")
        # normalize
        .withColumn("pol_no_norm", F.trim(F.col("h.pol_no").cast("string")))
        .withColumn("notifyno_norm", F.trim(F.col("h.notifyno").cast("string")))
    )
    h_base = (
        h_base
        .join(df_ref_health_reject.alias("ref_h_reject"), on=F.col("ref_h_reject.claimsts")==F.col("s.claimsts"), how="left")
        .withColumn("claim_refer_code_of_company", F.concat(F.lit("CLM_"), F.col("notifyno_norm")).cast("string"))
        .withColumn("claim_pol_refer_code_of_company", F.concat(F.lit("POL_KAL_"), F.col("h.pol_no").cast("string")))
        .withColumn("claim_pol_refer_code_of_company",
            F.when(F.col("py.max_ccdate").isNotNull(), F.concat(F.lit("POL_KAL_"),F.col("h.pol_no").cast("string"),F.lit("_"),F.substring(F.col("py.max_ccdate"), 1, 4))
            ).otherwise(F.concat(F.lit("POL_KAL_"),F.col("h.pol_no").cast("string")))
        )
        .withColumn("claim_incurred_date",
            F.when(F.col("h.acc_date").isNotNull() | (F.col("h.acc_date").cast("string")!='0'), _to_iso_tz7_from_yyyymmdd(F.col("h.acc_date")))
             .otherwise(_to_iso_tz7_from_yyyymmdd(F.col("h.admitdate")))
        )
        .withColumn("claim_report_date",     _to_iso_tz7_from_yyyymmdd(F.col("h.notifydate")))
        .withColumn("claim_completion_date", _to_iso_tz7_from_yyyymmdd(F.col("h.assess_date")))
        .withColumn("claim_close_date",
            F.when(F.col("s.claimstsgroup").isNotNull() & (F.lower(F.col("s.claimstsgroup")) != 'pending'), _to_iso_tz7_from_yyyymmdd(F.col("h.ld_mainstatus")))
             .otherwise(F.lit(None).cast("string"))
        )
        # map OIC status from group
        # .withColumn("claim_status", map_health_oic_from_group(F.col("s.claimstsgroup")))
        .withColumn("claim_status", map_health_oic_from_group(F.col("s.claimstsgroup"), F.col("h.clm_cstatus")))
        # reject reason: only Declined
        .withColumn("claim_reject_reason",
            F.when(F.col("claim_status")=='05', F.col("ref_h_reject.oic_code")).otherwise(F.lit('99').cast("string"))
        )
        # .withColumn("claim_hospital_id",   F.col("h.hosp_code").cast("string"))
        .withColumn("claim_hospital_id",   F.lit(None).cast("string"))
        # .withColumn("claim_hospital_name", F.col("h.hosp_name").cast("string"))
        .withColumn("claim_hospital_name", F.lit(None).cast("string"))
        .withColumn("claim_notify_date",   _to_iso_tz7_from_yyyymmdd(F.col("h.admitdate")))
        .withColumn("claim_discharge_date",_to_iso_tz7_from_yyyymmdd(F.col("h.dischdate")))
    )
    h_header = (
        h_base
        .withColumn("claim_transaction_status", F.lit(""))
        .withColumn("claim_company_id", F.lit("1022"))
        # .withColumn("claim_pol_id", F.col("h.pol_no").cast("string"))
        .withColumn("claim_pol_id", F.col("pol_no_norm").cast("string"))
        # .withColumn("claim_id", F.col("h.notifyno").cast("string"))
        .withColumn("claim_id", F.col("notifyno_norm").cast("string"))
        # .withColumn("claim_no", F.col("h.notifyno").cast("string"))
        .withColumn("claim_no", F.col("notifyno_norm").cast("string"))
        .withColumn("claim_type", F.lit("01"))
        # .withColumn("claim_pol_type", F.lit(None).cast("string"))
        .withColumn("claim_pol_type", map_policy_type(F.col("c.cnttype")))
        .withColumn("claim_doctor_name", F.lit(None).cast("string"))
        .withColumn("claim_document_link", F.lit(None).cast("string"))
        .withColumn("claim_causes_of_death", F.lit("98").cast("string"))
        .withColumn("claim_oic_case_id", F.lit(None).cast("string"))
        .withColumn("claim_oic_case_detail", F.lit(None).cast("string"))
        .withColumn("claim_request_channel", F.lit(None).cast("string"))
        .withColumn("claim_icd_10_codes", F.array(F.lit("OTHER99")))
        .withColumn("claim_icd_9_codes",  F.array(F.lit("OTHER99")))
    )

    h_insrd = (
        df_h_claim_header
        .select('notifyno','cust_id')
        .groupBy("notifyno")
        .agg(F.array_distinct(F.collect_list(F.col("cust_id"))).alias("claim_insrd_ids"))
    )

    h_header = h_header.join(h_insrd.alias('h_insrd'), on=F.col("h.notifyno")==F.col("h_insrd.notifyno"), how="inner")\
                             .withColumn("claim_insrd_ids", F.coalesce(F.col("h_insrd.claim_insrd_ids"), F.array()))
    

    coverage_struct_schema = T.StructType([
        T.StructField("claim_coverage_code", T.StringType()),
        T.StructField("claim_coverage_detail", T.StringType()),
        T.StructField("claim_sum_insured_per_coverage_per_year", T.DecimalType(18,2)),
        T.StructField("claim_sum_insured_per_coverage_per_time", T.DecimalType(18,2)),
        T.StructField("claim_coverage_amt", T.DecimalType(18,2)),
        T.StructField("claim_deductible_amt", T.DecimalType(18,2)),
        T.StructField("claim_amount", T.DecimalType(18,2)),
        T.StructField("simb2s", T.ArrayType(T.StringType())),
    ])

    coverage_array_schema = T.ArrayType(coverage_struct_schema)

    EMPTY_COVERAGE_ARRAY = F.from_json(F.lit("[]"), coverage_array_schema)

    h_cov_norm = (
        df_h_cdp.alias("d")
        .join(df_h_claim_header.alias("h"),
              F.col("d.notifyno")==F.col("h.notifyno"), "left")
        .where(F.col("h.bustype")=="GRP")
        .join(
            df_h_benefit_mas.alias("bm"),
            F.expr("cast(regexp_extract(bm.benefit_cd,'[0-9]+',0) as int) = cast(d.benefitid as int)"),
            "left"
        )
        .join(
            df_ref_health_cov.alias("rm"),
            [
                F.col("bm.benefit_th_name")==F.col("rm.map_benefit_th_name"),
                F.col("bm.benefit_group_name")==F.col("rm.map_benefit_group_name")
            ],
            "left"
        )
        .withColumn("sum_insured_normal", F.round(F.col("d.maxbendtmamount").cast("double"), 2))
        .withColumn("netapprove_normal", F.round(F.col("d.netapproveamount").cast("double"), 2))
        .withColumn("actual_normal", F.round(F.col("d.actualamount").cast("double"), 2))
    )

    h_cov_group = (
        h_cov_norm
        .groupBy("d.notifyno", "rm.map_benefit_oic_code")
        .agg(
            F.first("bm.benefit_th_name").alias("benefit_detail"),
            F.first("sum_insured_normal").alias("sum_insured"),
            F.first("netapprove_normal").alias("cov_amt"),
            F.first("actual_normal").alias("act_amt"),
            F.first("bm.limit_type_cd").alias("limit_type_cd")
        )
    )

    h_cov = (
        h_cov_group
        .withColumn("claim_coverage_code", F.col("map_benefit_oic_code"))
        .withColumn("claim_coverage_detail", F.col("benefit_detail"))
        .withColumn(
            "claim_sum_insured_per_coverage_per_year",
            F.when(F.col("limit_type_cd").isin("AY","ADA"), 
                   F.col("sum_insured").cast("decimal(18,2)"))
                   .otherwise(_zero_guard_decimal(F.lit(0)))
        )
        .withColumn(
            "claim_sum_insured_per_coverage_per_time",
            F.when(F.col("limit_type_cd").isin("AD","APT"), 
                   F.col("sum_insured").cast("decimal(18,2)"))
                   .otherwise(_zero_guard_decimal(F.lit(0)))
        )
        .withColumn("claim_coverage_amt", F.col("cov_amt").cast("decimal(18,2)"))
        .withColumn("claim_deductible_amt", F.lit(None).cast("decimal(18,2)"))
        .withColumn("claim_amount", F.col("act_amt").cast("decimal(18,2)"))
        .withColumn("simb2s", F.array().cast(T.ArrayType(T.StringType())))
        .groupBy("notifyno")
        .agg(
            F.collect_list(
                F.struct(
                    "claim_coverage_code",
                    "claim_coverage_detail",
                    "claim_sum_insured_per_coverage_per_year",
                    "claim_sum_insured_per_coverage_per_time",
                    "claim_coverage_amt",
                    "claim_deductible_amt",
                    "claim_amount",
                    "simb2s"
                )
            ).alias("claim_coverages")
        )
    )

    h_header = h_header.join(h_cov.select(F.col("notifyno").alias("key_no"), "claim_coverages"),
                            #   on=F.col("h.notifyno")==F.col("key_no"), how="left")\
                              on=F.col("h.notifyno")==F.col("key_no"), how="inner")\
                       .drop("key_no")\
                       .withColumn("claim_coverages", F.coalesce(F.col("claim_coverages"), F.array()))
    

    # Health : Estimates
    h_est = (
        df_h_claim_header
        .withColumn("claim_estimate_seq", F.lit(1).cast("int"))
        .withColumn("claim_estimate_date", F.when(_to_iso_tz7_from_yyyymmdd(F.col("submit_date")).isNotNull(), _to_iso_tz7_from_yyyymmdd(F.col("submit_date")))
                                            .otherwise(F.col("notifyno")))
        .withColumn("claim_estimate_amount", _zero_guard_decimal(F.col("actualbill").cast(T.DecimalType(18,2))))
        .groupBy("notifyno")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("claim_estimate_seq").alias("claim_estimate_seq"),
                    F.col("claim_estimate_date").alias("claim_estimate_date"),
                    F.col("claim_estimate_amount").alias("claim_estimate_amount"),
                )
            ).alias("estimates")
        )
    )

    h_header = h_header.join(h_est.select(F.col("notifyno").alias("key_no"), "estimates"),
                              on=F.col("h.notifyno")==F.col("key_no"), how="left")\
                       .drop("key_no")\
                       .withColumn("estimates", F.coalesce(F.col("estimates"), F.array()))

    # Health : Payments
    h_pay = (
        df_h_payment.alias("p")
        .join(df_h_claim_header.alias("h"), on=F.col("p.notifyno")==F.col("h.notifyno"), how="left")
        .where(F.col("h.bustype")=='GRP')
        .withColumn("claim_payment_id", F.col("p.paymentno").cast("string"))
        .withColumn("claim_payment_type", F.lit("01"))
        .withColumn("claim_payment_term", F.lit(1).cast("int"))
        .withColumn("claim_payment_term_paid", F.lit(1).cast("int"))
        # .withColumn("claim_payment_channel", F.col("p.paid_type"))
        .withColumn("claim_payment_channel", map_claim_payment_channel(F.col("p.paid_type")))
        .withColumn("claim_payment_date", _to_iso_tz7_from_yyyymmdd(F.col("p.paid_date")))
        .withColumn("claim_payment_account_date", _to_iso_tz7_from_yyyymmdd(F.col("p.paid_date")))
        .withColumn("claim_payment_invoice_amt", _zero_guard_decimal(F.col("h.actualbill").cast(T.DecimalType(18,2))))
        .withColumn("claim_payment_amt",         _zero_guard_decimal(F.col("h.actualbill").cast(T.DecimalType(18,2))))
        .withColumn("claim_payment_other_invoice_amt", F.lit(None).cast(T.DecimalType(18,2)))
        .withColumn("claim_payment_beneficiary_name", F.col("p.payee_name").cast("string"))
        .groupBy("p.notifyno")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("claim_payment_id").alias("claim_payment_id"),
                    F.col("claim_payment_type").alias("claim_payment_type"),
                    F.col("claim_payment_term").alias("claim_payment_term"),
                    F.col("claim_payment_term_paid").alias("claim_payment_term_paid"),
                    F.col("claim_payment_channel").alias("claim_payment_channel"),
                    F.col("claim_payment_date").alias("claim_payment_date"),
                    F.col("claim_payment_account_date").alias("claim_payment_account_date"),
                    F.col("claim_payment_invoice_amt").alias("claim_payment_invoice_amt"),
                    F.col("claim_payment_amt").alias("claim_payment_amt"),
                    F.col("claim_payment_other_invoice_amt").alias("claim_payment_other_invoice_amt"),
                    F.col("claim_payment_beneficiary_name").alias("claim_payment_beneficiary_name"),
                )
            ).alias("claim_payments")
        )
    )
    h_header = h_header.join(h_pay.select(F.col("notifyno").alias("key_no"), "claim_payments"),
                              on=F.col("h.notifyno")==F.col("key_no"), how="left")\
                       .drop("key_no")\
                       .withColumn("claim_payments", F.coalesce(F.col("claim_payments"), F.array()))


    # =====================================================
    # SECTION 3: Union & Final Select
    # =====================================================

    final_cols = [
        'claim_refer_code_of_company',
        'claim_transaction_status',
        'claim_company_id',
        'claim_pol_id',
        'claim_pol_refer_code_of_company',
        'claim_id',
        'claim_no',
        'claim_type',
        'claim_pol_type',
        'claim_incurred_date',
        'claim_report_date',
        'claim_notify_date',
        'claim_discharge_date',
        'claim_hospital_id',
        'claim_hospital_name',
        'claim_doctor_name',
        'claim_completion_date',
        'claim_close_date',
        'claim_status',
        'claim_reject_reason',
        'claim_document_link',
        'claim_causes_of_death',
        'claim_oic_case_id',
        'claim_oic_case_detail',
        'claim_request_channel',
        'claim_insrd_ids',
        'claim_icd_10_codes',
        'claim_icd_9_codes',
        'claim_coverages',
        'estimates',
        'claim_payments'
    ]

    output_df = g400_header.select(*final_cols).unionByName(h_header.select(*final_cols)).select(*final_cols)
    return output_df