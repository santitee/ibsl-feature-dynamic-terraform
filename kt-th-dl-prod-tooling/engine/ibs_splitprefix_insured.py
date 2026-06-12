import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import re
import ibs_utilities as ibs_utils


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_params = ibs_utils.get_run_parameters(sys.argv)

s3_bucket = run_params.args['BUCKET_NAME']

def replaceUdf(column):
    # column = column.encode("utf-8")
    # column = re.sub("(\.)", "\\\\.", column)
    for i in range(len(encode_namesCollect)):
        if encode_namesCollect[i].lower() in column.lower(): 
            remove_prefix = column.replace(encode_namesCollect[i], "")
            # remove_prefix = re.sub("(\\\\.)", ".", remove_prefix)
            break
        else:
            remove_prefix = column
            # remove_prefix = re.sub("(\\\\.)", ".", remove_prefix)
    return remove_prefix

def splitprefixUdf(column):
    # column = column.encode("utf-8")
    # column = re.sub("(\.)", "\\\\.", column)
    for i in range(len(encode_namesCollect)):
        if encode_namesCollect[i].lower() in column.lower(): 
            # prefix = re.sub("(\\\\.)", ".", encode_namesCollect[i])
            prefix = encode_namesCollect[i]
            break
        else:
            prefix = "ERROR"
    return prefix
    
print("--Reading source_table & prefix_table...")
source_table = "processing_ibs.ibs_lfkludta_lfppml"
prefix_table = "processing_ibs.ibs_tblcustomer_nameprefix"
tblCustomer_NameMasterByPNO = spark.table(source_table)
df_prefix = spark.table(prefix_table)

print("--Clean prefix name (encode) and sort lists from max len...")
df_prefix = df_prefix.select("abbv")
# df_prefix = df_prefix.withColumn("abbv_cleaned", regexp_replace(col("abbv"), r"\.", "\\\\."))
# df_prefix = df_prefix.drop("abbv")
# df_prefix = df_prefix.withColumnRenamed("abbv_cleaned", "abbv")
df_prefix_len = df_prefix.withColumn("trim_len_col",length(trim(col("abbv"))))
sort_index = df_prefix_len.sort(col("trim_len_col").desc())
sort_index = sort_index.drop("trim_len_col")
encode_namesCollect = [row.abbv for row in sort_index.collect()]

print("--Define reg_replaceUdf udf function")
reg_replaceUdf = udf(replaceUdf, StringType())
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn('nameonly', reg_replaceUdf(col('pnamf')))
print("--Define reg_splitUdf udf function")
reg_splitUdf = udf(splitprefixUdf, StringType())
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn('prefix_only', reg_splitUdf(col('pnamf')))

print("--Transformation - Prefix names...")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("prefix", when((col("prefix_only")=="")|(col("prefix_only").isNull()), "ERROR").otherwise(col("prefix_only")))\
                                                            .withColumn("fullname", concat(tblCustomer_NameMasterByPNO['nameonly'],lit(" "),tblCustomer_NameMasterByPNO['pname']))
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.select("PNO", 
                                                                "PDOI", 
                                                                "LPLAN",
                                                                ltrim(rtrim(tblCustomer_NameMasterByPNO.pid)).alias("PID"),
                                                                "PNAMF",
                                                                "PNAME",
                                                                tblCustomer_NameMasterByPNO.prefix.alias("PREFIX"),
                                                                ltrim(rtrim(tblCustomer_NameMasterByPNO.nameonly)).alias("NameOnly"),
                                                                ltrim(rtrim(tblCustomer_NameMasterByPNO.fullname)).alias("FullName"),
                                                                trim(concat(tblCustomer_NameMasterByPNO['NameOnly'],lit(""),tblCustomer_NameMasterByPNO['pname'])).alias("SearchString"),
                                                                "PSEX",
                                                                "PDOB",  
                                                                "PMRSTU",     
                                                                "PADR1",  
                                                                "PADR2",
                                                                "PADR3",
                                                                "PADR4",
                                                                "PSTU",
                                                                "PDOT",
                                                                "pcty")
                                                                
print("--Transformation - Zipcode...")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("ZipCode", regexp_extract(rtrim(concat(col("PADR1"), lit(" "), col("PADR2"), lit(" "),col("PADR3"), lit(" "),col("PADR4"))), "(\d{5})", 1))
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("ZipCode_Temp", 
                                                                        when(tblCustomer_NameMasterByPNO.ZipCode == '00000', '')
                                                                        .when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), substring("PNAMF", 8, 4))
                                                                        .otherwise(tblCustomer_NameMasterByPNO.ZipCode))\
                                                            .drop("ZipCode")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumnRenamed("ZipCode_Temp", "ZipCode")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("ZipCode_Temp", 
                                                                        when(col("ZipCode").rlike("(.*[^0-9].*)"), "")
                                                                        .otherwise(tblCustomer_NameMasterByPNO.ZipCode))\
                                                            .drop("ZipCode")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumnRenamed("ZipCode_Temp", "ZipCode")

print("--Transformation - ZipProvince...")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("ZipProvince", 
                                                                        when(length("zipcode")==5, 
                                                                            when(substring("zipcode",1,1) == '1', substring("zipcode", 1, 3))
                                                                            .when((col("zipcode") != '') & (substring("zipcode",1,1) != '1'), substring("zipcode", 1, 2))
                                                                            .otherwise(''))
                                                                        .when((length("zipcode")<5), substring("zipcode", 1, 1)))
print("--Transformation - Province & Regional...")
df_ct_amphor = spark.table("processing_ibs.ibs_ct_amphor")
df_ct_changwat = spark.table("processing_ibs.ibs_ct_changwat")
df_ct_tambol = spark.table("processing_ibs.ibs_ct_tambol")
df_ct_amphor = df_ct_amphor.select("AMP_CHW_CD", "amp_postcode").dropDuplicates()
df_ct_changwat = df_ct_changwat.select("CHW_REG","CHW_T_NM", "CHW_CD").dropDuplicates()
df_ct_tambol = df_ct_tambol.select("THM_CHW_CD", "thm_postcode").dropDuplicates()
df_dimcitycode = spark.table("processing_ibs.ibs_dimcitycode")
df_dimcitycode = df_dimcitycode.select("pcty", "province", "region")
print("--Transformation - Province & Regional: Join table (df_ct_amphor, df_ct_changwat, df_ct_tambol)...")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.join(df_ct_amphor, tblCustomer_NameMasterByPNO.ZipCode == df_ct_amphor.amp_postcode, "left")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.join(df_ct_changwat, tblCustomer_NameMasterByPNO.AMP_CHW_CD == df_ct_changwat.CHW_CD, "left")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.join(df_ct_tambol, tblCustomer_NameMasterByPNO.ZipCode == df_ct_tambol.thm_postcode, "left")
df_ct_changwat_temp = df_ct_changwat \
                .withColumnRenamed("CHW_REG", "CHW_REG_temp") \
                .withColumnRenamed("CHW_T_NM", "CHW_T_NM_temp") \
                .withColumnRenamed("CHW_CD", "CHW_CD_temp")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.join(df_ct_changwat_temp, tblCustomer_NameMasterByPNO.THM_CHW_CD == df_ct_changwat_temp.CHW_CD_temp, "left")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("CHW_T_NM_final", 
                                                                        when((col("CHW_T_NM")=='') & (col("thm_postcode") == col("ZipCode")) & (col("THM_CHW_CD") != '') & (col("THM_CHW_CD") == col("CHW_CD_temp")), col("CHW_T_NM_temp"))
                                                                        .when((col("CHW_T_NM")=='') & (col("amp_postcode") == col("ZipCode")) & (col("THM_CHW_CD") == '') & (col("AMP_CHW_CD") == col("CHW_CD")), col("CHW_T_NM"))
                                                                        .otherwise(col("CHW_T_NM")))
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("CHW_REG_final", 
                                                                        when((col("CHW_T_NM")=='') & (col("thm_postcode") == col("ZipCode")) & (col("THM_CHW_CD").isNotNull()) & (col("THM_CHW_CD") == col("CHW_CD_temp")),col("CHW_REG_temp"))
                                                                        .when((col("CHW_T_NM")=='') & (col("amp_postcode") == col("ZipCode")) & (col("THM_CHW_CD").isNull()) & (col("AMP_CHW_CD") == col("CHW_CD")), col("CHW_REG"))
                                                                        .otherwise(col("CHW_REG")))
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.drop("AMP_CHW_CD", 
                                                                "amp_postcode", 
                                                                "CHW_REG", 
                                                                "CHW_T_NM", 
                                                                "CHW_CD", 
                                                                "THM_CHW_CD", 
                                                                "thm_postcode", 
                                                                "CHW_REG_temp", 
                                                                "CHW_T_NM_temp", 
                                                                "CHW_CD_temp")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumnRenamed("CHW_T_NM_final", "CHW_T_NM")\
                                                            .withColumnRenamed("CHW_REG_final", "CHW_REG")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.dropDuplicates()
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.withColumn("Address", rtrim(concat(col("PADR3"), lit(" "),col("PADR4"))))

print("--Transformation pno de-dup")
df_correct_pno = tblCustomer_NameMasterByPNO.groupby("PNO").agg(count("PNO").alias("count")).filter(col("count")==1)
df_dup_pno = tblCustomer_NameMasterByPNO.groupby("PNO").agg(count("PNO").alias("count")).filter(col("count")>1)
df_correct_pno = df_correct_pno.drop("count")
df_dup_pno = df_dup_pno.drop("count")
df_correct_pno = df_correct_pno.withColumnRenamed("PNO", "Dup_PNO")
df_dup_pno = df_dup_pno.withColumnRenamed("PNO", "Dup_PNO")
df_correct_pno = df_correct_pno.join(tblCustomer_NameMasterByPNO, df_correct_pno.Dup_PNO==tblCustomer_NameMasterByPNO.PNO, "left")
df_dup_pno = df_dup_pno.join(tblCustomer_NameMasterByPNO, df_dup_pno.Dup_PNO==tblCustomer_NameMasterByPNO.PNO, "left")
df_correct_pno = df_correct_pno.withColumnRenamed("pcty", "Dup_pcty")
df_dup_pno = df_dup_pno.withColumnRenamed("pcty", "Dup_pcty")
df_fix_dup_pno = df_dup_pno.join(df_dimcitycode, df_dup_pno.Dup_pcty==df_dimcitycode.pcty, "left")
df_fix_dup_pno = df_fix_dup_pno.withColumn("String_Matching", 
                                            when(col("Address").contains(trim(col("CHW_T_NM"))), lit(1))
                                            .otherwise(lit(0)))
Flag_1 = df_fix_dup_pno.select("*").filter(col("String_Matching")==1)
Flag_0 = df_fix_dup_pno.join(Flag_1, ["PNO"], "leftanti")
Flag_0 = Flag_0.withColumn("CHW_T_NM_temp", col("province"))
Flag_0 = Flag_0.withColumn("CHW_REG_temp", col("region"))
Flag_0 = Flag_0.drop("CHW_T_NM")
Flag_0 = Flag_0.drop("CHW_REG")
Flag_0 = Flag_0.withColumnRenamed("CHW_T_NM_temp", "CHW_T_NM")
Flag_0 = Flag_0.withColumnRenamed("CHW_REG_temp", "CHW_REG")
Flag_0 = Flag_0.dropDuplicates()
Flag_1 = Flag_1.drop("Dup_PNO", "Address", "pcty", "Dup_pcty", "province", "region", "String_Matching")
Flag_0 = Flag_0.drop("Dup_PNO", "Address", "pcty", "Dup_pcty", "province", "region", "String_Matching")
union_flag = Flag_1.union(Flag_0)

df_correct_pno = df_correct_pno.drop("Dup_PNO", "Address", "Dup_pcty")
tblCustomer_NameMasterByPNO = df_correct_pno.union(union_flag)
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO \
.withColumn("PADR1_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR1"))) \
.withColumn("PADR2_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR2"))) \
.withColumn("PADR3_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR3"))) \
.withColumn("PADR4_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR4"))) \
.withColumn("CHW_T_NM_temp",
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), None)
            .otherwise(col("CHW_T_NM"))) \
.withColumn("CHW_REG_temp",
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), None)
            .otherwise(col("CHW_REG")))
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.drop("PADR1", "PADR2", "PADR3", "PADR4", "CHW_T_NM", "CHW_REG")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO \
                    .withColumnRenamed("PADR1_temp", "PADR1") \
                    .withColumnRenamed("PADR2_temp", "PADR2") \
                    .withColumnRenamed("PADR3_temp", "PADR3") \
                    .withColumnRenamed("PADR4_temp", "PADR4") \
                    .withColumnRenamed("CHW_T_NM_temp", "CHW_T_NM") \
                    .withColumnRenamed("CHW_REG_temp", "CHW_REG")
print("--Select require field...")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.select("PNO",
                                                                    "PDOI",
                                                                    "LPLAN",
                                                                    "PID",
                                                                    "PNAMF",
                                                                    "PNAME",
                                                                    "Prefix",
                                                                    "NameOnly",
                                                                    "FullName",
                                                                    "SearchString",
                                                                    "PSEX",
                                                                    "PDOB",
                                                                    "PMRSTU",
                                                                    "PADR1",
                                                                    "PADR2",
                                                                    "PADR3",
                                                                    "PADR4",
                                                                    "ZipCode",
                                                                    "ZipProvince",
                                                                    "CHW_T_NM",
                                                                    "CHW_REG",
                                                                    "PSTU",
                                                                    "PDOT")
print("--Sort data")
tblCustomer_NameMasterByPNO = tblCustomer_NameMasterByPNO.sort(col("PNO"))

print("--Write df to s3!")
tblCustomer_NameMasterByPNO.write.mode("overwrite").format("parquet").save(f"s3://{s3_bucket}/data/split_prefix/tblCustomer_NameMasterByPNO/")
print("--Complete!")
job.commit()
    