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

print("--Reading Data")
source_table = "processing_ibs.ibs_lfkludta_lfppml"
prefix_table = "processing_ibs.ibs_tblcustomer_nameprefix"
df_prefix_company = "processing_ibs.ibs_tblcompany_nameprefix"

df_splitprefix_owner = spark.table(source_table)
df_prefix = spark.table(prefix_table)
df_prefix_company = spark.table(df_prefix_company)

print("--filter owner")
df_splitprefix_owner = df_splitprefix_owner \
.select("*") \
.where(df_splitprefix_owner.pownrf.isNotNull()) \
.where(df_splitprefix_owner.pownrf != '') \
.where(df_splitprefix_owner.pownrf != '-')



print("--clean prefix")
df_prefix = df_prefix.select("abbv")
# df_prefix = df_prefix.withColumn("abbv_cleaned", regexp_replace(col("abbv"), "(\.)", "\\\\."))
# df_prefix = df_prefix.drop("abbv")
# df_prefix = df_prefix.withColumnRenamed("abbv_cleaned", "abbv")
df_prefix_len = df_prefix.withColumn("trim_len_col",length(trim(col("abbv"))))
sort_index = df_prefix_len.sort(col("trim_len_col").desc())
sort_index = sort_index.drop("trim_len_col")
encode_namesCollect = [row.abbv for row in sort_index.collect()]

print("--define udf")
reg_replaceUdf = udf(replaceUdf, StringType())
df_splitprefix_owner = df_splitprefix_owner.withColumn('nameonly_temp', reg_replaceUdf(col('pownrf')))
reg_splitUdf = udf(splitprefixUdf, StringType())
df_splitprefix_owner = df_splitprefix_owner.withColumn('prefix_temp', reg_splitUdf(col('pownrf')))

df_splitprefix_owner = df_splitprefix_owner.withColumn("fullname_temp", concat(df_splitprefix_owner['nameonly_temp'],lit(" "),df_splitprefix_owner['powner']))


print("--clean company prefix")
df_prefix_company = df_prefix_company.select(trim(col("abbv")).alias("abbv"))
# df_prefix_company = df_prefix_company.withColumn("abbv_cleaned", regexp_replace(col("abbv"), "(\.)", "\\\\."))
# df_prefix_company = df_prefix_company.drop("abbv")
# df_prefix_company = df_prefix_company.withColumnRenamed("abbv_cleaned", "abbv")
df_prefix_len_c = df_prefix_company.withColumn("trim_len_col",length(trim(col("abbv"))))
sort_index_c = df_prefix_len_c.sort(col("trim_len_col").desc())
sort_index_c = sort_index_c.drop("trim_len_col")
encode_namesCollect = [row.abbv for row in sort_index_c.collect()]

print("--processing udf")
reg_replaceUdf = udf(replaceUdf, StringType())
df_splitprefix_owner = df_splitprefix_owner.withColumn('company_nameonly', reg_replaceUdf(col('pownrf')))
reg_splitUdf = udf(splitprefixUdf, StringType())
df_splitprefix_owner = df_splitprefix_owner.withColumn('company_prefix_only', reg_splitUdf(col('pownrf')))
df_splitprefix_owner = df_splitprefix_owner.withColumn("company_fullname", concat(df_splitprefix_owner['company_nameonly'],lit(" "),df_splitprefix_owner['powner']))

print("--merge dataframe")
df_splitprefix_owner = df_splitprefix_owner.withColumn("nameonly",
                                                      when((col("nameonly_temp").isNotNull())|(col("nameonly_temp")!=""), col("nameonly_temp"))
                                                       .when((col("company_nameonly").isNotNull())&(col("company_nameonly")!=""), col("company_nameonly"))
                                                       .otherwise(col("nameonly_temp"))
                                                      )
df_splitprefix_owner = df_splitprefix_owner.withColumn("prefix_only",
                                                        when((col("prefix_temp")!="ERROR"), col("prefix_temp"))
                                                       .when((col("prefix_temp")!="ERROR")&(col("company_prefix_only")!="ERROR"), col("company_prefix_only"))
                                                       .otherwise("ERROR")
                                                      )
df_splitprefix_owner = df_splitprefix_owner.withColumn("fullname",
                                                      when((col("fullname_temp").isNotNull())|(col("fullname_temp")!=""), col("fullname_temp"))
                                                       .when((col("company_fullname").isNotNull())|(col("company_fullname")!=""), col("company_fullname"))
                                                       .otherwise(col("fullname_temp"))
                                                      )
df_splitprefix_owner = df_splitprefix_owner.withColumn("CUSTOMER_TYPE_CODE",
                                                       when((col("prefix_only")=="ERROR"), None)
                                                       .when((col("prefix_only")!="ERROR")&(col("prefix_temp")!="ERROR"), "P")
                                                       .when((col("prefix_only")!="ERROR")&(col("company_prefix_only")!="ERROR"), "C")
                                                       .otherwise(None)
                                                      )

tblCustomer_NameMasterByPNO_Owner = df_splitprefix_owner.select("PNO",
                                                    "PDOI",
                                                    "LPLAN",
                                                    ltrim(rtrim(df_splitprefix_owner.pownid)).alias("POWNID"),
                                                    "POWNRF",
                                                    "POWNER",
                                                    df_splitprefix_owner.prefix_only.alias("PREFIX"),
                                                    ltrim(rtrim(df_splitprefix_owner.nameonly)).alias("NameOnly"),
                                                    ltrim(rtrim(df_splitprefix_owner.fullname)).alias("FullName"),
                                                    trim(concat(df_splitprefix_owner['NameOnly'],lit(""),df_splitprefix_owner['powner'])).alias("SearchString"),
                                                    df_splitprefix_owner.powsex.alias("PSEX"),
                                                    df_splitprefix_owner.lpaydb.alias("PDOB"),  
                                                    "PMRSTU",     
                                                    "PADR1",  
                                                    "PADR2",
                                                    "PADR3",
                                                    "PADR4",
                                                    "PSTU",
                                                    "PDOT",
                                                    "pcty",
                                                    "CUSTOMER_TYPE_CODE")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner \
.withColumnRenamed("POWNID", "PID") \
.withColumnRenamed("POWNRF", "PNAMF") \
.withColumnRenamed("POWNER", "PNAME")

tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn("ZipCode", 
                                                                     regexp_extract(rtrim(concat(
                                                                         col("PADR1"), 
                                                                         lit(" "), 
                                                                         col("PADR2"), 
                                                                         lit(" "),
                                                                         col("PADR3"), 
                                                                         lit(" "),
                                                                         col("PADR4"))), "(\d{5})", 1))
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn("ZipCode_Temp",
                                                                            when(tblCustomer_NameMasterByPNO_Owner.ZipCode == '00000', '')
                                                                            .when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), 
                                                                                substring("PNAMF", 8, 4))
                                                                            .otherwise(tblCustomer_NameMasterByPNO_Owner.ZipCode))
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.drop("ZipCode")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumnRenamed("ZipCode_Temp", "ZipCode")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn("ZipCode_Temp", 
                                       when(col("ZipCode").rlike("(.*[^0-9].*)"), "")
                                       .otherwise(tblCustomer_NameMasterByPNO_Owner.ZipCode)
                                      )
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.drop("ZipCode")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumnRenamed("ZipCode_Temp", "ZipCode")

print("--Spliting ZipProvince")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn(
    "ZipProvince", 
    when(length("zipcode")==5,
         when(substring("zipcode",1,1) == '1', substring("zipcode", 1, 3))
         .when((col("zipcode") != '') & (substring("zipcode",1,1) != '1'), substring("zipcode", 1, 2))
         .otherwise(''))
    .when((length("zipcode")<5), substring("zipcode", 1, 1))
    )

print("--Reading address data")
df_ct_amphor = "processing_ibs.ibs_ct_amphor"
df_ct_amphor = spark.table(df_ct_amphor)
df_ct_amphor = df_ct_amphor.select("AMP_CHW_CD", "amp_postcode").dropDuplicates()
df_ct_changwat = "processing_ibs.ibs_ct_changwat"
df_ct_changwat = spark.table(df_ct_changwat)
df_ct_changwat = df_ct_changwat.select("CHW_REG","CHW_T_NM", "CHW_CD").dropDuplicates()
df_ct_tambol = "processing_ibs.ibs_ct_tambol"
df_ct_tambol = spark.table(df_ct_tambol)
df_ct_tambol = df_ct_tambol.select("THM_CHW_CD", "thm_postcode").dropDuplicates()

tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.join(df_ct_amphor, 
                                        tblCustomer_NameMasterByPNO_Owner.ZipCode == df_ct_amphor.amp_postcode, 
                                        "left")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.join(df_ct_changwat, 
                                 tblCustomer_NameMasterByPNO_Owner.AMP_CHW_CD == df_ct_changwat.CHW_CD, 
                                 "left")
df_dimcitycode = "processing_ibs.ibs_dimcitycode"
df_dimcitycode = spark.table(df_dimcitycode)
df_dimcitycode = df_dimcitycode.select("pcty", "province", "region")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner \
                    .join(df_ct_tambol, tblCustomer_NameMasterByPNO_Owner.ZipCode == df_ct_tambol.thm_postcode, "left")
df_ct_changwat_temp = df_ct_changwat \
                    .withColumnRenamed("CHW_REG", "CHW_REG_temp") \
                    .withColumnRenamed("CHW_T_NM", "CHW_T_NM_temp") \
                    .withColumnRenamed("CHW_CD", "CHW_CD_temp")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.join(df_ct_changwat_temp, 
                                                               tblCustomer_NameMasterByPNO_Owner.THM_CHW_CD == df_ct_changwat_temp.CHW_CD_temp, "left")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn("CHW_T_NM_final", 
                when((col("CHW_T_NM").isNull()) & (col("thm_postcode") == col("ZipCode")) & (col("THM_CHW_CD") != '') & (col("THM_CHW_CD") == col("CHW_CD_temp")), 
                    col("CHW_T_NM_temp"))
                .when((col("CHW_T_NM").isNull()) & (col("amp_postcode") == col("ZipCode")) & (col("THM_CHW_CD") == '') & (col("AMP_CHW_CD") == col("CHW_CD")), 
                     col("CHW_T_NM"))
                .otherwise(col("CHW_T_NM"))
               )
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn("CHW_REG_final", 
                when((col("CHW_T_NM").isNull()) & (col("thm_postcode") == col("ZipCode")) & (col("THM_CHW_CD").isNotNull()) & (col("THM_CHW_CD") == col("CHW_CD_temp")),
                     col("CHW_REG_temp"))
                .when((col("CHW_T_NM").isNull()) & (col("amp_postcode") == col("ZipCode")) & (col("THM_CHW_CD").isNull()) & (col("AMP_CHW_CD") == col("CHW_CD")), 
                     col("CHW_REG"))
                .otherwise(col("CHW_REG"))
                      )
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.drop("AMP_CHW_CD", 
                                                                 "amp_postcode", 
                                                                 "CHW_REG", 
                                                                 "CHW_T_NM", 
                                                                 "CHW_CD", 
                                                                 "THM_CHW_CD", 
                                                                 "thm_postcode", 
                                                                 "CHW_REG_temp", 
                                                                 "CHW_T_NM_temp", 
                                                                 "CHW_CD_temp")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumnRenamed("CHW_T_NM_final", "CHW_T_NM") \
                                                        .withColumnRenamed("CHW_REG_final", "CHW_REG")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.dropDuplicates()
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.withColumn("Address",
                                                                        rtrim(concat(
                                                                        col("PADR3"), 
                                                                        lit(" "),
                                                                        col("PADR4")))
                                                                      )
df_correct_pno = tblCustomer_NameMasterByPNO_Owner.groupby("PNO").agg(count("PNO").alias("count")).filter(col("count")==1)
df_dup_pno = tblCustomer_NameMasterByPNO_Owner.groupby("PNO").agg(count("PNO").alias("count")).filter(col("count")>1)
df_correct_pno = df_correct_pno.drop("count")
df_dup_pno = df_dup_pno.drop("count")
df_correct_pno = df_correct_pno.withColumnRenamed("PNO", "Dup_PNO")
df_dup_pno = df_dup_pno.withColumnRenamed("PNO", "Dup_PNO")
df_correct_pno = df_correct_pno.join(tblCustomer_NameMasterByPNO_Owner, df_correct_pno.Dup_PNO==tblCustomer_NameMasterByPNO_Owner.PNO, "left")
df_dup_pno = df_dup_pno.join(tblCustomer_NameMasterByPNO_Owner, df_dup_pno.Dup_PNO==tblCustomer_NameMasterByPNO_Owner.PNO, "left")
df_correct_pno = df_correct_pno.withColumnRenamed("pcty", "Dup_pcty")
df_dup_pno = df_dup_pno.withColumnRenamed("pcty", "Dup_pcty")
df_fix_dup_pno = df_dup_pno.join(df_dimcitycode, df_dup_pno.Dup_pcty==df_dimcitycode.pcty, "left")
df_fix_dup_pno = df_fix_dup_pno.withColumn("String_Matching", 
                                            when(col("Address").contains(trim(col("CHW_T_NM"))), lit(1))
                                           .otherwise(lit(0))
                                           )
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
df_final = df_correct_pno.union(union_flag)
df_final = df_final \
.withColumn("PADR1_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR1"))
            ) \
.withColumn("PADR2_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR2"))
            ) \
.withColumn("PADR3_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR3"))
            ) \
.withColumn("PADR4_temp", 
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), col("PNO"))
            .otherwise(col("PADR4"))
            ) \
.withColumn("CHW_T_NM_temp",
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), None)
            .otherwise(col("CHW_T_NM"))
           ) \
.withColumn("CHW_REG_temp",
            when(col("PNAMF").rlike("^[0-9]{3}-[0-9]{7}"), None)
            .otherwise(col("CHW_REG"))
           )
df_final = df_final.drop("PADR1", "PADR2", "PADR3", "PADR4", "CHW_T_NM", "CHW_REG")
df_final = df_final \
                    .withColumnRenamed("PADR1_temp", "PADR1") \
                    .withColumnRenamed("PADR2_temp", "PADR2") \
                    .withColumnRenamed("PADR3_temp", "PADR3") \
                    .withColumnRenamed("PADR4_temp", "PADR4") \
                    .withColumnRenamed("CHW_T_NM_temp", "CHW_T_NM") \
                    .withColumnRenamed("CHW_REG_temp", "CHW_REG") 
tblCustomer_NameMasterByPNO_Owner = df_final
print("--Select required field")
tblCustomer_NameMasterByPNO_Owner = tblCustomer_NameMasterByPNO_Owner.select(
"PNO",
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
"PDOT",
"CUSTOMER_TYPE_CODE")
print("--Write df to s3!")
tblCustomer_NameMasterByPNO_Owner.write.mode("overwrite").parquet(f"s3://{s3_bucket}/data/split_prefix/tblCustomer_NameMasterByPNO_Owner/")
print("--Complete!!")
job.commit()