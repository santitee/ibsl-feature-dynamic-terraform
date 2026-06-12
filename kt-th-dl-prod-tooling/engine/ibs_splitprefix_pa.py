# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job

# from pyspark.sql.types import *
# from pyspark.sql.functions import *
# from pyspark.sql import Window
# import re

# import ibs_utilities as ibs_utils

# ## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# run_params = ibs_utils.get_run_parameters(sys.argv)
# s3_bucket = run_params.args['BUCKET_NAME']

# def replaceUdf(column):
#     # column = column.encode("utf-8")
#     # column = re.sub("(\.)", "\\\\.", column)
#     for i in range(len(encode_namesCollect)):
#         if encode_namesCollect[i].lower() in column.lower(): 
#             remove_prefix = column.replace(encode_namesCollect[i], "")
#             # remove_prefix = re.sub("(\\\\.)", ".", remove_prefix)
#             break
#         else:
#             remove_prefix = column
#             # remove_prefix = re.sub("(\\\\.)", ".", remove_prefix)
#     return remove_prefix

# def splitprefixUdf(column):
#     # column = column.encode("utf-8")
#     # column = re.sub("(\.)", "\\\\.", column)
#     for i in range(len(encode_namesCollect)):
#         if encode_namesCollect[i].lower() in column.lower(): 
#             # prefix = re.sub("(\\\\.)", ".", encode_namesCollect[i])
#             prefix = encode_namesCollect[i]
#             break
#         else:
#             prefix = "ERROR"
#     return prefix

# print("--Reading Data")
# source_table = "processing_ibs.ibs_lfkpa_lfppml"
# prefix_table = "processing_ibs.ibs_tblcustomer_nameprefix"
# df_tblPolicyMaster_pa = spark.table(source_table)
# df_prefix = spark.table(prefix_table)

# print("--filter flag Insured")
# df_pa_I = df_tblPolicyMaster_pa.select("PNO",
#                                          "PDOI",
#                                          "DOS",
#                                          "PLANCODE",
#                                          "INSUIDNO",
#                                          "ORTHIDNO",
#                                          "INSUNAME",
#                                          "INSULNAME",
#                                          "INSUSEX", 
#                                          "INSUDOB", 
#                                          "INSUSTU", 
#                                          "INSUADD", 
#                                          "INSUDIST", 
#                                          "INSUPROV", 
#                                          "INSUPOST", 
#                                          "INSUMTEL", 
#                                          "INSUHTEL",
#                                          "PSTU",
#                                          "CRTDATE", 
#                                          "LSTDATE",
#                                          "OWNNAME"
#                                         ).filter(col("OWNNAME")=="")
# print("--filter flag Owner")
# df_pa_O = df_tblPolicyMaster_pa.select("PNO",
#                                        "PDOI",
#                                        "DOS",
#                                        "PLANCODE",
#                                        "OWNIDNO",
#                                        "ORTHIDNO",
#                                        "OWNNAME",
#                                        "OWNLNAME",
#                                        "OWNSEX", 
#                                        "OWNDOB", 
#                                        "OWNMTSTU",
#                                        "INSUADD", 
#                                        "INSUDIST", 
#                                        "INSUPROV", 
#                                        "INSUPOST", 
#                                        "INSUMTEL", 
#                                        "INSUHTEL", 
#                                        "PSTU",
#                                        "CRTDATE",
#                                        "LSTDATE"
#                                     ).filter(col("OWNNAME")!="")                                        

# print("--clean prefix")
# df_prefix = df_prefix.select("abbv")
# # df_prefix = df_prefix.withColumn("abbv_cleaned", regexp_replace(col("abbv"), "(\.)", "\\\\."))
# # df_prefix = df_prefix.drop("abbv")
# # df_prefix = df_prefix.withColumnRenamed("abbv_cleaned", "abbv")
# df_prefix_len = df_prefix.withColumn("trim_len_col",length(trim(col("abbv"))))
# sort_index = df_prefix_len.sort(col("trim_len_col").desc())
# sort_index = sort_index.drop("trim_len_col")
# encode_namesCollect = [row.abbv for row in sort_index.collect()]

# print("--define and process udf for insured")
# reg_replaceUdf = udf(replaceUdf, StringType())
# df_pa_I = df_pa_I.withColumn('nameonly', reg_replaceUdf(col('INSUNAME')))
# reg_splitUdf = udf(splitprefixUdf, StringType())
# df_pa_I = df_pa_I.withColumn('prefix', reg_splitUdf(col('INSUNAME')))

# print("--transforming for insured")
# df_pa_I = df_pa_I \
#                 .withColumn("fullname", concat(df_pa_I['nameonly'], lit(" "), df_pa_I['INSULNAME']))\
#                 .withColumn("IDNO", 
#                              when((regexp_replace("INSUIDNO", "-", "")) != "", regexp_replace("INSUIDNO", "-", ""))
#                              .otherwise(regexp_replace("ORTHIDNO", "-", ""))
#                             )\
#                 .withColumn("INS_OWN_FLAG", 
#                              when(lower(col("INSUNAME")).contains(lower(col("prefix"))), "I")
#                              .otherwise("")
#                             )
# df_pa_I = df_pa_I \
#             .withColumnRenamed("INSUNAME", "GIVEN_NAME") \
#             .withColumnRenamed("INSULNAME", "SURNAME") \
#             .withColumnRenamed("INSUSEX", "GENDER") \
#             .withColumnRenamed("INSUDOB", "DATE_OF_BIRTH") \
#             .withColumnRenamed("INSUSTU", "MSTU") \
#             .withColumnRenamed("INSUADD", "[ADDRESS]") \
#             .withColumnRenamed("INSUDIST", "ADD_DIST") \
#             .withColumnRenamed("INSUPROV", "ADD_PROV") \
#             .withColumnRenamed("INSUPOST", "ZIPCODE") \
#             .withColumnRenamed("INSUMTEL", "MTEL") \
#             .withColumnRenamed("INSUHTEL", "HTEL") 
# df_pa_I = df_pa_I.drop('OWNNAME')

# print("--define and process udf for owner")
# reg_replaceUdf = udf(replaceUdf, StringType())
# df_pa_O = df_pa_O.withColumn('nameonly', reg_replaceUdf(col('OWNNAME')))
# reg_splitUdf = udf(splitprefixUdf, StringType())
# df_pa_O = df_pa_O.withColumn('prefix', reg_splitUdf(col('OWNNAME')))

# print("--transforming for owner")
# df_pa_O = df_pa_O \
#                 .withColumn("fullname", concat(df_pa_O['nameonly'],lit(" "),df_pa_O['OWNLNAME']))\
#                 .withColumn("IDNO", regexp_replace("OWNIDNO", "-", ""))\
#                 .withColumn("INS_OWN_FLAG", when((col("OWNNAME")!="")&(col("OWNNAME").contains(col("prefix"))), "O"))
# df_pa_O = df_pa_O \
#             .withColumnRenamed("OWNNAME", "GIVEN_NAME") \
#             .withColumnRenamed("OWNLNAME", "SURNAME") \
#             .withColumnRenamed("OWNSEX", "GENDER") \
#             .withColumnRenamed("OWNDOB", "DATE_OF_BIRTH") \
#             .withColumnRenamed("OWNMTSTU", "MSTU") \
#             .withColumnRenamed("INSUADD", "[ADDRESS]") \
#             .withColumnRenamed("INSUDIST", "ADD_DIST") \
#             .withColumnRenamed("INSUPROV", "ADD_PROV") \
#             .withColumnRenamed("INSUPOST", "ZIPCODE") \
#             .withColumnRenamed("INSUMTEL", "MTEL") \
#             .withColumnRenamed("INSUHTEL", "HTEL") 

# print("--merge data")
# df_pa_final = df_pa_I.union(df_pa_O)

# print("--select required field")
# tblCustomer_NameMasterByPNO_PA = df_pa_final.select("PNO",
#                                  "PDOI",
#                                  "DOS",
#                                  "PLANCODE",
#                                  "IDNO",
#                                  "GIVEN_NAME",
#                                  "SURNAME",
#                                  "PREFIX",
#                                  "NAMEONLY",
#                                  "FULLNAME",
#                                  trim(concat(df_pa_final['NAMEONLY'],lit(""),df_pa_final['SURNAME'])).alias("SearchString"),
#                                  "GENDER",
#                                  "DATE_OF_BIRTH",
#                                  "MSTU",
#                                  col("[ADDRESS]").alias("ADDRESS"),
#                                  "ADD_DIST",
#                                  "ADD_PROV",
#                                  "ZIPCODE",
#                                  "MTEL",
#                                  "HTEL",
#                                  "PSTU",
#                                  "CRTDATE",
#                                  "LSTDATE",
#                                  "INS_OWN_FLAG"
#                                 )
# print("--Write df to s3!")
# tblCustomer_NameMasterByPNO_PA.write.mode("overwrite").parquet(f"s3://{s3_bucket}/data/split_prefix/tblCustomer_NameMasterByPNO_PA/")
# print("--Complete!!")
# job.commit()