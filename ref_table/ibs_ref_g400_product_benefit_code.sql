CREATE EXTERNAL TABLE `ibs_ref_g400_product_benefit_code`(
  `prodtyp` string, 
  `longdesc1` string, 
  `bencde` string, 
  `longdesc2` string, 
  `oic_code` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://kt-th-dl-prod-ibs/ref_table/ibs_ref_g400_product_benefit_code'
TBLPROPERTIES (
  'transient_lastDdlTime'='1771903223')