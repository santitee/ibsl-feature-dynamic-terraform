CREATE EXTERNAL TABLE `ibs_ref_group_product_type_lserff`(
  `itemtabl` string, 
  `descitem` string, 
  `longdesc` string, 
  `validflag` string, 
  `lserff_code` string, 
  `dt_ppt_code` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://kt-th-dl-prod-ibs/ref_table/ibs_ref_group_product_type_lserff'
TBLPROPERTIES (
  'transient_lastDdlTime'='1770639786')