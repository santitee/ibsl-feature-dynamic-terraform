CREATE EXTERNAL TABLE `ibs_ref_g400_atlldta_itemsmt`(
  `itemtabl` string, 
  `itemitem` string, 
  `genarea` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://kt-th-dl-prod-ibs/ref_table/ibs_ref_g400_atlldta_itemsmt'
TBLPROPERTIES (
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1775041333')