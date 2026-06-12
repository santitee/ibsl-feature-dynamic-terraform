CREATE EXTERNAL TABLE `ibs_ref_health_claim_reject_reason`(
  `claimsts` string, 
  `claimstsgroup` string, 
  `claimstsdes` string, 
  `oic_code` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://kt-th-dl-prod-ibs/ref_table/ibs_ref_health_claim_reject_reason'
TBLPROPERTIES (
  'transient_lastDdlTime'='1775669247')