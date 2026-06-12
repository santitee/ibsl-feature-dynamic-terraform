-- Create all reference table with group policy

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
  'transient_lastDdlTime'='1775041333');
  
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
  'transient_lastDdlTime'='1775669247');
  
CREATE EXTERNAL TABLE `ibs_ref_group_policy_type_lserff`(
  `desctabl` string, 
  `cnttype` string, 
  `longdesc` string, 
  `lserff_code` string, 
  `dt_ppt_code` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://kt-th-dl-prod-ibs/ref_table/ibs_ref_group_policy_type_lserff'
TBLPROPERTIES (
  'transient_lastDdlTime'='1776849353');
  
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
  'transient_lastDdlTime'='1771903223');

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
  'transient_lastDdlTime'='1770639786');

CREATE EXTERNAL TABLE `ibs_ref_g400_claim_reject_reason`(
  `desctabl` string, 
  `descitem` string, 
  `shortdesc` string, 
  `longdesc` string, 
  `validflag` string, 
  `oic_code` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://kt-th-dl-prod-ibs/ref_table/ibs_ref_g400_claim_reject_reason'
TBLPROPERTIES (
  'transient_lastDdlTime'='1773028116');