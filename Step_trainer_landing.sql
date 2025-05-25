CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-mostafa/step_trainer/landing/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Landing', 
  'CreatedByJobRun'='jr_e0836b699d3f555b28679bd5655cf0281403aaaf823d5b69331cda40a1537242', 
  'classification'='json')
