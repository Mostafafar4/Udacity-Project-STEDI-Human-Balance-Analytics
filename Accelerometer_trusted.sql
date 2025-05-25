CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `serialnumber` string COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-mostafa/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='accelerometer trusted ', 
  'CreatedByJobRun'='jr_2eb29179a6d082283f15881d57fff264c7f444dc0bee645e146fd5144227f346', 
  'classification'='json')
