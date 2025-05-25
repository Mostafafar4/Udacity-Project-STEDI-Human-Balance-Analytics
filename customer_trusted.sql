CREATE EXTERNAL TABLE `customer_trusted`(
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-mostafa/customer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='customer trusted', 
  'CreatedByJobRun'='jr_b58833718127dff53271001e2b15e73b4b5844bc501a853f15cb32c31f8c195c', 
  'classification'='json')
