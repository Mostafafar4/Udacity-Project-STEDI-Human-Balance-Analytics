CREATE EXTERNAL TABLE `machine_learning_curated`(
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-mostafa/machine_learning/curated/'
TBLPROPERTIES (
  'CreatedByJob'='machine_learning_curated', 
  'CreatedByJobRun'='jr_eab5c58906a8476d3c700eb1e2bf47942e3fd6ee41d7cd6b7331e56536cfa86a', 
  'classification'='json')
