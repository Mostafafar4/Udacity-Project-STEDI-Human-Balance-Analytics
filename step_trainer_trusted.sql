CREATE EXTERNAL TABLE `step_trainer_trusted`(
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-mostafa/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='step trainer trusted', 
  'CreatedByJobRun'='jr_5b0412be5d57369d4f034dd9b0d395651cce33286e078ce266e19130eda02c35', 
  'classification'='json')
