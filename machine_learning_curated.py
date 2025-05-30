import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node step_trainer_trused
step_trainer_trused_node1748162299669 = glueContext.create_dynamic_frame.from_options(
  format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options=
  {"paths": ["s3://stedi-lake-house-mostafa/step_trainer/trusted/"],
   "recurse": True}, transformation_ctx="step_trainer_trused_node1748162299669")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1748162320276 = glueContext.create_dynamic_frame.from_options
(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options=
{"paths": ["s3://stedi-lake-house-mostafa/accelerometer/trusted/"], "recurse": True},
transformation_ctx="accelerometer_trusted_node1748162320276")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource1
join myDataSource2 
on myDataSource1.sensorreadingtime = myDataSource2.timestamp
'''
SQLQuery_node1748163043949 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping =
                                           {"myDataSource1":step_trainer_trused_node1748162299669, "myDataSource2":accelerometer_trusted_node1748162320276},
                                           transformation_ctx = "SQLQuery_node1748163043949")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748163043949, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options=
                                   {"dataQualityEvaluationContext": "EvaluateDataQuality_node1748162285570", "enableDataQualityResultsPublishing": True},
                                   additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748163404572 = glueContext.getSink(path="s3://stedi-lake-house-mostafa/machine_learning/curated/",
                                                 connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[],
                                                 enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748163404572")
AmazonS3_node1748163404572.setCatalogInfo(catalogDatabase="mostafa-database2",catalogTableName="machine_learning_curated")
AmazonS3_node1748163404572.setFormat("json")
AmazonS3_node1748163404572.writeFrame(SQLQuery_node1748163043949)
job.commit()
