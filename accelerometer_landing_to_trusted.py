import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Amazon S3
AmazonS3_node1748167555029 = glueContext.create_dynamic_frame.from_options(format_options=
                                                                           {"multiLine": "false"}, connection_type="s3", format="json", connection_options=
                                                                           {"paths": ["s3://stedi-lake-house-mostafa/customer/trusted/"], "recurse": True},
                                                                           transformation_ctx="AmazonS3_node1748167555029")

# Script generated for node Amazon S3
AmazonS3_node1748167807468 = glueContext.create_dynamic_frame.from_options(format_options=
                                                                           {"multiLine": "false"}, connection_type="s3", format="json", connection_options=
                                                                           {"paths": ["s3://stedi-lake-house-mostafa/accelerometer/landing/"], "recurse": True},
                                                                           transformation_ctx="AmazonS3_node1748167807468")

# Script generated for node Join
Join_node1748167871519 = Join.apply(frame1=AmazonS3_node1748167807468, frame2=AmazonS3_node1748167555029, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1748167871519")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1748167871519, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options=
                                   {"dataQualityEvaluationContext": "EvaluateDataQuality_node1748167549741", "enableDataQualityResultsPublishing": True},
                                   additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748167623858 = glueContext.getSink(path="s3://stedi-lake-house-mostafa/accelerometer/trusted/", connection_type="s3",
                                                 updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True,
                                                 transformation_ctx="AmazonS3_node1748167623858")
AmazonS3_node1748167623858.setCatalogInfo(catalogDatabase="mostafa-database2",catalogTableName="accelerometer_trusted")
AmazonS3_node1748167623858.setFormat("json")
AmazonS3_node1748167623858.writeFrame(Join_node1748167871519)
