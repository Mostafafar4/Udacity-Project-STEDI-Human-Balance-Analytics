import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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
AmazonS3_node1748167555029 = glueContext.create_dynamic_frame.from_options
(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options=
{"paths": ["s3://stedi-lake-house-mostafa/customer/landing/"], "recurse": True},
transformation_ctx="AmazonS3_node1748167555029")

# Script generated for node Filter
Filter_node1748167605490 = Filter.apply(frame=AmazonS3_node1748167555029, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filter_node1748167605490")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Filter_node1748167605490, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options=
                                   {"dataQualityEvaluationContext": "EvaluateDataQuality_node1748167549741", "enableDataQualityResultsPublishing": True},
                                   additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748167623858 = glueContext.getSink(path="s3://stedi-lake-house-mostafa/customer/trusted/",
                                                 connection_type="s3", updateBehavior="UPDATE_IN_DATABASE",
                                                 partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748167623858")
AmazonS3_node1748167623858.setCatalogInfo(catalogDatabase="mostafa-database2",catalogTableName="customer_trusted")
AmazonS3_node1748167623858.setFormat("json")
AmazonS3_node1748167623858.writeFrame(Filter_node1748167605490)
job.commit()
