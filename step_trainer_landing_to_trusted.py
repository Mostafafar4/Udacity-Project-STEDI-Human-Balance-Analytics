import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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
AmazonS3_node1748167555029 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, 
                                                                           connection_type="s3", format="json", connection_options=
                                                                           {"paths": ["s3://stedi-lake-house-mostafa/customer/curated/"], "recurse": True},
                                                                           transformation_ctx="AmazonS3_node1748167555029")

# Script generated for node Amazon S3
AmazonS3_node1748167807468 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options=
    {"paths": ["s3://stedi-lake-house-mostafa/step_trainer/landing/"], "recurse": True},
    transformation_ctx="AmazonS3_node1748167807468")

# Script generated for node Join
AmazonS3_node1748167555029DF = AmazonS3_node1748167555029.toDF()
AmazonS3_node1748167807468DF = AmazonS3_node1748167807468.toDF()
Join_node1748169349579 = DynamicFrame.fromDF(AmazonS3_node1748167555029DF.join(AmazonS3_node1748167807468DF,
                                                                               (AmazonS3_node1748167555029DF['serialnumber'] 
                                                                                == AmazonS3_node1748167807468DF['serialnumber'])
                                                                               , "outer"), glueContext, "Join_node1748169349579")

# Script generated for node Drop Fields
DropFields_node1748169618129 = DropFields.apply(frame=Join_node1748169349579, paths=
                                                ["`.serialnumber`",
                                                 "sharewithfriendsasofdate",
                                                 "sharewithpublicasofdate",
                                                 "sharewithresearchasofdate", 
                                                 "lastupdatedate",
                                                 "registrationdate",
                                                 "birthday",
                                                 "phone", 
                                                 "customername",
                                                 "email", 
                                                 "serialnumber"],
                                                transformation_ctx="DropFields_node1748169618129")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1748169618129, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options=
                                   {"dataQualityEvaluationContext": "EvaluateDataQuality_node1748167549741", "enableDataQualityResultsPublishing": True}
                                   , additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748167623858 = glueContext.getSink(path="s3://stedi-lake-house-mostafa/step_trainer/trusted/", 
                                                 connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", 
                                                 partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748167623858")
AmazonS3_node1748167623858.setCatalogInfo(catalogDatabase="mostafa-database2",catalogTableName="step_trainer_trusted")
AmazonS3_node1748167623858.setFormat("json")
AmazonS3_node1748167623858.writeFrame(DropFields_node1748169618129)
job.commit()
