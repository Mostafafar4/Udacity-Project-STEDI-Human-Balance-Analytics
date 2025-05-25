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

# Script generated for node Step Trainer
StepTrainer_node1748023636703 = glueContext.create_dynamic_frame.from_options
(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options=
{"paths": ["s3://stedi-lake-house-mostafa/step_trainer/landing/"],
 "recurse": True}, transformation_ctx="StepTrainer_node1748023636703")

# Script generated for node Customer Curated
CustomerCurated_node1748023718945 = glueContext.create_dynamic_frame.from_options
(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options=
{"paths": ["s3://stedi-lake-house-mostafa/customer/curated/"], "recurse": True},
transformation_ctx="CustomerCurated_node1748023718945")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1748024641671 = ApplyMapping.apply
(frame=StepTrainer_node1748023636703, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), 
                                                ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")]
, transformation_ctx="RenamedkeysforJoin_node1748024641671")

# Script generated for node Join
Join_node1748023749131 = Join.apply(frame1=CustomerCurated_node1748023718945, frame2=RenamedkeysforJoin_node1748024641671, keys1=
                                    ["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1748023749131")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1748023749131, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options=
                                   {"dataQualityEvaluationContext": "EvaluateDataQuality_node1748021992589", "enableDataQualityResultsPublishing": True},
                                   additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748024196472 = glueContext.getSink(path="s3://stedi-lake-house-mostafa/step_trainer/trusted/", connection_type=
                                                 "s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True,
                                                 transformation_ctx="AmazonS3_node1748024196472")
AmazonS3_node1748024196472.setCatalogInfo(catalogDatabase="mostafa-database2",catalogTableName="Step_trainer_trusted")
AmazonS3_node1748024196472.setFormat("json")
AmazonS3_node1748024196472.writeFrame(Join_node1748023749131)
job.commit()



