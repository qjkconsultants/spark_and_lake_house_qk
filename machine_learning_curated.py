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

# Script generated for node Customer Trusted
CustomerTrusted_node1757155489473 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1757155489473")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1757155397491 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1757155397491")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1757155386493 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1757155386493")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1757155584336 = ApplyMapping.apply(frame=CustomerTrusted_node1757155489473, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("birthday", "string", "right_birthday", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("customername", "string", "right_customername", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("email", "string", "right_email", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1757155584336")

# Script generated for node Step Trainer Join Accelerometer
StepTrainerJoinAccelerometer_node1757155423358 = Join.apply(frame1=StepTrainerTrusted_node1757155397491, frame2=AccelerometerTrusted_node1757155386493, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="StepTrainerJoinAccelerometer_node1757155423358")

# Script generated for node Step Trainer Accelerometer JOIN Customer
StepTrainerAccelerometerJOINCustomer_node1757155472373 = Join.apply(frame1=StepTrainerJoinAccelerometer_node1757155423358, frame2=RenamedkeysforJoin_node1757155584336, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="StepTrainerAccelerometerJOINCustomer_node1757155472373")

# Script generated for node Machine Learning Curated.
EvaluateDataQuality().process_rows(frame=StepTrainerAccelerometerJOINCustomer_node1757155472373, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757152980933", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1757155615028 = glueContext.getSink(path="s3://stedi-lake-house-qk/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1757155615028")
MachineLearningCurated_node1757155615028.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1757155615028.setFormat("json")
MachineLearningCurated_node1757155615028.writeFrame(StepTrainerAccelerometerJOINCustomer_node1757155472373)
job.commit()