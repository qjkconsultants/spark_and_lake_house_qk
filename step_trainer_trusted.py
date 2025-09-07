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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1757143330187 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1757143330187")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1757143347320 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-qk/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1757143347320")

# Script generated for node SQL Query
SqlQuery919 = '''
SELECT st.serialnumber,
       sensorreadingtime,
       FIRST_VALUE(distancefromobject) 
         OVER (PARTITION BY st.serialnumber, st.sensorreadingtime ORDER BY st.sensorreadingtime) AS distancefromobject
FROM stedi.step_trainer_landing st
JOIN customer_curated c
  ON c.serialnumber = st.serialnumber;
'''
SQLQuery_node1757143772990 = sparkSqlQuery(glueContext, query = SqlQuery919, mapping = {"customer_curated":AWSGlueDataCatalog_node1757143330187, "step_trainer_landing":StepTrainerLanding_node1757143347320}, transformation_ctx = "SQLQuery_node1757143772990")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757143772990, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757139860297", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1757143463027 = glueContext.getSink(path="s3://stedi-lake-house-qk/step_trainer/final_output/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1757143463027")
AmazonS3_node1757143463027.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1757143463027.setFormat("json")
AmazonS3_node1757143463027.writeFrame(SQLQuery_node1757143772990)
job.commit()