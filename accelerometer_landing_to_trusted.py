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

# Script generated for node Amazon S3
AmazonS3_node1757128664531 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelorometer_landing", transformation_ctx="AmazonS3_node1757128664531")

# Script generated for node Amazon S3
AmazonS3_node1757128643733 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-qk/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1757128643733")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1757128680082 = Join.apply(frame1=AmazonS3_node1757128664531, frame2=AmazonS3_node1757128643733, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1757128680082")

# Script generated for node SQL Query
SqlQuery914 = '''
select user, x, y, z, timestamp from myDataSource

'''
SQLQuery_node1757135173003 = sparkSqlQuery(glueContext, query = SqlQuery914, mapping = {"myDataSource":CustomerPrivacyFilter_node1757128680082}, transformation_ctx = "SQLQuery_node1757135173003")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757135173003, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757128621835", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1757128772635 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1757135173003, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-qk/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelerometerTrusted_node1757128772635")

job.commit()