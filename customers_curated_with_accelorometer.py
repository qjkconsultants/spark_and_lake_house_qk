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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1757139949188 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-qk/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1757139949188")

# Script generated for node Customer Trusted
CustomerTrusted_node1757139909252 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-qk/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1757139909252")

# Script generated for node Join
Join_node1757139980937 = Join.apply(frame1=AccelerometerLanding_node1757139949188, frame2=CustomerTrusted_node1757139909252, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1757139980937")

# Script generated for node SQL Query
SqlQuery923 = '''
select distinct customerName, email, phone, birthday, serialNumber,
registrationDate, lastUpdateDate, shareWithResearchAsOfDate, 
shareWithPublicAsOfDate, shareWithFriendsAsOfDate from myDataSource
'''
SQLQuery_node1757140002420 = sparkSqlQuery(glueContext, query = SqlQuery923, mapping = {"myDataSource":Join_node1757139980937}, transformation_ctx = "SQLQuery_node1757140002420")

# Script generated for node Customers Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757140002420, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757139860297", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomersCurated_node1757140019180 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1757140002420, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-qk/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomersCurated_node1757140019180")

job.commit()