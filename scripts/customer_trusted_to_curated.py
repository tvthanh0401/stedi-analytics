import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1683561118929 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1683561118929",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1683561120566 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1683561120566",
)

# Script generated for node Join
Join_node1683561202140 = Join.apply(
    frame1=AWSGlueDataCatalog_node1683561118929,
    frame2=AWSGlueDataCatalog_node1683561120566,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1683561202140",
)

# Script generated for node Drop Fields
DropFields_node1683561234926 = DropFields.apply(
    frame=Join_node1683561202140,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1683561234926",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1683561293955 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1683561234926,
    database="stedi",
    table_name="customer_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1683561293955",
)

job.commit()
