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
AWSGlueDataCatalog_node1683559253480 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1683559253480",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1683559256037 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1683559256037",
)

# Script generated for node Join
Join_node1683559314442 = Join.apply(
    frame1=AWSGlueDataCatalog_node1683559253480,
    frame2=AWSGlueDataCatalog_node1683559256037,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1683559314442",
)

# Script generated for node Drop Fields
DropFields_node1683559388741 = DropFields.apply(
    frame=Join_node1683559314442,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1683559388741",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1683559455557 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1683559388741,
    database="stedi",
    table_name="accelerometer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1683559455557",
)

job.commit()
