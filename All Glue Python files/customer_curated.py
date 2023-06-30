import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tvkbucket/customer/trusted/"], "recurse": True},
    transformation_ctx="customertrusted_node1",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1688061229930 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tvkbucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1688061229930",
)

# Script generated for node customers with accelerometer data
customerswithaccelerometerdata_node1688061328209 = Join.apply(
    frame1=customertrusted_node1,
    frame2=accelerometerlanding_node1688061229930,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customerswithaccelerometerdata_node1688061328209",
)

# Script generated for node Drop accelerometer fields
Dropaccelerometerfields_node1688061686254 = DropFields.apply(
    frame=customerswithaccelerometerdata_node1688061328209,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="Dropaccelerometerfields_node1688061686254",
)

# Script generated for node Drop customer Duplicates
DropcustomerDuplicates_node1688061793597 = DynamicFrame.fromDF(
    Dropaccelerometerfields_node1688061686254.toDF().dropDuplicates(),
    glueContext,
    "DropcustomerDuplicates_node1688061793597",
)

# Script generated for node customer curated
customercurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropcustomerDuplicates_node1688061793597,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tvkbucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customercurated_node3",
)

job.commit()
