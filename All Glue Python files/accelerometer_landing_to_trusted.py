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

# Script generated for node accelerometer landing
accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tvkbucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1",
)

# Script generated for node customer trusted
customertrusted_node1688053002043 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tvkbucket/customer/trusted/"], "recurse": True},
    transformation_ctx="customertrusted_node1688053002043",
)

# Script generated for node consent filter
consentfilter_node1688053125350 = Join.apply(
    frame1=accelerometerlanding_node1,
    frame2=customertrusted_node1688053002043,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="consentfilter_node1688053125350",
)

# Script generated for node remove non accelerometer data
removenonaccelerometerdata_node1688053928475 = DropFields.apply(
    frame=consentfilter_node1688053125350,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithResearchAsOfDate",
    ],
    transformation_ctx="removenonaccelerometerdata_node1688053928475",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=removenonaccelerometerdata_node1688053928475,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tvkbucket/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometertrusted_node3",
)

job.commit()
