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

# Script generated for node customer curated
customercurated_node1688069307052 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tvkbucket/customer/curated/"], "recurse": True},
    transformation_ctx="customercurated_node1688069307052",
)

# Script generated for node step trainer landing
steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tvkbucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1",
)

# Script generated for node filter step trainer
filtersteptrainer_node1688069631240 = Join.apply(
    frame1=customercurated_node1688069307052,
    frame2=steptrainerlanding_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="filtersteptrainer_node1688069631240",
)

# Script generated for node Drop customer fields
Dropcustomerfields_node1688069646260 = DropFields.apply(
    frame=filtersteptrainer_node1688069631240,
    paths=[
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "birthDay",
        "`.serialNumber`",
    ],
    transformation_ctx="Dropcustomerfields_node1688069646260",
)

# Script generated for node step trainer trusted
steptrainertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Dropcustomerfields_node1688069646260,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tvkbucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="steptrainertrusted_node3",
)

job.commit()
