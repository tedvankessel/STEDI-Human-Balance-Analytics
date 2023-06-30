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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tvkbucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node accelerometeru_trusted
accelerometeru_trusted_node1688085258855 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://tvkbucket/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="accelerometeru_trusted_node1688085258855",
    )
)

# Script generated for node same timestamp
sametimestamp_node1688085622449 = Join.apply(
    frame1=accelerometeru_trusted_node1688085258855,
    frame2=step_trainer_trusted_node1,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="sametimestamp_node1688085622449",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=sametimestamp_node1688085622449,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tvkbucket/step_trainer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()
