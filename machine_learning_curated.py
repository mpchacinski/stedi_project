import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1708422096045 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1708422096045",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1708422097217 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1708422097217",
)

# Script generated for node Join Step Trainer Landing with Accelerometer Trusted
SqlQuery143 = """
select 
    stt.*,
    at.user,
    at.x,
    at.y,
    at.z
from step_trainer_trusted stt
join accelerometer_trusted at
    on stt.sensorreadingtime = at.timestamp
"""
JoinStepTrainerLandingwithAccelerometerTrusted_node1708422346846 = sparkSqlQuery(
    glueContext,
    query=SqlQuery143,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1708422096045,
        "accelerometer_trusted": AccelerometerTrusted_node1708422097217,
    },
    transformation_ctx="JoinStepTrainerLandingwithAccelerometerTrusted_node1708422346846",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1708422554051 = glueContext.getSink(
    path="s3://chaciu-stedi-lake-house/machine-learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1708422554051",
)
MachineLearningCurated_node1708422554051.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1708422554051.setFormat("json")
MachineLearningCurated_node1708422554051.writeFrame(
    JoinStepTrainerLandingwithAccelerometerTrusted_node1708422346846
)
job.commit()
