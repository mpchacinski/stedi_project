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

# Script generated for node Customer Curated
CustomerCurated_node1708419291539 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1708419291539",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1708419289956 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1708419289956",
)

# Script generated for node Join Step Trainer Landing with Customer Curated
SqlQuery117 = """
select stl.* 
from step_trainer_landing stl
join customer_curated cc
    on stl.serialnumber = cc.serialnumber

"""
JoinStepTrainerLandingwithCustomerCurated_node1708421403396 = sparkSqlQuery(
    glueContext,
    query=SqlQuery117,
    mapping={
        "customer_curated": CustomerCurated_node1708419291539,
        "step_trainer_landing": StepTrainerLanding_node1708419289956,
    },
    transformation_ctx="JoinStepTrainerLandingwithCustomerCurated_node1708421403396",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1708421714008 = glueContext.getSink(
    path="s3://chaciu-stedi-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1708421714008",
)
StepTrainerTrusted_node1708421714008.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1708421714008.setFormat("json")
StepTrainerTrusted_node1708421714008.writeFrame(
    JoinStepTrainerLandingwithCustomerCurated_node1708421403396
)
job.commit()
