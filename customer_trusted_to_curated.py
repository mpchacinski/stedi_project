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

# Script generated for node Customer Trusted
CustomerTrusted_node1708417857228 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1708417857228",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1708417564929 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1708417564929",
)

# Script generated for node Join Accelerometer Landing with Customer Trusted
SqlQuery190 = """
select distinct ct.*
from customer_trusted ct
join accelerometer_landing al
    on al.user = ct.email
"""
JoinAccelerometerLandingwithCustomerTrusted_node1708417908370 = sparkSqlQuery(
    glueContext,
    query=SqlQuery190,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1708417564929,
        "customer_trusted": CustomerTrusted_node1708417857228,
    },
    transformation_ctx="JoinAccelerometerLandingwithCustomerTrusted_node1708417908370",
)

# Script generated for node Customer Curated
CustomerCurated_node1708418030984 = glueContext.getSink(
    path="s3://chaciu-stedi-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1708418030984",
)
CustomerCurated_node1708418030984.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="customer_curated"
)
CustomerCurated_node1708418030984.setFormat("json")
CustomerCurated_node1708418030984.writeFrame(
    JoinAccelerometerLandingwithCustomerTrusted_node1708417908370
)
job.commit()
