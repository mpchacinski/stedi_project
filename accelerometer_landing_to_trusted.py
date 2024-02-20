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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1708423418432 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1708423418432",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1708366902512 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1708366902512",
)

# Script generated for node Join Customer Trusted to Accelerometer Landing
SqlQuery180 = """
select al.* 
from accelerometer_landing al
join customer_trusted ct
    on al.user = ct.email


"""
JoinCustomerTrustedtoAccelerometerLanding_node1708423452704 = sparkSqlQuery(
    glueContext,
    query=SqlQuery180,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1708423418432,
        "customer_trusted": CustomerTrusted_node1708366902512,
    },
    transformation_ctx="JoinCustomerTrustedtoAccelerometerLanding_node1708423452704",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1708423528721 = glueContext.getSink(
    path="s3://chaciu-stedi-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1708423528721",
)
AccelerometerTrusted_node1708423528721.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1708423528721.setFormat("json")
AccelerometerTrusted_node1708423528721.writeFrame(
    JoinCustomerTrustedtoAccelerometerLanding_node1708423452704
)
job.commit()
