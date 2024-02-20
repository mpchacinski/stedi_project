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

# Script generated for node Customer Landing
CustomerLanding_node1708249666061 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaciu-stedi-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1708249666061",
)

# Script generated for node SQL Query
SqlQuery201 = """
select * from myDataSource
where sharewithresearchasofdate != 0
"""
SQLQuery_node1708277790929 = sparkSqlQuery(
    glueContext,
    query=SqlQuery201,
    mapping={"myDataSource": CustomerLanding_node1708249666061},
    transformation_ctx="SQLQuery_node1708277790929",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1708277819412 = glueContext.getSink(
    path="s3://chaciu-stedi-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1708277819412",
)
CustomerTrusted_node1708277819412.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="customer_trusted"
)
CustomerTrusted_node1708277819412.setFormat("json")
CustomerTrusted_node1708277819412.writeFrame(SQLQuery_node1708277790929)
job.commit()
