# Databricks notebook source
import dlt
from pyspark.sql.functions import col, from_json, decode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

sys.path.append("./shared")
from stream_utils import read_kinesis_stream

# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
# param = dbutils.widgets.get("FOO_PARAM")
# print(param)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


AWS_KEY = dbutils.secrets.get(scope="TestSecretBucket", key="AWS_KEY")
AWS_SECRET_KEY = dbutils.secrets.get(scope="TestSecretBucket", key="AWS_SECRET_KEY")
STREAM = 'DelNorthDataStream'
REGION = "us-east-1"

print(AWS_KEY)
print(AWS_SECRET_KEY)


@dlt.table(name="kinesis_raw_stream", table_properties={"pipelines.reset.allowed": "false"})
def kinesis_raw_stream():
    return read_kinesis_stream(stream_name=STREAM, dbutils=dbutils, spark=spark)


raw_schema = StructType([
    StructField("metadata", StructType([
        StructField("table-name", StringType(), True)
    ]), True),
    StructField("data", StringType(), True)
])


@dlt.table(name="users_bronze")
def users_bronze():
    users_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ])

    return (
        dlt.readStream("kinesis_raw_stream")
        .select(decode(col("data"), "UTF-8").alias("json_string"))
        .select(from_json(col("json_string"), raw_schema).alias("json_data"))
        .filter(col("json_data.metadata.table-name") == "users")
        .select(from_json(col("json_data.data"), users_schema).alias("user_data"))
        .select("user_data.id", "user_data.first_name", "user_data.last_name", "user_data.email",
                "user_data.phone_number", "user_data.age", "user_data.address", "user_data.city", "user_data.state",
                "user_data.zip")
    )



@dlt.table(name="organizations_bronze")
def organizations_bronze():
    org_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("org_name", StringType(), True),
        StructField("region", StringType(), True)
    ])

    return (
        dlt.readStream("kinesis_raw_stream")
        .select(decode(col("data"), "UTF-8").alias("json_string"))
        .select(from_json(col("json_string"), raw_schema).alias("json_data"))
        .filter(col("json_data.metadata.table-name") == "organizations")
        .select(from_json(col("json_data.data"), org_schema).alias("org_data"))
        .select("org_data.id", "org_data.org_name", "org_data.region")
    )

# COMMAND ----------


# COMMAND ----------


# COMMAND ----------


# COMMAND ----------
