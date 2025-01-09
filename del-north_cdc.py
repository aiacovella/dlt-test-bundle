# Databricks notebook source
import dlt
from pyspark.sql.functions import col, from_json, decode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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


@dlt.table(name="kinesis_raw_cdc_stream", table_properties={"pipelines.reset.allowed": "false"})
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
    user_schema = StructType([
        StructField("metadata", StructType([
            StructField("table-name", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ]), True),
        StructField("data", StructType([
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
        ]), True),
    ])

    return (
        dlt.readStream("kinesis_raw_cdc_stream")
        .select(decode(col("data"), "UTF-8").alias("json_string"))
        .select(from_json(col("json_string"), user_schema).alias("json_data")) \
        .filter(col("json_data.metadata.table-name") == "users") \
        .select("json_data.metadata.timestamp", "json_data.data.id", "json_data.data.first_name",
            "json_data.data.last_name", "json_data.data.email","json_data.data.phone_number", "json_data.data.age", "json_data.data.address", "json_data.data.city", "json_data.data.state", "json_data.data.zip")
    )

@dlt.table(name="organizations_bronze")
def organizations_bronze():
    org_schema = StructType([
        StructField("metadata", StructType([
            StructField("table-name", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ]), True),
        StructField("data", StructType([
            StructField("id", IntegerType(), True),
            StructField("org_name", StringType(), True),
            StructField("region", StringType(), True)
        ]), True),
    ])

    return (
        dlt.readStream("kinesis_raw_cdc_stream") \
            .select(decode(col("data"), "UTF-8").alias("json_string")) \
            .select(from_json(col("json_string"), org_schema).alias("json_data")) \
            .filter(col("json_data.metadata.table-name") == "organizations") \
            .select("json_data.metadata.timestamp", "json_data.data.id", "json_data.data.org_name",
                    "json_data.data.region")
    )

# COMMAND ----------


# COMMAND ----------


# COMMAND ----------


# COMMAND ----------
