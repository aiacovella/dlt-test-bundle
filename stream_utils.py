from typing import List

import dlt
from pyspark.sql.functions import col, from_json, decode, expr

def read_kinesis_stream(stream_name: str, dbutils, spark):
    """
    Reads a real-time stream from an Amazon Kinesis Data Stream.

    This function accesses the stream using the AWS key and secret key obtained from
    the Databricks Secrets API. The stream is read using the Spark Structured Streaming
    API with the initial position set to "earliest". The AWS region is configured as
    "us-east-1". The output of this function can be utilized for further stream processing
    in a Spark environment.

    :param stream_name: Name of the Amazon Kinesis Data Stream to read from
    :type stream_name: str
    :param dbutils: Databricks Utilities object to access secrets for AWS authentication
    :type dbutils: object
    :param spark: Spark session for creating the data stream
    :type spark: object
    :return: A streaming DataFrame containing the data read from the Amazon Kinesis Data Stream
    :rtype: pyspark.sql.DataFrame
    """
    AWS_KEY = dbutils.secrets.get(scope="TestSecretBucket", key="AWS_KEY")
    AWS_SECRET_KEY = dbutils.secrets.get(scope="TestSecretBucket", key="AWS_SECRET_KEY")
    REGION="us-east-1"

    print(AWS_KEY)
    print(AWS_SECRET_KEY)

    return (spark
            .readStream
            .format("kinesis")
            .option("streamName", stream_name)
            .option("region", REGION)
            .option("initialPosition", 'earliest')
            .option("awsAccessKey", AWS_KEY)
            .option("awsSecretKey", AWS_SECRET_KEY)
            .load()
           )


def apply_changes(target_table: str, source_view: str, primary_keys: List[str]):
    """
    Applies changes from a source view to a target table using the specified primary keys. This function
    leverages Delta Live Tables (dlt) capabilities to manage change data capture (CDC) operations, ensuring
    updates, inserts, and deletes are appropriately handled. The function creates the target table as a
    streaming table and applies changes based on a timestamp sequence while allowing for specific operations
    like deletes and SCD type 1 transformations.

    :param target_table: The name of the target table to which changes will be applied.
    :param source_view: The source view containing updated data for CDC operations.
    :param primary_keys: A list of column names that constitute the primary keys for the CDC process.
    :return: None
    """
    dlt.create_streaming_table(target_table)
    dlt.apply_changes(
        target=target_table,
        source=source_view,
        keys=primary_keys,
        sequence_by=col("timestamp"),
        apply_as_deletes=expr("operation = 'delete'"),
        apply_as_truncates=None,
        except_column_list=[],
        stored_as_scd_type=1
    )
