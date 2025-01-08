def read_kinesis_stream(stream_name, dbutils, spark):
    """
    Reads data from an AWS Kinesis stream.

    Args:
    stream_name (str): The name of the Kinesis stream to read from.
    dbutils: Databricks utility object for accessing secrets.
    spark: Spark session object.

    Returns:
    DataFrame: A Spark DataFrame representing the Kinesis stream data.
    """
    AWS_KEY = dbutils.secrets.get(scope="TestSecretBucket", key="AWS_KEY")
    AWS_SECRET_KEY = dbutils.secrets.get(scope="TestSecretBucket", key="AWS_SECRET_KEY")
    STREAM='DelNorthDataStream'
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

