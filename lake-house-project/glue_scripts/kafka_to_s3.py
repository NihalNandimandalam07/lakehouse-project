import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

# --------------------------------------------------
# 1. Read Glue Job Parameters
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
        "S3_BUCKET",
        "CHECKPOINT_BUCKET"
    ]
)
bootstrap_servers = args["KAFKA_BOOTSTRAP_SERVERS"]
kafka_topic = args["KAFKA_TOPIC"]
s3_bucket = args["S3_BUCKET"]
checkpoint_bucket = args["CHECKPOINT_BUCKET"]

# --------------------------------------------------
# 2. Initialize Spark Session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("LakehouseKafkaToS3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 3. Define Kafka Message Schema
# --------------------------------------------------
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# --------------------------------------------------
# 4. Read Streaming Data from MSK
# --------------------------------------------------
# IAM auth over TLS (port 9098): the Glue job role signs in,
# so there is no username or password
# --------------------------------------------------
kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        bootstrap_servers
    )
    .option(
        "subscribe",
        kafka_topic
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "kafka.security.protocol",
        "SASL_SSL"
    )
    .option(
        "kafka.sasl.mechanism",
        "AWS_MSK_IAM"
    )
    .option(
        "kafka.sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required;"
    )
    .option(
        "kafka.sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)

# --------------------------------------------------
# 5. Convert Kafka Messages into JSON
# --------------------------------------------------
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_message"
)
employee_df = (
    json_df
    .select(
        from_json(
            col("json_message"),
            employee_schema
        ).alias("data")
    )
    .select("data.*")
    .filter(col("employee_id").isNotNull())
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
)

# --------------------------------------------------
# 6. Configure S3 Destination
# --------------------------------------------------
s3_output_path = (
    f"s3://{s3_bucket}/kafka/employees/"
)
# offsets are per cluster, so MSK gets its own checkpoint folder
checkpoint_path = (
    f"s3://{checkpoint_bucket}/checkpoints/msk/employees/"
)

# --------------------------------------------------
# 7. Write Kafka Data to S3
# --------------------------------------------------
query = (
    employee_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        s3_output_path
    )
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .trigger(
        processingTime="60 seconds"
    )
    .start()
)

# --------------------------------------------------
# 8. Keep Streaming Job Running
# --------------------------------------------------
query.awaitTermination()
