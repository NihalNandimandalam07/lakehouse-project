import sys
import json
import boto3

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
        "KAFKA_SECRET_ARN",
        "S3_BUCKET",
        "CHECKPOINT_BUCKET",
        "AWS_REGION"
    ]
)
bootstrap_servers = args["KAFKA_BOOTSTRAP_SERVERS"]
kafka_topic = args["KAFKA_TOPIC"]
secret_arn = args["KAFKA_SECRET_ARN"]
s3_bucket = args["S3_BUCKET"]
checkpoint_bucket = args["CHECKPOINT_BUCKET"]
aws_region = args["AWS_REGION"]

# --------------------------------------------------
# 2. Retrieve Kafka Credentials
# --------------------------------------------------
secrets_client = boto3.client(
    "secretsmanager",
    region_name=aws_region
)
secret_response = secrets_client.get_secret_value(
    SecretId=secret_arn
)
credentials = json.loads(
    secret_response["SecretString"]
)
# Keys match the kafka_credentials_testing secret
# created by the Kafka terraform project
kafka_username = credentials["username"]
kafka_password = credentials["password"]

# --------------------------------------------------
# 3. Initialize Spark Session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("LakehouseKafkaToS3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 4. Define Kafka Message Schema
# --------------------------------------------------
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# --------------------------------------------------
# 5. Configure Kafka Authentication
# --------------------------------------------------
jaas_config = (
    'org.apache.kafka.common.security.plain.'
    'PlainLoginModule required '
    f'username="{kafka_username}" '
    f'password="{kafka_password}";'
)

# --------------------------------------------------
# 6. Read Streaming Data from Kafka
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
        "SASL_PLAINTEXT"
    )
    .option(
        "kafka.sasl.mechanism",
        "PLAIN"
    )
    .option(
        "kafka.sasl.jaas.config",
        jaas_config
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)

# --------------------------------------------------
# 7. Convert Kafka Messages into JSON
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
# 8. Configure S3 Destination
# --------------------------------------------------
s3_output_path = (
    f"s3://{s3_bucket}/kafka/employees/"
)
checkpoint_path = (
    f"s3://{checkpoint_bucket}/checkpoints/kafka/employees/"
)

# --------------------------------------------------
# 9. Write Kafka Data to S3
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
# 10. Keep Streaming Job Running
# --------------------------------------------------
query.awaitTermination()
