import re
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.utils import AnalysisException


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "LANDING_BUCKET",
        "RAW_BUCKET",
    ],
)

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

landing = f"s3://{args['LANDING_BUCKET']}"
raw = f"s3://{args['RAW_BUCKET']}"

# one entry per source in the landing zone -> its raw zone prefix
DATASETS = [
    {"source": "mysql/employees/", "format": "parquet", "target": "mysql/employees/"},
    # read the folder (not a glob) so Spark uses _spark_metadata and skips uncommitted files
    {"source": "kafka/employees/", "format": "parquet", "target": "kafka/employees/"},
    {"source": "flatfiles/*.csv", "format": "csv", "target": "flatfiles/customers/"},
    # Data Exchange revisions land under a revision id folder
    {"source": "third-party-covid19/published/PUBLIC/COVID-19-Activity/*/*.csv", "format": "csv", "target": "dataexchange/covid19_activity/"},
]


def clean_columns(df):
    # Parquet rejects spaces and ,;{}()= in column names
    for name in df.columns:
        clean = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
        if clean != name:
            df = df.withColumnRenamed(name, clean)
    return df


for dataset in DATASETS:
    source_path = f"{landing}/{dataset['source']}"
    target_path = f"{raw}/{dataset['target']}"

    try:
        if dataset["format"] == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
        else:
            df = spark.read.parquet(source_path)
    except AnalysisException as error:
        # a source that hasn't landed yet shouldn't fail the other datasets
        print(f"Skipping {source_path}: {error}")
        continue

    df = (
        clean_columns(df)
        .withColumn("source_file", input_file_name())
        .withColumn("raw_load_timestamp", current_timestamp())
    )

    # raw is a full snapshot of landing, so reruns are idempotent
    df.write.mode("overwrite").parquet(target_path)

    print(f"Loaded {source_path} -> {target_path}")

job.commit()
