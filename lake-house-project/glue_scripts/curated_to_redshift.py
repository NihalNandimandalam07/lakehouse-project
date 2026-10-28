import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.utils import AnalysisException

# --------------------------------------------------
# 1. Glue Job Arguments
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "CURATED_BUCKET",
        "CONNECTION_NAME",
        "REDSHIFT_DATABASE",
        "REDSHIFT_ROLE_ARN",
        "REDSHIFT_TMP_DIR"
    ]
)

# --------------------------------------------------
# 2. Initialize Glue and Spark
# --------------------------------------------------
glue_context = GlueContext(
    SparkContext.getOrCreate()
)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# 3. S3 Bucket Paths
# --------------------------------------------------
curated = f"s3://{args['CURATED_BUCKET']}"

# --------------------------------------------------
# 4. Dataset Configuration
# --------------------------------------------------
# curated prefix -> Redshift table (public schema)
TABLES = {
    "employees/": "employees",
    "customers/": "customers",
    "covid/": "covid",
}

# --------------------------------------------------
# 5. Load Each Curated Dataset into Redshift
# --------------------------------------------------
for prefix, table in TABLES.items():
    source_path = f"{curated}/{prefix}"
    print(f"Loading {source_path} -> {table}")
    # Read Parquet from Curated bucket.
    try:
        df = spark.read.parquet(source_path)
    except AnalysisException as error:
        # a dataset that hasn't reached curated yet shouldn't fail the others
        print(f"Skipping {source_path}: {error}")
        continue
    frame = DynamicFrame.fromDF(df, glue_context, table)
    # Glue creates the table on the first run, then TRUNCATE runs before
    # COPY, so every run is a full refresh like the other zones.
    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=frame,
        catalog_connection=args["CONNECTION_NAME"],
        connection_options={
            "database": args["REDSHIFT_DATABASE"],
            "dbtable": f"public.{table}",
            "preactions": f"TRUNCATE TABLE public.{table};",
            "aws_iam_role": args["REDSHIFT_ROLE_ARN"],
        },
        redshift_tmp_dir=args["REDSHIFT_TMP_DIR"],
    )
    print(f"Successfully loaded {table}")

# --------------------------------------------------
# 6. Commit Glue Job
# --------------------------------------------------
job.commit()
print("Curated to Redshift load completed")
