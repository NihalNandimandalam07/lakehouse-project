import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# --------------------------------------------------
# 1. Glue Job Arguments
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET",
        "TRANSFORMED_BUCKET"
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
raw = f"s3://{args['RAW_BUCKET']}"
transformed = f"s3://{args['TRANSFORMED_BUCKET']}"

# --------------------------------------------------
# 4. Dataset Configuration
# --------------------------------------------------
DATASETS = [
    {
        "name": "mysql_employees",
        "source": "mysql/employees/",
        "target": "mysql/employees/"
    },
    {
        "name": "kafka_employees",
        "source": "kafka/employees/",
        "target": "kafka/employees/"
    },
    {
        "name": "flatfile_customers",
        "source": "flatfiles/customers/",
        "target": "flatfiles/customers/"
    },
    {
        "name": "covid19_activity",
        "source": "dataexchange/covid19_activity/",
        "target": "dataexchange/covid19_activity/",
        # 250 MB+ and queried by period, so partition for Athena
        "partition_by": ["year", "month"]
    }
]

# --------------------------------------------------
# 5. Standardize Column Names
# --------------------------------------------------
def clean_columns(df):
    used = set()
    for column in df.columns:
        cleaned = re.sub(
            r"[^a-zA-Z0-9_]+",
            "_",
            column
        ).strip("_").lower()
        cleaned = cleaned or "column"
        base = cleaned
        index = 2
        while cleaned in used:
            cleaned = f"{base}_{index}"
            index += 1
        used.add(cleaned)
        if column != cleaned:
            df = df.withColumnRenamed(
                column,
                cleaned
            )
    return df

# --------------------------------------------------
# 6. Basic Data Cleaning
# --------------------------------------------------
def basic_cleaning(df):
    # Remove duplicate records.
    # Exclude ingestion metadata from duplicate checks.
    metadata_columns = {
        "source_file",
        "raw_load_timestamp",
        "ingestion_timestamp"
    }
    business_columns = [
        column
        for column in df.columns
        if column not in metadata_columns
    ]
    if business_columns:
        df = df.dropDuplicates(business_columns)
    # Trim string columns and convert empty strings to null.
    for column, dtype in df.dtypes:
        if dtype == "string":
            trimmed = F.trim(F.col(column))
            df = df.withColumn(
                column,
                F.when(
                    trimmed == "",
                    F.lit(None)
                ).otherwise(trimmed)
            )
    # Add processing timestamp.
    df = df.withColumn(
        "transformed_timestamp",
        F.current_timestamp()
    )
    return df

# --------------------------------------------------
# 7. Shared Employee Transformations
# --------------------------------------------------
def add_salary_band(df):
    # MySQL lands salary as decimal(10,2), Kafka as double
    df = df.withColumn(
        "salary",
        F.col("salary").cast("double")
    )
    return df.withColumn(
        "salary_band",
        F.when(
            F.col("salary").isNull(),
            "Unknown"
        ).when(
            F.col("salary") < 50000,
            "Low"
        ).when(
            F.col("salary") < 100000,
            "Medium"
        ).otherwise("High")
    )

def standardize_department(df):
    return df.withColumn(
        "department",
        F.initcap(F.col("department"))
    )

# --------------------------------------------------
# 8. MySQL Employee Transformations
# --------------------------------------------------
def transform_mysql_employees(df):
    print("Transforming MySQL employee data")
    df = standardize_department(df)
    return add_salary_band(df)

# --------------------------------------------------
# 9. Kafka Employee Transformations
# --------------------------------------------------
def transform_kafka_employees(df):
    print("Transforming Kafka employee data")
    # The stream can carry several events per employee;
    # keep only the most recent one.
    latest_first = Window.partitionBy("employee_id").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    df = (
        df.withColumn("row_number", F.row_number().over(latest_first))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )
    df = standardize_department(df)
    return add_salary_band(df)

# --------------------------------------------------
# 10. Customer Transformations
# --------------------------------------------------
def transform_customers(df):
    print("Transforming customer data")
    # Standardize email addresses and status values.
    df = df.withColumn(
        "email",
        F.lower(F.col("email"))
    )
    df = df.withColumn(
        "customer_status",
        F.lower(F.col("customer_status"))
    )
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )
    df = df.withColumn(
        "date_of_birth",
        F.to_date(F.col("date_of_birth"))
    )
    df = df.withColumn(
        "creation_date",
        F.to_date(F.col("creation_date"))
    )
    # Customer segmentation based on annual income.
    df = df.withColumn(
        "annual_income",
        F.col("annual_income").cast("double")
    )
    df = df.withColumn(
        "income_segment",
        F.when(
            F.col("annual_income").isNull(),
            "Unknown"
        ).when(
            F.col("annual_income") >= 100000,
            "Premium"
        ).when(
            F.col("annual_income") >= 70000,
            "Gold"
        ).otherwise("Standard")
    )
    return df

# --------------------------------------------------
# 11. COVID-19 Activity Transformations
# --------------------------------------------------
def transform_covid(df):
    print("Transforming COVID-19 activity data")
    df = df.withColumn(
        "report_date",
        F.to_date(F.col("report_date"))
    )
    df = df.withColumn(
        "year",
        F.year(F.col("report_date"))
    )
    df = df.withColumn(
        "month",
        F.month(F.col("report_date"))
    )
    # Case and death counts as whole numbers.
    for column in df.columns:
        if column.startswith("people_") and column.endswith("_count"):
            df = df.withColumn(
                column,
                F.col(column).cast("long")
            )
    df = df.withColumn(
        "country_short_name",
        F.initcap(F.col("country_short_name"))
    )
    return df

TRANSFORMS = {
    "mysql_employees": transform_mysql_employees,
    "kafka_employees": transform_kafka_employees,
    "flatfile_customers": transform_customers,
    "covid19_activity": transform_covid,
}

# --------------------------------------------------
# 12. Dataset Processing
# --------------------------------------------------
for dataset in DATASETS:
    source_path = (
        f"{raw}/{dataset['source']}"
    )
    target_path = (
        f"{transformed}/{dataset['target']}"
    )
    print(
        f"Processing {dataset['name']}"
    )
    # Read Parquet from Raw bucket.
    try:
        df = spark.read.parquet(source_path)
    except AnalysisException as error:
        # a source that hasn't reached raw yet shouldn't fail the others
        print(f"Skipping {source_path}: {error}")
        continue
    # Standardize column names.
    df = clean_columns(df)
    # Apply common cleaning.
    df = basic_cleaning(df)
    # Apply dataset-specific transformations.
    df = TRANSFORMS[dataset["name"]](df)
    # Write transformed data.
    writer = df.write.mode("overwrite")
    if dataset.get("partition_by"):
        writer = writer.partitionBy(*dataset["partition_by"])
    writer.parquet(target_path)
    print(
        f"Successfully transformed "
        f"{dataset['name']} -> {target_path}"
    )

# --------------------------------------------------
# 13. Commit Glue Job
# --------------------------------------------------
job.commit()
print("Raw to Transformed ETL completed")
