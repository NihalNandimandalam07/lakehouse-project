import sys
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
        "TRANSFORMED_BUCKET",
        "CURATED_BUCKET"
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
transformed = f"s3://{args['TRANSFORMED_BUCKET']}"
curated = f"s3://{args['CURATED_BUCKET']}"

# --------------------------------------------------
# 4. Helpers
# --------------------------------------------------
def read_transformed(prefix):
    path = f"{transformed}/{prefix}"
    try:
        return spark.read.parquet(path)
    except AnalysisException as error:
        # a dataset that hasn't reached transformed yet shouldn't fail the others
        print(f"Skipping {path}: {error}")
        return None

def write_curated(df, prefix, partition_by=None):
    path = f"{curated}/{prefix}"
    df = df.withColumn(
        "curated_timestamp",
        F.current_timestamp()
    )
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    print(f"Successfully curated -> {path}")

# --------------------------------------------------
# 5. Employees (MySQL + Kafka)
# --------------------------------------------------
def curate_employees(mysql_df, kafka_df):
    print("Curating employees")
    columns = [
        "employee_id",
        "employee_name",
        "department",
        "salary",
        "salary_band"
    ]
    sources = []
    if mysql_df is not None:
        sources.append(
            mysql_df.select(
                *columns,
                F.lit("mysql").alias("source"),
                F.col("raw_load_timestamp").alias("updated_at")
            )
        )
    if kafka_df is not None:
        sources.append(
            kafka_df.select(
                *columns,
                F.lit("kafka").alias("source"),
                F.col("ingestion_timestamp").cast("timestamp").alias("updated_at")
            )
        )
    df = sources[0]
    for other in sources[1:]:
        df = df.unionByName(other)
    # One row per employee: keep the most recent record from either source.
    latest_first = Window.partitionBy("employee_id").orderBy(
        F.col("updated_at").desc()
    )
    return (
        df.withColumn("row_number", F.row_number().over(latest_first))
        .filter(F.col("row_number") == 1)
        .drop("row_number", "updated_at")
    )

# --------------------------------------------------
# 6. Customers
# --------------------------------------------------
def curate_customers(df):
    print("Curating customers")
    return df.select(
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "first_name",
        "last_name",
        "date_of_birth",
        "email",
        "phone",
        "address_state",
        "customer_status",
        "annual_income",
        "creation_date"
    )

# --------------------------------------------------
# 7. COVID-19 Daily Totals by Country
# --------------------------------------------------
def curate_covid(df):
    print("Curating COVID-19 activity")
    return (
        df.groupBy(
            "report_date",
            "country_short_name",
            "year",
            "month"
        )
        .agg(
            F.sum("people_positive_new_cases_count").alias("new_cases"),
            F.sum("people_death_new_count").alias("new_deaths")
        )
    )

# --------------------------------------------------
# 8. Dataset Processing
# --------------------------------------------------
mysql_employees = read_transformed("mysql/employees/")
kafka_employees = read_transformed("kafka/employees/")
if mysql_employees is not None or kafka_employees is not None:
    write_curated(
        curate_employees(mysql_employees, kafka_employees),
        "employees/"
    )

customers = read_transformed("flatfiles/customers/")
if customers is not None:
    write_curated(
        curate_customers(customers),
        "customers/"
    )

covid = read_transformed("dataexchange/covid19_activity/")
if covid is not None:
    write_curated(
        curate_covid(covid),
        "covid/",
        partition_by=["year", "month"]
    )

# --------------------------------------------------
# 9. Commit Glue Job
# --------------------------------------------------
job.commit()
print("Transformed to Curated ETL completed")
