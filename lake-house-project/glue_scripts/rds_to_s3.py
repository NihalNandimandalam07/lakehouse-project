import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "CONNECTION_NAME",
        "LANDING_BUCKET",
    ],
)

glue_context = GlueContext(SparkContext.getOrCreate())
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# URL and credentials (from Secrets Manager) come from the Glue connection
dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": args["CONNECTION_NAME"],
        "dbtable": "employees",
    },
)

# cache so the count and the write don't each re-read MySQL
df = dyf.toDF().cache()
record_count = df.count()

output_path = f"s3://{args['LANDING_BUCKET']}/mysql/employees/"

df.write.mode("overwrite").parquet(output_path)

print(f"Successfully ingested {record_count} employee records")

job.commit()
