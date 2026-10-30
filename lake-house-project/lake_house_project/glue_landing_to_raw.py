from aws_cdk import (
    Stack,
    CfnOutput,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_kms as kms,
)
from constructs import Construct


class LakehouseLandingToRaw:
    """Glue job moving landing zone data into the raw zone as Parquet.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        landing_bucket: s3.IBucket,
        raw_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        script_deployment: Construct,
    ) -> None:

        #Glue IAM Role
        glue_role = iam.Role(
            scope,
            "LakehouseLandingToRawRole",
            role_name="lakehouse-glue-landing-to-raw-role-nn",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        #read landing, write raw; script bucket holds the script and TempDir
        kms_key.grant_encrypt_decrypt(glue_role)
        landing_bucket.grant_read(glue_role)
        raw_bucket.grant_read_write(glue_role)
        script_bucket.grant_read_write(glue_role)

        #Glue ETL Job
        #S3 only, so no Glue connection / VPC needed
        self.job = glue.CfnJob(
            scope,
            "LakehouseLandingToRawJob",
            name="lakehouse-landing-to-raw-nn",
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{script_bucket.bucket_name}/glue-scripts/landing_to_raw.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-job-insights": "true",
                "--TempDir": f"s3://{script_bucket.bucket_name}/temp/",
                "--LANDING_BUCKET": landing_bucket.bucket_name,
                "--RAW_BUCKET": raw_bucket.bucket_name,
            },
        )

        #landing_to_raw.py is uploaded by the LakehouseRdsToS3 script deployment
        self.job.node.add_dependency(script_deployment)

        #outputs
        CfnOutput(
            scope,
            "LakehouseLandingToRawJobName",
            value=self.job.ref
        )
