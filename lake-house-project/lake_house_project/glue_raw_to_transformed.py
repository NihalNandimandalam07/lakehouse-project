from aws_cdk import (
    Stack,
    CfnOutput,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_kms as kms,
)
from constructs import Construct


class LakehouseRawToTransformed:
    """Glue job cleaning raw zone data into the transformed zone.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        raw_bucket: s3.IBucket,
        transformed_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        script_deployment: Construct,
    ) -> None:

        #Glue IAM Role
        glue_role = iam.Role(
            scope,
            "LakehouseRawToTransformedRole",
            role_name="lakehouse-glue-raw-to-transformed-role-nn",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        #read raw, write transformed; script bucket holds the script and TempDir
        kms_key.grant_encrypt_decrypt(glue_role)
        raw_bucket.grant_read(glue_role)
        transformed_bucket.grant_read_write(glue_role)
        script_bucket.grant_read_write(glue_role)

        #Glue ETL Job
        self.job = glue.CfnJob(
            scope,
            "LakehouseRawToTransformedJob",
            name="lakehouse-raw-to-transformed-nn",
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{script_bucket.bucket_name}/glue-scripts/raw_to_transformed.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-job-insights": "true",
                "--TempDir": f"s3://{script_bucket.bucket_name}/temp/",
                "--RAW_BUCKET": raw_bucket.bucket_name,
                "--TRANSFORMED_BUCKET": transformed_bucket.bucket_name,
            },
        )

        #raw_to_transformed.py is uploaded by the LakehouseGlue script deployment
        self.job.node.add_dependency(script_deployment)

        #outputs
        CfnOutput(
            scope,
            "LakehouseRawToTransformedJobName",
            value=self.job.ref
        )
