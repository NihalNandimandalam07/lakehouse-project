from aws_cdk import (
    Stack,
    CfnOutput,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_kms as kms,
)
from constructs import Construct


class LakehouseTransformedToCurated:
    """Glue job building business-ready datasets in the curated zone.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        transformed_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        script_deployment: Construct,
    ) -> None:

        #Glue IAM Role
        glue_role = iam.Role(
            scope,
            "LakehouseTransformedToCuratedRole",
            role_name="lakehouse-glue-transformed-to-curated-role-nn",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        #read transformed, write curated; script bucket holds the script and TempDir
        kms_key.grant_encrypt_decrypt(glue_role)
        transformed_bucket.grant_read(glue_role)
        curated_bucket.grant_read_write(glue_role)
        script_bucket.grant_read_write(glue_role)

        #Glue ETL Job
        self.job = glue.CfnJob(
            scope,
            "LakehouseTransformedToCuratedJob",
            name="lakehouse-transformed-to-curated-nn",
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{script_bucket.bucket_name}/glue-scripts/transformed_to_curated.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-job-insights": "true",
                "--TempDir": f"s3://{script_bucket.bucket_name}/temp/",
                "--TRANSFORMED_BUCKET": transformed_bucket.bucket_name,
                "--CURATED_BUCKET": curated_bucket.bucket_name,
            },
        )

        #transformed_to_curated.py is uploaded by the LakehouseRdsToS3 script deployment
        self.job.node.add_dependency(script_deployment)

        #outputs
        CfnOutput(
            scope,
            "LakehouseTransformedToCuratedJobName",
            value=self.job.ref
        )
