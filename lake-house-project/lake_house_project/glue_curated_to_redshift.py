from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_kms as kms,
)
from constructs import Construct

from lake_house_project.redshift import LakehouseRedshift


class LakehouseCuratedToRedshift:
    """Glue JDBC connection and job loading the curated zone into Redshift.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        vpc: ec2.IVpc,
        glue_sg: ec2.ISecurityGroup,
        curated_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        redshift: LakehouseRedshift,
        script_deployment: Construct,
    ) -> None:

        #Glue IAM Role
        glue_role = iam.Role(
            scope,
            "LakehouseCuratedToRedshiftRole",
            role_name="lakehouse-glue-curated-to-redshift-role-nn",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        #read curated, stage in the script bucket's TempDir for COPY
        kms_key.grant_encrypt_decrypt(glue_role)
        curated_bucket.grant_read(glue_role)
        script_bucket.grant_read_write(glue_role)
        redshift.secret.grant_read(glue_role)

        #Glue connection subnet (same AZ as the Secrets Manager and Logs endpoints)
        subnet = vpc.isolated_subnets[0]

        #JDBC Connection
        connection = glue.CfnConnection(
            scope,
            "LakehouseRedshiftConnection",
            catalog_id=scope.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="lakehouse-redshift-connection-nn",
                connection_type="JDBC",
                connection_properties={
                    "JDBC_CONNECTION_URL": f"jdbc:redshift://{redshift.endpoint}:5439/{redshift.database_name}",
                    "SECRET_ID": redshift.secret.secret_arn,
                },
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=subnet.subnet_id,
                    security_group_id_list=[glue_sg.security_group_id],
                    availability_zone=subnet.availability_zone,
                ),
            ),
        )

        #Glue ETL Job
        self.job = glue.CfnJob(
            scope,
            "LakehouseCuratedToRedshiftJob",
            name="lakehouse-curated-to-redshift-nn",
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{script_bucket.bucket_name}/glue-scripts/curated_to_redshift.py",
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[connection.ref]
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-job-insights": "true",
                "--TempDir": f"s3://{script_bucket.bucket_name}/temp/",
                "--CURATED_BUCKET": curated_bucket.bucket_name,
                "--CONNECTION_NAME": connection.ref,
                "--REDSHIFT_DATABASE": redshift.database_name,
                "--REDSHIFT_ROLE_ARN": redshift.role.role_arn,
                "--REDSHIFT_TMP_DIR": f"s3://{script_bucket.bucket_name}/temp/redshift/",
            },
        )

        #curated_to_redshift.py is uploaded by the LakehouseRdsToS3 script deployment;
        #the workgroup must exist before the connection can reach it
        self.job.node.add_dependency(script_deployment)
        connection.node.add_dependency(redshift.workgroup)

        #outputs
        CfnOutput(
            scope,
            "LakehouseCuratedToRedshiftJobName",
            value=self.job.ref
        )
