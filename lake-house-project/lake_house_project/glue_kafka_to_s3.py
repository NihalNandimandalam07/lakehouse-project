from constructs import Construct
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_secretsmanager as secretsmanager,
)


class LakehouseKafkaToS3(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        streaming_subnet: ec2.ISubnet,
        streaming_security_group: ec2.ISecurityGroup,
        landing_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        script_deployment: Construct,
        kafka_secret_arn: str,
        kafka_bootstrap_servers: str,
        kafka_topic: str = "lakehouse-topic",
        **kwargs
    ):
        super().__init__(
            scope,
            construct_id,
            **kwargs
        )
        stack = Stack.of(self)
        # ------------------------------------------
        # 1. Import Existing Kafka Secret
        # ------------------------------------------
        kafka_secret = (
            secretsmanager.Secret.from_secret_complete_arn(
                self,
                "KafkaSecret",
                kafka_secret_arn
            )
        )
        # ------------------------------------------
        # 2. Create Glue IAM Role
        # ------------------------------------------
        glue_role = iam.Role(
            self,
            "GlueStreamingRole",
            role_name="lakehouse-glue-streaming-role-nn",
            assumed_by=iam.ServicePrincipal(
                "glue.amazonaws.com"
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ]
        )
        # ------------------------------------------
        # 3. Grant S3 and KMS Permissions
        # ------------------------------------------
        # buckets are encrypted with the lakehouse KMS key
        kms_key.grant_encrypt_decrypt(
            glue_role
        )
        landing_bucket.grant_read_write(
            glue_role
        )
        # script read + streaming checkpoints
        script_bucket.grant_read_write(
            glue_role
        )
        # ------------------------------------------
        # 4. Grant Secrets Manager Permissions
        # ------------------------------------------
        kafka_secret.grant_read(
            glue_role
        )
        # ------------------------------------------
        # 5. Glue Script Location
        # ------------------------------------------
        # kafka_to_s3.py is uploaded by the LakehouseRdsToS3 script deployment;
        # a second deployment to the same prefix would prune the other script
        script_location = (
            f"s3://{script_bucket.bucket_name}/"
            "glue-scripts/kafka_to_s3.py"
        )
        # ------------------------------------------
        # 6. Create Glue Network Connection
        # ------------------------------------------
        glue_connection = glue.CfnConnection(
            self,
            "KafkaGlueNetworkConnection",
            catalog_id=stack.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="lakehouse-kafka-network-connection-nn",
                connection_type="NETWORK",
                connection_properties={},
                physical_connection_requirements=(
                    glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        availability_zone=(
                            streaming_subnet.availability_zone
                        ),
                        subnet_id=streaming_subnet.subnet_id,
                        security_group_id_list=[
                            streaming_security_group.security_group_id
                        ]
                    )
                )
            )
        )
        # ------------------------------------------
        # 7. Create Glue Streaming Job
        # ------------------------------------------
        glue_job = glue.CfnJob(
            self,
            "KafkaToS3StreamingJob",
            name="lakehouse-kafka-to-s3-nn",
            role=glue_role.role_arn,
            glue_version="5.0",
            command=glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                python_version="3",
                script_location=script_location
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[
                    glue_connection.ref
                ]
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-job-insights": "true",
                "--KAFKA_BOOTSTRAP_SERVERS": (
                    kafka_bootstrap_servers
                ),
                "--KAFKA_TOPIC": kafka_topic,
                "--KAFKA_SECRET_ARN": kafka_secret_arn,
                "--S3_BUCKET": landing_bucket.bucket_name,
                "--CHECKPOINT_BUCKET": script_bucket.bucket_name,
                "--AWS_REGION": stack.region
            },
            worker_type="G.1X",
            number_of_workers=2,
            # streaming jobs are capped at 7 days, then Glue restarts them
            timeout=10080,
            max_retries=0,
            execution_property=(
                glue.CfnJob.ExecutionPropertyProperty(
                    max_concurrent_runs=1
                )
            )
        )
        # ------------------------------------------
        # 8. Define Deployment Dependencies
        # ------------------------------------------
        glue_job.node.add_dependency(
            script_deployment
        )
        self.glue_job = glue_job
        self.glue_role = glue_role
        self.glue_connection = glue_connection
