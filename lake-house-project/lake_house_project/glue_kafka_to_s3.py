from constructs import Construct
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_kms as kms,
)

from lake_house_project.msk import LakehouseMSK


class LakehouseKafkaToS3(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        subnet: ec2.ISubnet,
        glue_security_group: ec2.ISecurityGroup,
        landing_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        script_deployment: Construct,
        msk_cluster: LakehouseMSK,
        **kwargs
    ):
        super().__init__(
            scope,
            construct_id,
            **kwargs
        )
        stack = Stack.of(self)
        # ------------------------------------------
        # 1. Create Glue IAM Role
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
        # 2. Grant S3 and KMS Permissions
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
        # 3. Grant MSK Permissions (IAM auth, no secret)
        # ------------------------------------------
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:Connect",
                    "kafka-cluster:DescribeCluster"
                ],
                resources=[msk_cluster.cluster.attr_arn]
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:DescribeTopic",
                    "kafka-cluster:ReadData"
                ],
                resources=[msk_cluster.topic_arn(stack)]
            )
        )
        # Spark's Kafka source uses its own consumer groups
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:AlterGroup",
                    "kafka-cluster:DescribeGroup"
                ],
                resources=[msk_cluster.group_arn(stack)]
            )
        )
        # ------------------------------------------
        # 4. Glue Script Location
        # ------------------------------------------
        # kafka_to_s3.py is uploaded by the LakehouseRdsToS3 script deployment;
        # a second deployment to the same prefix would prune the other script
        script_location = (
            f"s3://{script_bucket.bucket_name}/"
            "glue-scripts/kafka_to_s3.py"
        )
        # ------------------------------------------
        # 5. Create Glue Network Connection
        # ------------------------------------------
        # lakehouse VPC subnet with the Secrets Manager/Logs endpoints;
        # the Glue SG already allows 9098 to the MSK (stream) SG
        glue_connection = glue.CfnConnection(
            self,
            "MSKGlueNetworkConnection",
            catalog_id=stack.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="lakehouse-msk-network-connection-nn",
                connection_type="NETWORK",
                connection_properties={},
                physical_connection_requirements=(
                    glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        availability_zone=(
                            subnet.availability_zone
                        ),
                        subnet_id=subnet.subnet_id,
                        security_group_id_list=[
                            glue_security_group.security_group_id
                        ]
                    )
                )
            )
        )
        # ------------------------------------------
        # 6. Create Glue Streaming Job
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
                    msk_cluster.bootstrap_brokers
                ),
                "--KAFKA_TOPIC": msk_cluster.topic,
                "--S3_BUCKET": landing_bucket.bucket_name,
                "--CHECKPOINT_BUCKET": script_bucket.bucket_name
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
        # 7. Define Deployment Dependencies
        # ------------------------------------------
        glue_job.node.add_dependency(
            script_deployment
        )
        self.glue_job = glue_job
        self.glue_role = glue_role
        self.glue_connection = glue_connection
