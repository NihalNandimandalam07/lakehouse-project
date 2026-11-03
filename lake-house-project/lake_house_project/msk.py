import shutil
import subprocess
import sys
from pathlib import Path

import jsii
from aws_cdk import (
    Stack,
    CfnOutput,
    Duration,
    BundlingOptions,
    DockerImage,
    ILocalBundling,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kms as kms,
    aws_lambda as lambda_,
    aws_msk as msk,
    custom_resources as cr,
)


PRODUCER_DIR = Path(__file__).resolve().parent.parent / "lambda_functions" / "msk_producer"


@jsii.implements(ILocalBundling)
class _PipLocalBundling:
    """Bundles the producer Lambda with pip on this machine (no Docker needed).

    Both libraries are pure Python, so a local install runs on Lambda's Linux.
    """

    def try_bundle(self, output_dir: str, _options=None, **_kwargs) -> bool:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "--quiet", "--no-deps",
            "-r", str(PRODUCER_DIR / "requirements.txt"),
            "--target", output_dir,
        ])
        shutil.copy(PRODUCER_DIR / "handler.py", output_dir)
        return True


class LakehouseMSK:
    """MSK cluster for the streaming source, with IAM auth and a producer Lambda.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        vpc: ec2.IVpc,
        stream_sg: ec2.ISecurityGroup,
        kms_key: kms.IKey,
        cluster_name: str = "lakehouse-msk-nn",
        topic: str = "lakehouse-topic",
    ) -> None:

        self.cluster_name = cluster_name
        self.topic = topic

        #MSK Cluster
        #one t3.small broker per AZ in the isolated subnets; clients sign in with IAM (port 9098)
        self.cluster = msk.CfnCluster(
            scope,
            "LakehouseMSKCluster",
            cluster_name=cluster_name,
            kafka_version="3.9.x",
            number_of_broker_nodes=len(vpc.isolated_subnets),
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type="kafka.t3.small",
                client_subnets=[subnet.subnet_id for subnet in vpc.isolated_subnets],
                security_groups=[stream_sg.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(volume_size=10)
                ),
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_at_rest=msk.CfnCluster.EncryptionAtRestProperty(
                    data_volume_kms_key_id=kms_key.key_arn
                ),
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS",
                    in_cluster=True,
                ),
            ),
            client_authentication=msk.CfnCluster.ClientAuthenticationProperty(
                sasl=msk.CfnCluster.SaslProperty(
                    iam=msk.CfnCluster.IamProperty(enabled=True)
                ),
                unauthenticated=msk.CfnCluster.UnauthenticatedProperty(enabled=False),
            ),
        )

        #CloudFormation doesn't return the broker list, so look it up after the cluster exists
        get_brokers = cr.AwsSdkCall(
            service="Kafka",
            action="getBootstrapBrokers",
            parameters={"ClusterArn": self.cluster.attr_arn},
            physical_resource_id=cr.PhysicalResourceId.of(f"{cluster_name}-bootstrap-brokers"),
        )
        bootstrap = cr.AwsCustomResource(
            scope,
            "LakehouseMSKBootstrapBrokers",
            on_create=get_brokers,
            on_update=get_brokers,
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(resources=[self.cluster.attr_arn]),
            install_latest_aws_sdk=False,
        )
        self.bootstrap_brokers = bootstrap.get_response_field("BootstrapBrokerStringSaslIam")

        #Producer Lambda SG
        producer_sg = ec2.SecurityGroup(
            scope,
            "LakehouseMSKProducerSG",
            vpc=vpc,
            security_group_name="lakehouse-msk-producer-sg-nn",
            allow_all_outbound=False,
        )

        stream_sg.add_ingress_rule(
            peer=producer_sg,
            connection=ec2.Port.tcp(9098),
            description="Allow MSK producer Lambda",
        )

        producer_sg.add_egress_rule(
            peer=stream_sg,
            connection=ec2.Port.tcp(9098),
            description="Producer Lambda to MSK",
        )

        #Producer Lambda IAM Role
        producer_role = iam.Role(
            scope,
            "LakehouseMSKProducerRole",
            role_name="lakehouse-msk-producer-role-nn",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                )
            ],
        )

        #connect, create the topic and write to it
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"],
                resources=[self.cluster.attr_arn],
            )
        )
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka-cluster:CreateTopic", "kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"],
                resources=[self.topic_arn(scope)],
            )
        )

        #Producer Lambda
        self.producer = lambda_.Function(
            scope,
            "LakehouseMSKProducer",
            function_name="lakehouse-msk-producer-nn",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                str(PRODUCER_DIR),
                bundling=BundlingOptions(
                    image=DockerImage.from_registry("unused-local-bundling"),
                    local=_PipLocalBundling(),
                ),
            ),
            role=producer_role,
            timeout=Duration.seconds(60),
            memory_size=256,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[producer_sg],
            environment={
                "BOOTSTRAP_SERVERS": self.bootstrap_brokers,
                "KAFKA_TOPIC": topic,
            },
        )

        #outputs
        CfnOutput(
            scope,
            "LakehouseMSKClusterArn",
            value=self.cluster.attr_arn
        )

        CfnOutput(
            scope,
            "LakehouseMSKBootstrapBrokersOutput",
            value=self.bootstrap_brokers
        )

        CfnOutput(
            scope,
            "LakehouseMSKProducerName",
            value=self.producer.function_name
        )

    def topic_arn(self, scope: Stack) -> str:
        #the cluster UUID isn't known until deploy, so match any
        return f"arn:aws:kafka:{scope.region}:{scope.account}:topic/{self.cluster_name}/*/{self.topic}"

    def group_arn(self, scope: Stack) -> str:
        return f"arn:aws:kafka:{scope.region}:{scope.account}:group/{self.cluster_name}/*/*"
