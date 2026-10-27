from aws_cdk import (
    CfnTag,
    aws_ec2 as ec2,
)
from constructs import Construct


class LakehouseStreamingNetworking(Construct):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        kafka_vpc_id: str,
        kafka_vpc_cidr: str,
        kafka_route_table_id: str,
        kafka_sg_id: str,
    ) -> None:

        super().__init__(scope, construct_id)

        # Create a dedicated VPC for Glue streaming
        self.vpc = ec2.Vpc(
            self,
            "StreamingVPC",
            vpc_name="lakehouse-streaming-vpc-nn",
            ip_addresses=ec2.IpAddresses.cidr("10.1.0.0/16"),
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="StreamingIsolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
            enable_dns_support=True,
            enable_dns_hostnames=True,
        )

        # S3 Gateway Endpoint
        self.s3_endpoint = self.vpc.add_gateway_endpoint(
            "StreamingS3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # Glue streaming security group
        self.glue_sg = ec2.SecurityGroup(
            self,
            "StreamingGlueSG",
            vpc=self.vpc,
            security_group_name="lakehouse-streaming-glue-sg-nn",
            description="Security group for Glue Kafka streaming",
            allow_all_outbound=False,
        )

        # Glue workers need internal communication
        self.glue_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.all_tcp(),
            description="Allow Glue worker communication",
        )

        self.glue_sg.add_egress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.all_tcp(),
            description="Allow Glue worker outbound communication",
        )

        # Glue -> S3 through the gateway endpoint
        self.glue_sg.add_egress_rule(
            peer=ec2.Peer.prefix_list("pl-7ba54012"),
            connection=ec2.Port.tcp(443),
            description="Glue to S3 Gateway Endpoint",
        )

        # Secrets Manager and CloudWatch Logs Endpoints
        # isolated subnets have no internet, so Glue reaches these through the VPC
        endpoint_sg = ec2.SecurityGroup(
            self,
            "StreamingEndpointSG",
            vpc=self.vpc,
            security_group_name="lakehouse-streaming-endpoint-sg-nn",
            description="Interface endpoints for Glue Kafka streaming",
            allow_all_outbound=False,
        )

        endpoint_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.tcp(443),
            description="Allow HTTPS from Glue",
        )

        self.glue_sg.add_egress_rule(
            peer=endpoint_sg,
            connection=ec2.Port.tcp(443),
            description="Glue to VPC endpoints",
        )

        endpoints = {
            "StreamingSecretsManagerEndpoint": ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            "StreamingLogsEndpoint": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
        }

        # Same subnet as the Glue connection
        for endpoint_id, service in endpoints.items():
            ec2.InterfaceVpcEndpoint(
                self,
                endpoint_id,
                vpc=self.vpc,
                service=service,
                private_dns_enabled=True,
                security_groups=[endpoint_sg],
                subnets=ec2.SubnetSelection(subnets=[self.vpc.isolated_subnets[0]]),
            )

        # VPC peering to the self-managed Kafka VPC (IAC-apache-kafka-cluster).
        # The lakehouse VPC overlaps the Kafka VPC (both 10.0.0.0/16), which is
        # why streaming runs in its own VPC. Same account, so it auto-accepts.
        self.kafka_peering = ec2.CfnVPCPeeringConnection(
            self,
            "KafkaPeering",
            vpc_id=self.vpc.vpc_id,
            peer_vpc_id=kafka_vpc_id,
            tags=[CfnTag(key="Name", value="kafka-lakehouse-peering-nn")],
        )

        # Streaming subnets -> Kafka VPC
        for index, subnet in enumerate(self.vpc.isolated_subnets, start=1):
            ec2.CfnRoute(
                self,
                f"KafkaPeeringRoute{index}",
                route_table_id=subnet.route_table.route_table_id,
                destination_cidr_block=kafka_vpc_cidr,
                vpc_peering_connection_id=self.kafka_peering.ref,
            )

        # Kafka VPC -> streaming subnets (route table owned by the Kafka terraform)
        ec2.CfnRoute(
            self,
            "KafkaReturnRoute",
            route_table_id=kafka_route_table_id,
            destination_cidr_block=self.vpc.vpc_cidr_block,
            vpc_peering_connection_id=self.kafka_peering.ref,
        )

        # Glue -> Kafka brokers (SASL_PLAINTEXT listener)
        self.glue_sg.add_egress_rule(
            peer=ec2.Peer.ipv4(kafka_vpc_cidr),
            connection=ec2.Port.tcp(9092),
            description="Glue to Kafka brokers",
        )

        # Kafka brokers accept the streaming VPC (SG owned by the Kafka terraform)
        ec2.CfnSecurityGroupIngress(
            self,
            "KafkaBrokerIngressFromStreaming",
            group_id=kafka_sg_id,
            ip_protocol="tcp",
            from_port=9092,
            to_port=9092,
            cidr_ip=self.vpc.vpc_cidr_block,
            description="Glue streaming to Kafka brokers",
        )