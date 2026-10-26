from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
)


class LakehouseNetworking:
    """VPC, subnets and S3 gateway endpoint for the lakehouse.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(self, scope: Stack) -> None:

        #VPC
        self.vpc = ec2.Vpc(
            scope,
            "lakehouseVPC",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            nat_gateways=2,

            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="lakehouse-public-nn",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="lakehouse-private-nn",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24
                )
            ]
        )

        #S3 Gateway Endpoint
        self.s3_endpoint = self.vpc.add_gateway_endpoint(
            "LakehouseS3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
            subnets=[
                ec2.SubnetSelection(
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
                )
            ]
        )

        #outputs
        CfnOutput(
            scope,
            "LakehouseVPCId",
            value=self.vpc.vpc_id
        )

        CfnOutput(
            scope,
            "LakehousePrivateSubnets",
            value=",".join(
                subnet.subnet_id
                for subnet in self.vpc.isolated_subnets
            )
        )

        CfnOutput(
            scope,
            "LakehousePublicSubnets",
            value=",".join(
                subnet.subnet_id
                for subnet in self.vpc.public_subnets
            )
        )
