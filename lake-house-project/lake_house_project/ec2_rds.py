from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
)


class LakehouseEC2:
    """EC2 bastion (SSM port forwarding) and SSM endpoints for reaching RDS.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
            self,
            scope: Stack,
            vpc: ec2.IVpc,
            rds_sg: ec2.ISecurityGroup,
    ) -> None:

        #single AZ to keep interface endpoint costs down
        private_subnets = ec2.SubnetSelection(
            subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
            availability_zones=[vpc.availability_zones[0]]
        )

        #EC2 IAM role
        ec2_role = iam.Role(
            scope,
            "lakehouseEC2Role",
            role_name="lakehouse-ec2-role-nn",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")
            ]
        )

        #Security Group - Bastion EC2
        self.bastion_sg = ec2.SecurityGroup(
            scope,
            "LakehouseBastionSG",
            vpc=vpc,
            security_group_name="lakehouse-bastion-sg-nn",
            allow_all_outbound=False
        )

        #SSM Endpoints SG
        ssm_sg = ec2.SecurityGroup(
            scope,
            "LakehouseSSMSG",
            vpc=vpc,
            security_group_name="lakehouse-ssm-sg-nn",
            allow_all_outbound=False
        )

        ##
        ##SG Rules
        ##

        #Bastion to connect to RDS MySQL
        rds_sg.add_ingress_rule(
            peer=self.bastion_sg,
            connection=ec2.Port.tcp(3306),
            description="Bastion to access MySQL"
        )

        self.bastion_sg.add_egress_rule(
            peer=rds_sg,
            connection=ec2.Port.tcp(3306),
            description="Bastion to MySQL"
        )

        #Bastion to connect to SSM endpoints
        ssm_sg.add_ingress_rule(
            peer=self.bastion_sg,
            connection=ec2.Port.tcp(443),
            description="Allow HTTPS from bastion"
        )

        self.bastion_sg.add_egress_rule(
            peer=ssm_sg,
            connection=ec2.Port.tcp(443),
            description="Bastion to SSM endpoints"
        )

        #Bastion EC2
        self.instance = ec2.Instance(
            scope,
            "LakehouseEC2",
            instance_name="lakehouse-ec2-nn",

            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO
            ),

            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            vpc=vpc,
            vpc_subnets=private_subnets,

            security_group=self.bastion_sg,
            role=ec2_role,
            require_imdsv2=True
        )

        #SSM, SSM Messages and EC2 Messages Endpoints
        endpoints = {
            "LakehouseSSMEndpoint": ec2.InterfaceVpcEndpointAwsService.SSM,
            "LakehouseSSMMessagesEndpoint": ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES,
            "LakehouseEC2MessagesEndpoint": ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES,
        }

        for construct_id, service in endpoints.items():
            ec2.InterfaceVpcEndpoint(
                scope,
                construct_id,
                vpc=vpc,
                service=service,
                private_dns_enabled=True,
                security_groups=[ssm_sg],
                subnets=private_subnets,
            )

        #outputs
        CfnOutput(
            scope,
            "LakehouseEC2InstanceID",
            value=self.instance.instance_id
        )
