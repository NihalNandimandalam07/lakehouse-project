from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
)


class LakehouseSecurityGroups:
    """Security groups and rules for the lakehouse.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(self, scope: Stack, vpc: ec2.IVpc) -> None:

        #MySQL SG
        self.rds_sg = ec2.SecurityGroup(
            scope,
            "LakehouseRDSSG",
            vpc= vpc,
            security_group_name="lakehouse-rds-sg-nn",
            allow_all_outbound=False
        )

        #Glue SG
        self.glue_sg = ec2.SecurityGroup(
            scope,
            "LakehouseGlueSG",
            vpc= vpc,
            security_group_name="lakehouse-glue-sg-nn",
            allow_all_outbound=False
        )

        #streaming SG
        self.stream_sg = ec2.SecurityGroup(
            scope,
            "LakehouseStreamSG",
            vpc= vpc,
            security_group_name="lakehouse-stream-sg-nn",
            allow_all_outbound=False
        )

        #EC2 SG
        self.ec2_sg = ec2.SecurityGroup(
            scope,
            "LakehouseEC2SG",
            vpc= vpc,
            security_group_name="lakehouse-ec2-sg-nn",
            allow_all_outbound=False
        )

        ##
        ##SG Rules
        ##

        #Glue to connect to MySQL
        self.rds_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.tcp(3306),
            description="Glue to access MySQL"
        )

        self.glue_sg.add_egress_rule(
            peer=self.rds_sg,
            connection=ec2.Port.tcp(3306),
            description="Glue to MySQL"
        )

        #EC2 prodcucer to connect to stream
        self.stream_sg.add_ingress_rule(
            peer=self.ec2_sg,
            connection=ec2.Port.tcp(9098),
            description="Allow EC2 kafka producer"
        )

        self.ec2_sg.add_egress_rule(
            peer=self.stream_sg,
            connection=ec2.Port.tcp(9098),
            description="EC2 to stream"
        )

        #glue stream jobs to connect tp stream
        self.stream_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.tcp(9098),
            description="Allow glue streaming consumer"
        )

        self.glue_sg.add_egress_rule(
            peer=self.stream_sg,
            connection=ec2.Port.tcp(9098),
            description="Glue to stream"
        )

        #glue to glue
        self.glue_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.all_tcp(),
            description="allowing glue internal communication"
        )

        self.glue_sg.add_egress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.all_tcp(),
            description="allowing glue internal communication"
        )

        #outputs
        CfnOutput(
            scope,
            "LakehouseRDSSGID",
            value=self.rds_sg.security_group_id
        )

        CfnOutput(
            scope,
            "LakehouseGlueSGID",
            value=self.glue_sg.security_group_id
        )

        CfnOutput(
            scope,
            "LakehouseStreamSGID",
            value=self.stream_sg.security_group_id
        )

        CfnOutput(
            scope,
            "LakehouseEC2SGID",
            value=self.ec2_sg.security_group_id
        )
