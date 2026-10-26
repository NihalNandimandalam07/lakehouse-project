from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_kms as kms,
)


class LakehouseRDS:
    """RDS MySQL source database for the lakehouse.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        vpc: ec2.IVpc,
        rds_sg: ec2.ISecurityGroup,
        kms_key: kms.IKey,
    ) -> None:

        #RDS MySQL DB
        self.mysql_db = rds.DatabaseInstance(
            scope,
            "LakehouseMySQL",
            instance_identifier="lakehouse-mysql-nn",

            engine=rds.DatabaseInstanceEngine.mysql(
                version=rds.MysqlEngineVersion.VER_8_4_9
            ),

            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO
            ),

            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_groups=[rds_sg],
            database_name="lakehousedb",

            credentials=rds.Credentials.from_generated_secret(
                username="admin",
                secret_name="lakehouse-mysql-credential-nn"
            ),
            allocated_storage=20,
            max_allocated_storage=30,
            storage_type=rds.StorageType.GP3,

            storage_encrypted=True,
            storage_encryption_key=kms_key,

            multi_az=False,
            publicly_accessible=False,

            backup_retention=Duration.days(1),

            deletion_protection=False,
            removal_policy=RemovalPolicy.SNAPSHOT
        )

        #outputs
        CfnOutput(
            scope,
            "LakehouseMySQLEndpoint",
            value=self.mysql_db.db_instance_endpoint_address
        )

        CfnOutput(
            scope,
            "LakehouseMySQLSecretARN",
            value=self.mysql_db.secret.secret_arn
        )

        CfnOutput(
            scope,
            "LakehouseMySQLPort",
            value=self.mysql_db.db_instance_endpoint_port
        )
