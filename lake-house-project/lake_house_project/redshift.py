import json

from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_kms as kms,
    aws_secretsmanager as secretsmanager,
    aws_redshiftserverless as redshiftserverless,
)


class LakehouseRedshift:
    """Redshift Serverless warehouse loaded from the curated zone.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        vpc: ec2.IVpc,
        glue_sg: ec2.ISecurityGroup,
        staging_bucket: s3.IBucket,
        kms_key: kms.IKey,
        database_name: str = "lakehouse_dw",
    ) -> None:

        self.database_name = database_name

        #Redshift SG
        self.redshift_sg = ec2.SecurityGroup(
            scope,
            "LakehouseRedshiftSG",
            vpc=vpc,
            security_group_name="lakehouse-redshift-sg-nn",
            allow_all_outbound=False
        )

        #Glue to connect to Redshift
        self.redshift_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.tcp(5439),
            description="Glue to access Redshift"
        )

        glue_sg.add_egress_rule(
            peer=self.redshift_sg,
            connection=ec2.Port.tcp(5439),
            description="Glue to Redshift"
        )

        #Admin credentials (username/password keys, as the Glue connection expects)
        self.secret = secretsmanager.Secret(
            scope,
            "LakehouseRedshiftSecret",
            secret_name="lakehouse-redshift-credential-nn",
            encryption_key=kms_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": "lakehouseadmin"}),
                generate_string_key="password",
                #Redshift rejects ' " \ / @ and spaces in passwords
                exclude_characters="'\"\\/@ ",
                require_each_included_type=True,
                password_length=32,
            ),
        )

        #Redshift IAM Role
        #COPY uses this role to read the files Glue stages in the TempDir
        self.role = iam.Role(
            scope,
            "LakehouseRedshiftRole",
            role_name="lakehouse-redshift-role-nn",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("redshift.amazonaws.com"),
                iam.ServicePrincipal("redshift-serverless.amazonaws.com"),
            ),
        )

        kms_key.grant_decrypt(self.role)
        staging_bucket.grant_read(self.role, "temp/redshift/*")

        #Namespace
        self.namespace = redshiftserverless.CfnNamespace(
            scope,
            "LakehouseRedshiftNamespace",
            namespace_name="lakehouse-namespace-nn",
            db_name=database_name,
            admin_username="lakehouseadmin",
            admin_user_password=self.secret.secret_value_from_json("password").unsafe_unwrap(),
            kms_key_id=kms_key.key_arn,
            iam_roles=[self.role.role_arn],
            default_iam_role_arn=self.role.role_arn,
        )

        #Workgroup
        #without enhanced VPC routing two AZs are enough
        self.workgroup = redshiftserverless.CfnWorkgroup(
            scope,
            "LakehouseRedshiftWorkgroup",
            workgroup_name="lakehouse-workgroup-nn",
            namespace_name=self.namespace.namespace_name,
            base_capacity=8,
            publicly_accessible=False,
            enhanced_vpc_routing=False,
            subnet_ids=[subnet.subnet_id for subnet in vpc.isolated_subnets],
            security_group_ids=[self.redshift_sg.security_group_id],
        )
        self.workgroup.node.add_dependency(self.namespace)

        self.endpoint = self.workgroup.attr_workgroup_endpoint_address

        #outputs
        CfnOutput(
            scope,
            "LakehouseRedshiftWorkgroupName",
            value=self.workgroup.workgroup_name
        )

        CfnOutput(
            scope,
            "LakehouseRedshiftEndpoint",
            value=self.endpoint
        )

        CfnOutput(
            scope,
            "LakehouseRedshiftSecretArn",
            value=self.secret.secret_arn
        )
