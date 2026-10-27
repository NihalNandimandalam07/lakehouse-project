from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_s3_deployment as s3_deployment,
    aws_kms as kms,
)


class LakehouseGlue:
    """Glue JDBC connection and RDS -> S3 ingestion job for the lakehouse.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        vpc: ec2.IVpc,
        glue_sg: ec2.ISecurityGroup,
        rds_sg: ec2.ISecurityGroup,
        landing_bucket: s3.IBucket,
        script_bucket: s3.IBucket,
        kms_key: kms.IKey,
        db_endpoint: str,
        secret_arn: str,
    ) -> None:

        #Glue IAM Role
        glue_role = iam.Role(
            scope,
            "LakehouseGlueRole",
            role_name="lakehouse-glue-role-nn",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        kms_key.grant_encrypt_decrypt(glue_role)
        landing_bucket.grant_read_write(glue_role)
        script_bucket.grant_read_write(glue_role)

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[secret_arn],
            )
        )

        #Glue connection subnet (same AZ as the interface endpoints)
        subnet = vpc.isolated_subnets[0]

        #Secrets Manager and CloudWatch Logs Endpoints
        #isolated subnets have no internet, so Glue reaches these through the VPC
        glue_endpoint_sg = ec2.SecurityGroup(
            scope,
            "LakehouseGlueEndpointSG",
            vpc=vpc,
            security_group_name="lakehouse-glue-endpoint-sg-nn",
            allow_all_outbound=False
        )

        glue_endpoint_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.tcp(443),
            description="Allow HTTPS from Glue"
        )

        #443 out covers the interface endpoints and the S3 gateway endpoint;
        #there is no internet route from the isolated subnets
        glue_sg.add_egress_rule(
            peer=glue_endpoint_sg,
            connection=ec2.Port.tcp(443),
            description="Glue to VPC endpoints"
        )

        glue_sg.add_egress_rule(
            peer=ec2.Peer.prefix_list("pl-7ba54012"),
            connection=ec2.Port.tcp(443),
            description="Glue to S3 Gateway Endpoints"
        )

        glue_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.all_tcp()
        )

        glue_sg.add_egress_rule(
            peer=glue_sg,
            connection=ec2.Port.all_tcp()
        )

        glue_sg.add_egress_rule(
            peer=rds_sg,
            connection=ec2.Port.tcp(3306),
            description="Glue to RDS MySQL"
        )

        rds_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.tcp(3306),
            description="Glue to access MySQl"
        )

        endpoints = {
            "LakehouseSecretsManagerEndpoint": ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            "LakehouseLogsEndpoint": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
        }

        for construct_id, service in endpoints.items():
            ec2.InterfaceVpcEndpoint(
                scope,
                construct_id,
                vpc=vpc,
                service=service,
                private_dns_enabled=True,
                security_groups=[glue_endpoint_sg],
                subnets=ec2.SubnetSelection(subnets=[subnet]),
            )

        #JDBC Connection
        connection = glue.CfnConnection(
            scope,
            "LakehouseGlueConnection",
            catalog_id=scope.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="lakehouse-mysql-connection-nn",
                connection_type="JDBC",
                connection_properties={
                    "JDBC_CONNECTION_URL": f"jdbc:mysql://{db_endpoint}:3306/lakehousedb",
                    "SECRET_ID": secret_arn,
                },
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=subnet.subnet_id,
                    security_group_id_list=[glue_sg.security_group_id],
                    availability_zone=subnet.availability_zone,
                ),
            ),
        )

        #Upload Glue scripts to S3
        deployment = s3_deployment.BucketDeployment(
            scope,
            "LakehouseGlueScriptDeployment",
            sources=[s3_deployment.Source.asset("./glue_scripts")],
            destination_bucket=script_bucket,
            destination_key_prefix="glue-scripts",
        )

        #Glue ETL Job
        self.job = glue.CfnJob(
            scope,
            "LakehouseRdsToS3Job",
            name="lakehouse-rds-to-s3-nn",
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{script_bucket.bucket_name}/glue-scripts/rds_to_s3.py",
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[connection.ref]
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-job-insights": "true",
                "--TempDir": f"s3://{script_bucket.bucket_name}/temp/",
                "--CONNECTION_NAME": connection.ref,
                "--LANDING_BUCKET": landing_bucket.bucket_name,
            },
        )

        self.job.node.add_dependency(deployment)

        #outputs
        CfnOutput(
            scope,
            "LakehouseGlueJobName",
            value=self.job.ref
        )
