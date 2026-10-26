from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_s3 as s3,
    aws_kms as kms,
    aws_ec2 as ec2,
)
from constructs import Construct

class LakeHouseProjectStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #KMS Key
        lakehouse_kms_key = kms.Key(
            self,
            "LakehouseKMSKey",
            alias = "alias/lakehouse-kms-nn",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        #SourceBucket
        source_bucket = s3.Bucket(
            self,
            "SourceBucket",
            bucket_name="lakehouse-source-nn",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=lakehouse_kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        )

        #LandingBucket
        landing_bucket = s3.Bucket(
            self,
            "LandingBucket",
            bucket_name="lakehouse-landing-nn",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=lakehouse_kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        )   

        #RawBucket
        raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            bucket_name="lakehouse-raw-nn",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=lakehouse_kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        )  

        #TransformedBucket
        transformed_bucket = s3.Bucket(
            self,
            "TransformedBucket",
            bucket_name="lakehouse-transformed-nn",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=lakehouse_kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        ) 

        #CuratedBucket
        curated_bucket = s3.Bucket(
            self,
            "CuratedBucket",
            bucket_name="lakehouse-curated-nn",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=lakehouse_kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        )  

        #Glue Assets Bucket
        glue_assets_bucket = s3.Bucket(
            self,
            "GlueAssetsBucket",
            bucket_name="lakehouse-glue-assets-nn",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=lakehouse_kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        )

        #VPC
        lakehouse_vpc = ec2.Vpc(
            self,
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
        s3_endpoint = lakehouse_vpc.add_gateway_endpoint(
            "LakehouseS3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
            subnets=[
                ec2.SubnetSelection(
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
                )
            ]
        )


        #outputs
        buckets = {
            "SourceBucketName":source_bucket,
            "LandingBucketName":landing_bucket,
            "RawBucketName":raw_bucket,
            "TransformedBucketName":transformed_bucket,
            "CuratedBucketName":curated_bucket,
            "GlueAssetsBucketName":glue_assets_bucket
        }

        for output_name, bucket in buckets.items():
            CfnOutput(
                self,
                output_name,
                value=bucket.bucket_name
            )

        CfnOutput(
            self,
            "LakehouseKMSKeyOutput",
            value=lakehouse_kms_key.key_arn
        )

        CfnOutput(
            self,
            "LakehouseVPCId",
            value=lakehouse_vpc.vpc_id
        ) 

        CfnOutput(
            self,
            "LakehousePrivateSubnets",
            value=",".join(
                subnet.subnet_id
                for subnet in lakehouse_vpc.isolated_subnets
            )
        )

        CfnOutput(
            self,
            "LakehousePublicSubnets",
            value=",".join(
                subnet.subnet_id
                for subnet in lakehouse_vpc.public_subnets
            )
        )    




