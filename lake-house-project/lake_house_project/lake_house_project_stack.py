from aws_cdk import (
    Stack,
    RemovalPolicy,
    Tags,
    CfnOutput,
    aws_s3 as s3,
    aws_kms as kms,
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


