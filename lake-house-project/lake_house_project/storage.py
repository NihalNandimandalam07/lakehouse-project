from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    Duration,
    aws_s3 as s3,
    aws_kms as kms,
)


class LakehouseStorage:
    """KMS key and S3 buckets for the lakehouse.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(self, scope: Stack) -> None:

        #KMS Key
        self.kms_key = kms.Key(
            scope,
            "LakehouseKMSKey",
            alias = "alias/lakehouse-kms-nn",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN
        )

        self.source_bucket = self._create_bucket(scope, "SourceBucket", "lakehouse-source-nn")
        self.landing_bucket = self._create_bucket(scope, "LandingBucket", "lakehouse-landing-nn")
        self.raw_bucket = self._create_bucket(scope, "RawBucket", "lakehouse-raw-nn")
        self.transformed_bucket = self._create_bucket(scope, "TransformedBucket", "lakehouse-transformed-nn")
        self.curated_bucket = self._create_bucket(scope, "CuratedBucket", "lakehouse-curated-nn")
        self.glue_assets_bucket = self._create_bucket(scope, "GlueAssetsBucket", "lakehouse-glue-assets-nn")
        self.athena_results_bucket = self._create_bucket(scope, "AthenaResultsBucket", "lakehouse-athena-results-nn")

        #query results are only needed for a while
        self.athena_results_bucket.add_lifecycle_rule(
            expiration=Duration.days(30),
            noncurrent_version_expiration=Duration.days(1),
        )

        #outputs
        buckets = {
            "SourceBucketName":self.source_bucket,
            "LandingBucketName":self.landing_bucket,
            "RawBucketName":self.raw_bucket,
            "TransformedBucketName":self.transformed_bucket,
            "CuratedBucketName":self.curated_bucket,
            "GlueAssetsBucketName":self.glue_assets_bucket,
            "AthenaResultsBucketName":self.athena_results_bucket
        }

        for output_name, bucket in buckets.items():
            CfnOutput(
                scope,
                output_name,
                value=bucket.bucket_name
            )

        CfnOutput(
            scope,
            "LakehouseKMSKeyOutput",
            value=self.kms_key.key_arn
        )

    def _create_bucket(self, scope: Stack, construct_id: str, bucket_name: str) -> s3.Bucket:
        return s3.Bucket(
            scope,
            construct_id,
            bucket_name=bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            removal_policy=RemovalPolicy.RETAIN
        )
