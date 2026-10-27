from aws_cdk import (
    Stack,
    aws_datasync as datasync,
    aws_iam as iam,
    aws_s3 as s3,
    aws_kms as kms,
)


class LakehouseDataSync:
    """DataSync task copying flat files from the source bucket to the landing bucket.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        source_bucket: s3.IBucket,
        landing_bucket: s3.IBucket,
        kms_key: kms.IKey,
    ) -> None:

        #DataSync IAM Role
        self.datasync_role = iam.Role(
            scope,
            "LakehouseDataSyncRole",
            role_name="lakehouse-datasync-role-nn",
            assumed_by=iam.ServicePrincipal("datasync.amazonaws.com"),
        )

        source_bucket.grant_read(self.datasync_role)
        landing_bucket.grant_read_write(self.datasync_role)
        kms_key.grant_encrypt_decrypt(self.datasync_role)

        #Source Location
        self.source_location = datasync.CfnLocationS3(
            scope,
            "LakehouseDataSyncSource",
            s3_bucket_arn=source_bucket.bucket_arn,
            subdirectory="/flatfiles",
            s3_config=datasync.CfnLocationS3.S3ConfigProperty(
                bucket_access_role_arn=self.datasync_role.role_arn
            ),
        )

        #Destination Location
        self.destination_location = datasync.CfnLocationS3(
            scope,
            "LakehouseDataSyncDestination",
            s3_bucket_arn=landing_bucket.bucket_arn,
            subdirectory="/flatfiles",
            s3_config=datasync.CfnLocationS3.S3ConfigProperty(
                bucket_access_role_arn=self.datasync_role.role_arn
            ),
        )

        #DataSync Task
        self.datasync_task = datasync.CfnTask(
            scope,
            "LakehouseDataSyncTask",
            name="lakehouse-flatfiles-datasync-nn",
            source_location_arn=self.source_location.attr_location_arn,
            destination_location_arn=self.destination_location.attr_location_arn,
        )

        #role and its policy must exist before DataSync validates bucket access
        self.source_location.node.add_dependency(self.datasync_role)
        self.destination_location.node.add_dependency(self.datasync_role)
