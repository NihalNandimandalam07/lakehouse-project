from aws_cdk import Stack
from constructs import Construct

from lake_house_project.storage import LakehouseStorage
from lake_house_project.networking import LakehouseNetworking
from lake_house_project.security_groups import LakehouseSecurityGroups
from lake_house_project.rds import LakehouseRDS
from lake_house_project.ec2_rds import LakehouseEC2
from lake_house_project.glue_ingestion import LakehouseGlue


class LakeHouseProjectStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.storage = LakehouseStorage(self)

        self.networking = LakehouseNetworking(self)

        self.security_groups = LakehouseSecurityGroups(self, vpc=self.networking.vpc)

        self.rds = LakehouseRDS(
            self,
            vpc=self.networking.vpc,
            rds_sg=self.security_groups.rds_sg,
            kms_key=self.storage.kms_key,
        )

        self.ec2 = LakehouseEC2(
            self,
            vpc=self.networking.vpc,
            rds_sg=self.security_groups.rds_sg,
        )

        self.glue = LakehouseGlue(
            self,
            vpc=self.networking.vpc,
            glue_sg=self.security_groups.glue_sg,
            rds_sg=self.security_groups.rds_sg,
            landing_bucket=self.storage.landing_bucket,
            script_bucket=self.storage.glue_assets_bucket,
            db_endpoint=self.rds.mysql_db.db_instance_endpoint_address,
            secret_arn=self.rds.mysql_db.secret.secret_arn,
            kms_key=self.storage.kms_key,
        )
