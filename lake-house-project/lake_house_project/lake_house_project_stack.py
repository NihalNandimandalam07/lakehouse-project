from aws_cdk import Stack
from constructs import Construct

from lake_house_project.storage import LakehouseStorage
from lake_house_project.networking import LakehouseNetworking
from lake_house_project.security_groups import LakehouseSecurityGroups
from lake_house_project.rds import LakehouseRDS


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
