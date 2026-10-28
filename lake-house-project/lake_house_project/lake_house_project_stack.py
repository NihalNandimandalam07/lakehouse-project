from aws_cdk import Stack
from constructs import Construct

from lake_house_project.storage import LakehouseStorage
from lake_house_project.networking import LakehouseNetworking
from lake_house_project.security_groups import LakehouseSecurityGroups
from lake_house_project.rds import LakehouseRDS
from lake_house_project.ec2_rds import LakehouseEC2
from lake_house_project.glue_ingestion import LakehouseGlue
from lake_house_project.networking_streaming import LakehouseStreamingNetworking
from lake_house_project.glue_streaming import GlueStreaming
from lake_house_project.datasync import LakehouseDataSync
from lake_house_project.glue_landing_to_raw import LakehouseLandingToRaw
from lake_house_project.glue_raw_to_transformed import LakehouseRawToTransformed
from lake_house_project.databrew import LakehouseDataBrew
from lake_house_project.glue_transformed_to_curated import LakehouseTransformedToCurated
from lake_house_project.glue_transformed_crawler import LakehouseTransformedCrawler
from lake_house_project.step_functions import LakehouseStepFunctions


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

        #Kafka cluster lives in a separate terraform project (IAC-apache-kafka-cluster)
        self.streaming_network = LakehouseStreamingNetworking(
            self,
            "LakehouseStreamingNetwork",
            kafka_vpc_id="vpc-08641291830df4c09",
            kafka_vpc_cidr="10.0.0.0/16",
            kafka_route_table_id="rtb-08be945f33e9b652f",
            kafka_sg_id="sg-0cd48bbac01dec368",
        )

        #broker IPs are fixed in the Kafka terraform (cidrhost(subnet, 10))
        self.glue_streaming = GlueStreaming(
            self,
            "GlueKafkaStreaming",
            streaming_subnet=self.streaming_network.vpc.isolated_subnets[0],
            streaming_security_group=self.streaming_network.glue_sg,
            landing_bucket=self.storage.landing_bucket,
            script_bucket=self.storage.glue_assets_bucket,
            kms_key=self.storage.kms_key,
            script_deployment=self.glue.deployment,
            kafka_secret_arn="arn:aws:secretsmanager:us-east-2:909698979753:secret:kafka_credentials_testing-XVmehU",
            kafka_bootstrap_servers="10.0.0.10:9092,10.0.1.10:9092,10.0.2.10:9092",
            kafka_topic="lakehouse-topic",
        )

        self.datasync = LakehouseDataSync(
            self,
            source_bucket=self.storage.source_bucket,
            landing_bucket=self.storage.landing_bucket,
            kms_key=self.storage.kms_key,
        )

        self.landing_to_raw = LakehouseLandingToRaw(
            self,
            landing_bucket=self.storage.landing_bucket,
            raw_bucket=self.storage.raw_bucket,
            script_bucket=self.storage.glue_assets_bucket,
            kms_key=self.storage.kms_key,
            script_deployment=self.glue.deployment,
        )

        self.raw_to_transformed = LakehouseRawToTransformed(
            self,
            raw_bucket=self.storage.raw_bucket,
            transformed_bucket=self.storage.transformed_bucket,
            script_bucket=self.storage.glue_assets_bucket,
            kms_key=self.storage.kms_key,
            script_deployment=self.glue.deployment,
        )

        self.databrew = LakehouseDataBrew(
            self,
            raw_bucket=self.storage.raw_bucket,
            transformed_bucket=self.storage.transformed_bucket,
            results_bucket=self.storage.glue_assets_bucket,
            kms_key=self.storage.kms_key,
        )

        self.transformed_to_curated = LakehouseTransformedToCurated(
            self,
            transformed_bucket=self.storage.transformed_bucket,
            curated_bucket=self.storage.curated_bucket,
            script_bucket=self.storage.glue_assets_bucket,
            kms_key=self.storage.kms_key,
            script_deployment=self.glue.deployment,
        )

        self.transformed_crawler = LakehouseTransformedCrawler(
            self,
            transformed_bucket=self.storage.transformed_bucket,
            curated_bucket=self.storage.curated_bucket,
            kms_key=self.storage.kms_key,
        )

        self.step_functions = LakehouseStepFunctions(
            self,
            landing_to_raw_job=self.landing_to_raw.job,
            raw_to_transformed_job=self.raw_to_transformed.job,
            data_quality_jobs=self.databrew.profile_jobs,
            customers_recipe_job=self.databrew.customers_recipe_job,
            transformed_to_curated_job=self.transformed_to_curated.job,
        )
