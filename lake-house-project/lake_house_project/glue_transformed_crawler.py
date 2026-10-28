import json

from aws_cdk import (
    Stack,
    CfnOutput,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_kms as kms,
    aws_lakeformation as lakeformation,
)


#table prefix -> transformed zone prefix
#one crawler per dataset so the prefix gives each table a clear name
#(mysql/employees and kafka/employees would otherwise both be "employees")
TRANSFORMED_DATASETS = {
    "mysql": "mysql/employees/",
    "kafka": "kafka/employees/",
    "flatfiles": "flatfiles/customers/",
    "dataexchange": "dataexchange/covid19_activity/",
}

#curated tables are named curated_<folder> (curated_employees, ...)
CURATED_DATASETS = {
    "employees": "employees/",
    "customers": "customers/",
    "covid": "covid/",
}


class LakehouseTransformedCrawler:
    """Glue database and crawlers cataloguing the transformed and curated zones.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        transformed_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        kms_key: kms.IKey,
        database_name: str = "lakehouse_db_nn",
    ) -> None:

        #Glue Database
        self.database = glue.CfnDatabase(
            scope,
            "LakehouseGlueDatabase",
            catalog_id=scope.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description="Lakehouse transformed and curated zone tables",
            ),
        )

        #Crawler IAM Role
        crawler_role = iam.Role(
            scope,
            "LakehouseTransformedCrawlerRole",
            role_name="lakehouse-transformed-crawler-role-nn",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        #objects are SSE-KMS encrypted, so reading needs decrypt on the key
        kms_key.grant_decrypt(crawler_role)
        transformed_bucket.grant_read(crawler_role)
        curated_bucket.grant_read(crawler_role)

        #Lake Formation controls the catalog in this account, so IAM alone isn't enough.
        #The deploy role created the database, which lets it grant on it; the crawler
        #gets full access to the tables it creates, so the database grant is all it needs.
        database_grant = lakeformation.CfnPrincipalPermissions(
            scope,
            "LakehouseCrawlerDatabasePermissions",
            principal=lakeformation.CfnPrincipalPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn,
            ),
            resource=lakeformation.CfnPrincipalPermissions.ResourceProperty(
                database=lakeformation.CfnPrincipalPermissions.DatabaseResourceProperty(
                    catalog_id=scope.account,
                    name=database_name,
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"],
            permissions_with_grant_option=[],
        )
        database_grant.node.add_dependency(self.database)

        self.crawlers = []

        #(zone, crawler key, table prefix, S3 path)
        targets = [
            ("transformed", key, f"{key}_", f"s3://{transformed_bucket.bucket_name}/{prefix}")
            for key, prefix in TRANSFORMED_DATASETS.items()
        ] + [
            ("curated", key, "curated_", f"s3://{curated_bucket.bucket_name}/{prefix}")
            for key, prefix in CURATED_DATASETS.items()
        ]

        for zone, key, table_prefix, path in targets:

            #Crawler
            crawler = glue.CfnCrawler(
                scope,
                f"Lakehouse{zone.title()}Crawler{key.title()}",
                name=f"lakehouse-{zone}-{key}-crawler-nn",
                role=crawler_role.role_arn,
                database_name=database_name,
                table_prefix=table_prefix,
                targets=glue.CfnCrawler.TargetsProperty(
                    s3_targets=[glue.CfnCrawler.S3TargetProperty(path=path)]
                ),
                schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                    update_behavior="UPDATE_IN_DATABASE",
                    delete_behavior="LOG",
                ),
                #jobs overwrite the data, so recrawl all of it
                recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                    recrawl_behavior="CRAWL_EVERYTHING"
                ),
                #partitions (COVID year/month) take the table's schema
                configuration=json.dumps({
                    "Version": 1.0,
                    "CrawlerOutput": {
                        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
                    },
                }),
            )

            crawler.node.add_dependency(database_grant)
            crawler.node.add_dependency(crawler_role)
            self.crawlers.append(crawler)

        #outputs
        CfnOutput(
            scope,
            "LakehouseGlueDatabaseName",
            value=database_name
        )
