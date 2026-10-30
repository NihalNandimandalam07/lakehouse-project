import hashlib
import json

from aws_cdk import (
    Stack,
    CfnOutput,
    aws_iam as iam,
    aws_s3 as s3,
    aws_kms as kms,
    aws_databrew as databrew,
    custom_resources as cr,
)


#each publish adds a version (1.0, 2.0, ...); after changing the recipe steps,
#bump this so the job runs the newly published version
CUSTOMERS_RECIPE_VERSION = "1.0"


#raw zone datasets checked by DataBrew data quality rules
RAW_DATASETS = {
    "mysql-employees": "mysql/employees/",
    "kafka-employees": "kafka/employees/",
    "customers": "flatfiles/customers/",
    "covid19-activity": "dataexchange/covid19_activity/",
}


def not_missing(column: str) -> databrew.CfnRuleset.RuleProperty:
    return databrew.CfnRuleset.RuleProperty(
        name=f"{column} is not missing",
        check_expression="AGG(MISSING_VALUES_PERCENTAGE) == :val1",
        substitution_map=[
            databrew.CfnRuleset.SubstitutionValueProperty(value_reference=":val1", value="0"),
        ],
        column_selectors=[databrew.CfnRuleset.ColumnSelectorProperty(name=column)],
    )


def no_duplicate_rows() -> databrew.CfnRuleset.RuleProperty:
    return databrew.CfnRuleset.RuleProperty(
        name="no duplicate rows",
        check_expression="AGG(DUPLICATE_ROWS_COUNT) == :val1",
        substitution_map=[
            databrew.CfnRuleset.SubstitutionValueProperty(value_reference=":val1", value="0"),
        ],
    )


def at_least(column: str, minimum: int) -> databrew.CfnRuleset.RuleProperty:
    #every row must pass
    return databrew.CfnRuleset.RuleProperty(
        name=f"{column} is at least {minimum}",
        check_expression=":col1 >= :val1",
        substitution_map=[
            databrew.CfnRuleset.SubstitutionValueProperty(value_reference=":col1", value=f"`{column}`"),
            databrew.CfnRuleset.SubstitutionValueProperty(value_reference=":val1", value=str(minimum)),
        ],
        threshold=databrew.CfnRuleset.ThresholdProperty(
            value=100,
            type="GREATER_THAN_OR_EQUAL",
            unit="PERCENTAGE",
        ),
    )


RULES = {
    "mysql-employees": [not_missing("employee_id"), no_duplicate_rows(), at_least("salary", 0)],
    "kafka-employees": [not_missing("employee_id"), no_duplicate_rows(), at_least("salary", 0)],
    "customers": [not_missing("customer_id"), not_missing("email"), no_duplicate_rows(), at_least("annual_income", 0)],
    "covid19-activity": [not_missing("report_date"), not_missing("country_short_name"), no_duplicate_rows(), at_least("people_positive_cases_count", 0)],
}


class LakehouseDataBrew:
    """DataBrew data quality checks on the raw zone and the customers recipe job.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        raw_bucket: s3.IBucket,
        transformed_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        kms_key: kms.IKey,
    ) -> None:

        #DataBrew IAM Role
        databrew_role = iam.Role(
            scope,
            "LakehouseDataBrewRole",
            role_name="lakehouse-databrew-role-nn",
            assumed_by=iam.ServicePrincipal("databrew.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueDataBrewServiceRole"
                )
            ],
        )

        #read raw, write transformed; data quality results go to the results bucket
        kms_key.grant_encrypt_decrypt(databrew_role)
        raw_bucket.grant_read(databrew_role)
        transformed_bucket.grant_read_write(databrew_role)
        results_bucket.grant_read_write(databrew_role)
        self.databrew_role = databrew_role

        self.profile_jobs = []
        datasets = {}

        for key, prefix in RAW_DATASETS.items():
            construct_suffix = key.title().replace("-", "")

            #Dataset
            dataset = databrew.CfnDataset(
                scope,
                f"LakehouseDataBrewDataset{construct_suffix}",
                name=f"lakehouse-raw-{key}-nn",
                format="PARQUET",
                input=databrew.CfnDataset.InputProperty(
                    s3_input_definition=databrew.CfnDataset.S3LocationProperty(
                        bucket=raw_bucket.bucket_name,
                        key=prefix,
                    )
                ),
            )
            datasets[key] = dataset

            #Data Quality Ruleset
            ruleset = databrew.CfnRuleset(
                scope,
                f"LakehouseDataBrewRuleset{construct_suffix}",
                name=f"lakehouse-raw-{key}-dq-nn",
                target_arn=scope.format_arn(
                    service="databrew",
                    resource="dataset",
                    resource_name=dataset.name,
                ),
                rules=RULES[key],
            )
            ruleset.node.add_dependency(dataset)

            #Profile Job with validation (report only, rule failures don't fail the job)
            profile_job = databrew.CfnJob(
                scope,
                f"LakehouseDataBrewProfile{construct_suffix}",
                name=f"lakehouse-raw-{key}-profile-nn",
                type="PROFILE",
                role_arn=databrew_role.role_arn,
                dataset_name=dataset.name,
                max_capacity=2,
                timeout=60,
                encryption_mode="SSE-KMS",
                encryption_key_arn=kms_key.key_arn,
                job_sample=databrew.CfnJob.JobSampleProperty(mode="FULL_DATASET"),
                output_location=databrew.CfnJob.OutputLocationProperty(
                    bucket=results_bucket.bucket_name,
                    key=f"databrew/data-quality/{key}/",
                ),
                validation_configurations=[
                    databrew.CfnJob.ValidationConfigurationProperty(
                        ruleset_arn=scope.format_arn(
                            service="databrew",
                            resource="ruleset",
                            resource_name=ruleset.name,
                        ),
                        validation_mode="CHECK_ALL",
                    )
                ],
            )
            profile_job.node.add_dependency(ruleset)
            profile_job.node.add_dependency(databrew_role)
            self.profile_jobs.append(profile_job)

        #Customers Recipe (raw -> transformed)
        recipe_steps = [
            ("LOWER_CASE", {"sourceColumn": "email"}),
            ("LOWER_CASE", {"sourceColumn": "customer_status"}),
            ("FILL_WITH_CUSTOM", {"sourceColumn": "customer_status", "columnDataType": "string", "value": "unknown"}),
            ("CAPITAL_CASE", {"sourceColumn": "first_name"}),
            ("CAPITAL_CASE", {"sourceColumn": "last_name"}),
            ("UPPER_CASE", {"sourceColumn": "address_state"}),
            ("NOW", {"targetColumn": "transformed_timestamp"}),
        ]

        recipe = databrew.CfnRecipe(
            scope,
            "LakehouseCustomersRecipe",
            name="lakehouse-customers-recipe-nn",
            description="Standardize raw customers for the transformed zone",
            steps=[self._step(operation, **parameters) for operation, parameters in recipe_steps],
        )

        #CloudFormation only creates the working version; jobs need a published one
        recipe_arn = scope.format_arn(service="databrew", resource="recipe", resource_name=recipe.name)
        steps_hash = hashlib.sha256(json.dumps(recipe_steps).encode()).hexdigest()[:12]
        published_versions = [f"{major}.0" for major in range(1, int(float(CUSTOMERS_RECIPE_VERSION)) + 1)]

        publish_call = cr.AwsSdkCall(
            service="DataBrew",
            action="publishRecipe",
            #the hash makes CloudFormation republish whenever the steps change
            parameters={"Name": recipe.name, "Description": f"steps {steps_hash}"},
            physical_resource_id=cr.PhysicalResourceId.of(f"{recipe.name}-{steps_hash}"),
        )

        publish_recipe = cr.AwsCustomResource(
            scope,
            "LakehouseCustomersRecipePublish",
            on_create=publish_call,
            on_update=publish_call,
            #published versions block deleting the recipe, so remove them first
            on_delete=cr.AwsSdkCall(
                service="DataBrew",
                action="batchDeleteRecipeVersion",
                parameters={"Name": recipe.name, "RecipeVersions": published_versions},
                ignore_error_codes_matching="ResourceNotFoundException",
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(resources=[recipe_arn]),
            install_latest_aws_sdk=False,
        )
        publish_recipe.node.add_dependency(recipe)

        #Customers Recipe Job
        self.customers_recipe_job = databrew.CfnJob(
            scope,
            "LakehouseCustomersRecipeJob",
            name="lakehouse-customers-raw-to-transformed-nn",
            type="RECIPE",
            role_arn=databrew_role.role_arn,
            dataset_name=datasets["customers"].name,
            max_capacity=2,
            timeout=60,
            encryption_mode="SSE-KMS",
            encryption_key_arn=kms_key.key_arn,
            recipe=databrew.CfnJob.RecipeProperty(name=recipe.name, version=CUSTOMERS_RECIPE_VERSION),
            outputs=[
                databrew.CfnJob.OutputProperty(
                    location=databrew.CfnJob.S3LocationProperty(
                        bucket=transformed_bucket.bucket_name,
                        key="flatfiles/customers/",
                    ),
                    format="PARQUET",
                    overwrite=True,
                )
            ],
        )
        self.customers_recipe_job.node.add_dependency(publish_recipe)
        self.customers_recipe_job.node.add_dependency(datasets["customers"])
        self.customers_recipe_job.node.add_dependency(databrew_role)

        #outputs
        CfnOutput(
            scope,
            "LakehouseCustomersRecipeJobName",
            value=self.customers_recipe_job.ref
        )

    @staticmethod
    def _step(operation: str, **parameters: str) -> databrew.CfnRecipe.RecipeStepProperty:
        return databrew.CfnRecipe.RecipeStepProperty(
            action=databrew.CfnRecipe.ActionProperty(
                operation=operation,
                parameters=parameters,
            )
        )
