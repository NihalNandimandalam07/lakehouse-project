from typing import List

from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_glue as glue,
    aws_databrew as databrew,
)


class LakehouseStepFunctions:
    """Step Functions workflow running the lakehouse Glue and DataBrew jobs in order.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        landing_to_raw_job: glue.CfnJob,
        raw_to_transformed_job: glue.CfnJob,
        data_quality_jobs: List[databrew.CfnJob],
        customers_recipe_job: databrew.CfnJob,
        transformed_to_curated_job: glue.CfnJob,
        curated_to_redshift_job: glue.CfnJob,
    ) -> None:

        #Glue job tasks (RUN_JOB waits for each job to finish)
        landing_to_raw_task = tasks.GlueStartJobRun(
            scope,
            "LandingToRawTask",
            glue_job_name=landing_to_raw_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.landing_to_raw_result",
        )

        raw_to_transformed_task = tasks.GlueStartJobRun(
            scope,
            "RawToTransformedTask",
            glue_job_name=raw_to_transformed_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path=sfn.JsonPath.DISCARD,
        )

        transformed_to_curated_task = tasks.GlueStartJobRun(
            scope,
            "TransformedToCuratedTask",
            glue_job_name=transformed_to_curated_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path=sfn.JsonPath.DISCARD,
        )

        curated_to_redshift_task = tasks.GlueStartJobRun(
            scope,
            "CuratedToRedshiftTask",
            glue_job_name=curated_to_redshift_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path=sfn.JsonPath.DISCARD,
        )

        #retry transient Glue errors
        for task in [landing_to_raw_task, raw_to_transformed_task, transformed_to_curated_task, curated_to_redshift_task]:
            task.add_retry(
                errors=[
                    "Glue.ConcurrentRunsExceededException",
                    "Glue.OperationTimeoutException",
                ],
                interval=Duration.seconds(30),
                max_attempts=2,
                backoff_rate=2,
            )

        #DataBrew data quality checks on raw (report only, results in S3)
        data_quality = sfn.Parallel(
            scope,
            "RawDataQualityChecks",
            result_path=sfn.JsonPath.DISCARD,
        )
        for job in data_quality_jobs:
            data_quality.branch(
                tasks.GlueDataBrewStartJobRun(
                    scope,
                    f"DataQuality-{job.name}",
                    name=job.ref,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                )
            )

        #raw -> transformed: Glue (employees, COVID) and DataBrew (customers)
        transform = sfn.Parallel(
            scope,
            "RawToTransformed",
            result_path=sfn.JsonPath.DISCARD,
        )
        transform.branch(raw_to_transformed_task)
        transform.branch(
            tasks.GlueDataBrewStartJobRun(
                scope,
                "CustomersRecipeTask",
                name=customers_recipe_job.ref,
                integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            )
        )

        failure = sfn.Fail(
            scope,
            "LakehouseWorkflowFailed",
            cause="A Glue or DataBrew job failed",
        )

        success = sfn.Succeed(
            scope,
            "LakehouseWorkflowSucceeded",
        )

        for step in [landing_to_raw_task, data_quality, transform, transformed_to_curated_task, curated_to_redshift_task]:
            step.add_catch(failure, result_path="$.error")

        #landing -> raw -> data quality -> transformed -> curated -> Redshift
        definition = (
            landing_to_raw_task
            .next(data_quality)
            .next(transform)
            .next(transformed_to_curated_task)
            .next(curated_to_redshift_task)
            .next(success)
        )

        #State Machine
        self.state_machine = sfn.StateMachine(
            scope,
            "LakehouseETLStateMachine",
            state_machine_name="lakehouse-etl-workflow-nn",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(3),
        )

        #outputs
        CfnOutput(
            scope,
            "LakehouseStepFunctionsArn",
            value=self.state_machine.state_machine_arn
        )
