from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_glue as glue,
)


class LakehouseStepFunctions:
    """Step Functions workflow running the lakehouse Glue jobs in order.

    Resources are created directly on the stack (not a nested Construct)
    so their CloudFormation logical IDs stay unchanged.
    """

    def __init__(
        self,
        scope: Stack,
        landing_to_raw_job: glue.CfnJob,
        raw_to_transformed_job: glue.CfnJob,
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
            result_path="$.raw_to_transformed_result",
        )

        failure = sfn.Fail(
            scope,
            "LakehouseWorkflowFailed",
            cause="A Glue ETL job failed",
        )

        success = sfn.Succeed(
            scope,
            "LakehouseWorkflowSucceeded",
        )

        #retry transient Glue errors, otherwise stop the workflow
        for task in [landing_to_raw_task, raw_to_transformed_task]:
            task.add_retry(
                errors=[
                    "Glue.ConcurrentRunsExceededException",
                    "Glue.OperationTimeoutException",
                ],
                interval=Duration.seconds(30),
                max_attempts=2,
                backoff_rate=2,
            )
            task.add_catch(failure, result_path="$.error")

        #landing -> raw -> transformed
        definition = (
            landing_to_raw_task
            .next(raw_to_transformed_task)
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
