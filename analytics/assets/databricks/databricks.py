from databricks_cli.sdk import JobsService
from dagster import (
    AssetExecutionContext,
    Config, 
    EnvVar,
    SourceAsset,
    AssetKey,
    asset,
    FreshnessPolicy,
    AutoMaterializePolicy
)
import time


class DatabricksJobConfig(Config):
    job_id: int = EnvVar("DATABRICKS_JOB_ID")
    polling_seconds: int = 10


airbyte_ingestion = SourceAsset(key=AssetKey("airbyte_ingestion"))

@asset(
        required_resource_keys={"databricks"}, 
        deps=[airbyte_ingestion], 
        freshness_policy=FreshnessPolicy(
            maximum_lag_minutes=10,
            cron_schedule="* * * * *"
        ),
        auto_materialize_policy=AutoMaterializePolicy.eager(),
        group_name="databricks"
)
def databricks_workflow(context: AssetExecutionContext, config: DatabricksJobConfig) -> None:
    """Triggers a Databricks job

    Args:
        config: DatabricksJobConfig
    """
    databricks_api_client = context.resources.databricks.api_client
    jobs_service = JobsService(databricks_api_client)

    context.log.info(
        f"Triggering Job {config.job_id}."
    )
    job_response = jobs_service.run_now(job_id=config.job_id)
    run_id = job_response.get("run_id")
    context.log.info(
        f"Job trigger resulted in Run {run_id}."
    )

    job_status = jobs_service.get_run(run_id=run_id).get("state").get("life_cycle_state")
    while (
        job_status == "RUNNING"
    ):
        context.log.info(
            f"Run {run_id} is running. Polling job status again in {config.polling_seconds} seconds."
        )
        time.sleep(config.polling_seconds)

        # update job status
        job_status = jobs_service.get_run(run_id=run_id).get("state").get("life_cycle_state")

    # check job result state
    job_state = jobs_service.get_run(run_id=run_id).get("state")
    if job_state.get("result_state") == "SUCCESS":
        context.log.info(f"Job {config.job_id} and run {run_id} has ran successfully.")
    else:
        raise Exception(
            f"Job {config.job_id} has failed. Job status: {job_state.get('result_state')}. Error message: {job_state.get('state_message')}."
        )
