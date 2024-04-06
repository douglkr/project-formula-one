from dagster import ScheduleDefinition
from orchestrator.jobs import formula_one_pipeline_asset_job

formula_one_pipeline_schedule = ScheduleDefinition(
    job=formula_one_pipeline_asset_job, cron_schedule="*/5 * * * *"
)
