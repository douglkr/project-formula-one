from dagster import Definitions

from dagster_project.resources import databricks_client_instance
from dagster_project.assets.airbyte.airbyte import airbyte_ingestion
from dagster_project.assets.databricks.databricks import databricks_workflow
from dagster_project.jobs import formula_one_pipeline_asset_job
from dagster_project.schedules import formula_one_pipeline_schedule


defs = Definitions(
    assets=[airbyte_ingestion, databricks_workflow],
    resources={"databricks": databricks_client_instance},
    jobs=[formula_one_pipeline_asset_job],
    schedules=[formula_one_pipeline_schedule],
)
