from dagster import Definitions

from orchestrator.resources import databricks_client_instance
from orchestrator.assets.airbyte.airbyte import airbyte_ingestion
from orchestrator.assets.databricks.databricks import databricks_workflow
from orchestrator.jobs import formula_one_pipeline_asset_job
from orchestrator.schedules import formula_one_pipeline_schedule


defs = Definitions(
    assets=[airbyte_ingestion, databricks_workflow],
    resources={"databricks": databricks_client_instance},
    jobs=[formula_one_pipeline_asset_job],
    schedules=[formula_one_pipeline_schedule],
)
