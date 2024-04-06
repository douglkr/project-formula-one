from dagster import Definitions

from analytics.resources import databricks_client_instance
from analytics.assets.airbyte.airbyte import airbyte_ingestion
from analytics.assets.databricks.databricks import databricks_workflow
from analytics.jobs import formula_one_pipeline_asset_job
from analytics.schedules import formula_one_pipeline_schedule


defs = Definitions(
    assets=[airbyte_ingestion, databricks_workflow],
    resources={"databricks": databricks_client_instance},
    jobs=[formula_one_pipeline_asset_job],
    schedules=[formula_one_pipeline_schedule],
)
