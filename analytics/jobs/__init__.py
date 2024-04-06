from dagster import define_asset_job


formula_one_pipeline_asset_job = define_asset_job(
    name="pipeline",
    selection="*databricks_workflow*",
)
