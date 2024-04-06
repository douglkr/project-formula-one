from dagster_databricks import databricks_client


databricks_client_instance = databricks_client.configured(
    {
        "host": {"env": "DATABRICKS_HOST"},
        "token": {"env": "DATABRICKS_TOKEN"},
    }
)
