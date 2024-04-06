# Databricks notebook source
# import libraries
from pyspark.sql.functions import (
    from_json, 
    col, 
    row_number, 
    concat, 
    lit, 
    to_timestamp, 
    to_date,
    when,
    md5
)
from pyspark.sql.window import Window
from great_expectations.dataset import SparkDFDataset
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def load_data(table_path, table_schema):
    return (
        spark.table(spark.conf.get(table_path))
        .select(
            from_json(col("_airbyte_data"), table_schema).alias("airbyte_data")
        )
        .select("airbyte_data.*")
    )