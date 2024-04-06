# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./schemas

# COMMAND ----------

# bronze layer paths
spark.conf.set("table_path.bronze_circuit", "{your_database_path}._airbyte_raw_bronze_circuit")
spark.conf.set("table_path.bronze_driver", "{your_database_path}._airbyte_raw_bronze_driver")
spark.conf.set("table_path.bronze_constructor", "{your_database_path}._airbyte_raw_bronze_constructor")
spark.conf.set("table_path.bronze_race", "{your_database_path}._airbyte_raw_bronze_race")
spark.conf.set("table_path.bronze_race_result", "{your_database_path}._airbyte_raw_bronze_race_result")
spark.conf.set("table_path.bronze_sprint_result", "{your_database_path}._airbyte_raw_bronze_sprint_result")
spark.conf.set("table_path.bronze_status", "{your_database_path}._airbyte_raw_bronze_status")

# COMMAND ----------

# silver layer paths
spark.conf.set("table_path.silver_circuit", "{your_database_path}.silver_circuit")
spark.conf.set("table_path.silver_driver", "{your_database_path}.silver_driver")
spark.conf.set("table_path.silver_constructor", "{your_database_path}.silver_constructor")
spark.conf.set("table_path.silver_race", "{your_database_path}.silver_race")
spark.conf.set("table_path.silver_race_result", "{your_database_path}.silver_race_result")
spark.conf.set("table_path.silver_sprint_result", "{your_database_path}.silver_sprint_result")
spark.conf.set("table_path.silver_status", "{your_database_path}.silver_status")

# COMMAND ----------

# gold layer paths
spark.conf.set("table_path.gold_dim_circuit", "{your_database_path}.gold_dim_circuit")
spark.conf.set("table_path.gold_dim_driver", "{your_database_path}.gold_dim_driver")
spark.conf.set("table_path.gold_dim_constructor", "{your_database_path}.gold_dim_constructor")
spark.conf.set("table_path.gold_dim_race", "{your_database_path}.gold_dim_race")
spark.conf.set("table_path.gold_fct_race_result", "{your_database_path}.gold_fct_race_result")
spark.conf.set("table_path.gold_fct_sprint_result", "{your_database_path}.gold_fct_sprint_result")
spark.conf.set("table_path.gold_dim_status", "{your_database_path}.gold_dim_status")
spark.conf.set("table_path.gold_report_result", "{your_database_path}.gold_report_result")