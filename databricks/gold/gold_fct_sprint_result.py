# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer - fct_sprint_result

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup notebook

# COMMAND ----------

# MAGIC %run ../common/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data from Silver Layer

# COMMAND ----------

gold_fct_sprint_result_df = spark.sql("SELECT * FROM ${table_path.silver_sprint_result}")

silver_race_df = spark.sql("SELECT * FROM ${table_path.silver_race}")
silver_driver_df = spark.sql("SELECT * FROM ${table_path.silver_driver}")
silver_constructor_df = spark.sql("SELECT * FROM ${table_path.silver_constructor}")
silver_status_df = spark.sql("SELECT * FROM ${table_path.silver_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Gold Layer

# COMMAND ----------

# merge fct table with dimension tables
gold_fct_sprint_result_df = (
    gold_fct_sprint_result_df
    .join(
        other=silver_race_df.drop("updated_at"), 
        on="race_id",
        how="inner"
    )
    .join(
        other=silver_driver_df.drop("updated_at"),
        on="driver_id",
        how="inner"
    )
    .join(
        other=silver_constructor_df.drop("updated_at"),
        on="constructor_id",
        how="inner"
    )
    .join(
        other=silver_status_df.drop("updated_at"),
        on="status_id",
        how="inner"
    )
)

# only expose fields that business users need
business_columns = [
    "sprint_result_key",
    "sprint_result_id",
    "race_key",
    "driver_key",
    "constructor_key",
    "position_order",
    "points",
    "laps",
    "milliseconds",
    "status_key",
    "updated_at"
]

gold_fct_sprint_result_df = gold_fct_sprint_result_df.select(*business_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data in Gold Layer

# COMMAND ----------

# Check if table exists
try:
    table_location = spark.sql(
        f"describe detail {spark.conf.get('table_path.gold_fct_sprint_result')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            gold_fct_sprint_result_df.alias("newData"),
            "oldData.sprint_result_id = newData.sprint_result_id") \
        .whenMatchedUpdate(set = { "sprint_result_id": col("newData.sprint_result_id") }) \
        .whenNotMatchedInsert(values = { "sprint_result_id": col("newData.sprint_result_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.gold_fct_sprint_result} (
            sprint_result_key BIGINT,
            sprint_result_id INT,
            race_key BIGINT,
            driver_key BIGINT,
            constructor_key BIGINT,
            position_order INT,
            points FLOAT,
            laps INTEGER,
            milliseconds LONG,
            status_key BIGINT,
            updated_at TIMESTAMP
        );
        """
    )

    (gold_fct_sprint_result_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(spark.conf.get("table_path.gold_fct_sprint_result"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---