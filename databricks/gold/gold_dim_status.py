# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer - dim_status

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

gold_dim_status_df = spark.sql("SELECT * FROM ${table_path.silver_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Gold Layer

# COMMAND ----------

# only expose fields that business users need
business_columns = [
    "status_key",
    "status_name",
    "updated_at"
]

gold_dim_status_df = gold_dim_status_df.select(*business_columns)

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
        f"describe detail {spark.conf.get('table_path.gold_dim_status')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            gold_dim_status_df.alias("newData"),
            "oldData.status_id = newData.status_id") \
        .whenMatchedUpdate(set = { "status_id": col("newData.status_id") }) \
        .whenNotMatchedInsert(values = { "status_id": col("newData.status_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.gold_dim_status} (
            status_key BIGINT,
            status_name STRING,
            updated_at TIMESTAMP
        );
        """
    )

    (gold_dim_status_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(spark.conf.get("table_path.gold_dim_status"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---