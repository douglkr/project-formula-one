# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer - dim_constructor

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

gold_dim_constructor_df = spark.sql("SELECT * FROM ${table_path.silver_constructor}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Gold Layer

# COMMAND ----------

# rename fields and only expose fields that business users need
business_columns = [
    "constructor_key",
    "constructor_ref",
    "name",
    "nationality",
    "updated_at"
]

gold_dim_constructor_df = (
    gold_dim_constructor_df
    .select(*business_columns)
    .withColumnsRenamed(
        {
            "name": "constructor_name",
            "nationality": "constructor_nationality"
        }
    )
)

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
        f"describe detail {spark.conf.get('table_path.gold_dim_constructor')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            gold_dim_constructor_df.alias("newData"),
            "oldData.constructor_id = newData.constructor_id") \
        .whenMatchedUpdate(set = { "constructor_id": col("newData.constructor_id") }) \
        .whenNotMatchedInsert(values = { "constructor_id": col("newData.constructor_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.gold_dim_constructor} (
            constructor_key BIGINT,
            constructor_ref STRING,
            constructor_name STRING,
            constructor_nationality STRING, 
            updated_at TIMESTAMP
        );
        """
    )

    (gold_dim_constructor_df
        .sort(col("constructor_id").asc())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(spark.conf.get("table_path.gold_dim_constructor"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---