# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer - dim_race

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

gold_dim_race_df = spark.sql("SELECT * FROM ${table_path.silver_race}")

silver_circuit_df = spark.sql("SELECT * FROM ${table_path.silver_circuit}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Gold Layer

# COMMAND ----------

# merge fct table with dimension tables
gold_dim_race_df = (
    gold_dim_race_df
    .join(
        other=silver_circuit_df.drop("updated_at"), 
        on="circuit_id",
        how="inner"
    )
)

# rename fields and only expose fields that business users need
business_columns = [
    "race_key",
    "circuit_ref",
    "circuit_name",
    "circuit_location",
    "circuit_country",
    "circuit_lat",
    "circuit_lng",
    "circuit_alt",
    "name",
    "year",
    "race_date",  # column created below
    "datetime",
    "updated_at"
]

gold_dim_race_df = (
    gold_dim_race_df
    .withColumn("race_date", to_date(col("datetime")))
    .select(*business_columns)
    .withColumnsRenamed(
        {
            "name": "race_name",
            "year": "race_year",
            "datetime": "race_datetime"
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
        f"describe detail {spark.conf.get('table_path.gold_dim_race')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            gold_dim_race_df.alias("newData"),
            "oldData.race_id = newData.race_id") \
        .whenMatchedUpdate(set = { "race_id": col("newData.race_id") }) \
        .whenNotMatchedInsert(values = { "race_id": col("newData.race_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.gold_dim_race} (
            race_key BIGINT,
            circuit_ref STRING,
            circuit_name STRING,
            circuit_location STRING,
            circuit_country STRING,
            circuit_lat FLOAT,
            circuit_lng FLOAT, 
            circuit_alt INT,
            race_name STRING,
            race_year INT,
            race_date DATE,
            race_datetime TIMESTAMP,
            updated_at TIMESTAMP
        );
        """
    )

    (gold_dim_race_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(spark.conf.get("table_path.gold_dim_race"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---