# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer - dim_driver

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

gold_dim_driver_df = spark.sql("SELECT * FROM ${table_path.silver_driver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Gold Layer

# COMMAND ----------

# create full_name field, rename fields, and only expose fields that business users need
business_columns = [
    "driver_key",
    "driver_ref",
    "number",
    "code",
    "forename",
    "surname",
    "driver_fullname",  # column created below
    "date_of_birth",
    "nationality",
    "updated_at"
]

gold_dim_driver_df = (
    gold_dim_driver_df
    .withColumn("driver_fullname", concat(col("forename"), lit(" "), col("surname")))
    .select(*business_columns)
    .withColumnsRenamed(
        {
            "number": "driver_number",
            "code": "driver_code",
            "forename": "driver_forename",
            "surname": "driver_surname",
            "date_of_birth": "driver_date_of_birth",
            "nationality": "driver_nationality"
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
        f"describe detail {spark.conf.get('table_path.gold_dim_driver')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            gold_dim_driver_df.alias("newData"),
            "oldData.driver_id = newData.driver_id") \
        .whenMatchedUpdate(set = { "driver_id": col("newData.driver_id") }) \
        .whenNotMatchedInsert(values = { "driver_id": col("newData.driver_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.gold_dim_driver} (
            driver_key BIGINT,
            driver_ref STRING,
            driver_number INT,
            driver_code STRING,
            driver_forename STRING,
            driver_surname STRING,
            driver_fullname STRING,
            driver_date_of_birth DATE,
            driver_nationality STRING, 
            updated_at TIMESTAMP
        );
        """
    )

    (gold_dim_driver_df
        .sort(col("driver_id").asc())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(spark.conf.get("table_path.gold_dim_driver"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---