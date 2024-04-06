# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer - status

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
# MAGIC ### Load data from Bronze Layer

# COMMAND ----------

bronze_status_df = load_data("table_path.bronze_status", bronze_status_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Tests

# COMMAND ----------

ge_bronze_status_df = SparkDFDataset(bronze_status_df)

# not null quality check
pk_not_null_expectation = ge_bronze_status_df.expect_column_values_to_not_be_null(column="statusid")
if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Silver Layer

# COMMAND ----------

# keep only the latest record for each id
windowSpec = Window.partitionBy('statusid').orderBy(col("updated_at").desc())
silver_status_df = bronze_status_df.withColumn("_row_num", row_number().over(windowSpec))

silver_status_df = silver_status_df.filter(col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# rename columns
silver_status_df = (
    silver_status_df
    .withColumnsRenamed(
        {
            "statusid": "status_id",
            "status": "status_name"
        }
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data in Silver Layer

# COMMAND ----------

# Check if table exists
try:
    table_location = spark.sql(
        f"describe detail {spark.conf.get('table_path.silver_status')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            silver_status_df.alias("newData"),
            "oldData.status_id = newData.status_id") \
        .whenMatchedUpdate(set = { "status_id": col("newData.status_id") }) \
        .whenNotMatchedInsert(values = { "status_id": col("newData.status_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.silver_status} (
            status_key BIGINT GENERATED ALWAYS AS IDENTITY,
            status_id INT,
            status_name STRING,
            updated_at TIMESTAMP
        );
        """
    )

    (silver_status_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(spark.conf.get("table_path.silver_status"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---