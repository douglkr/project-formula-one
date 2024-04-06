# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer - constructor

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

bronze_constructor_df = load_data("table_path.bronze_constructor", bronze_constructor_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Tests

# COMMAND ----------

ge_bronze_constructor_df = SparkDFDataset(bronze_constructor_df)

# not null quality check
pk_not_null_expectation = ge_bronze_constructor_df.expect_column_values_to_not_be_null(column="constructorid")
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
windowSpec = Window.partitionBy('constructorid').orderBy(col("updated_at").desc())
silver_constructor_df = bronze_constructor_df.withColumn("_row_num", row_number().over(windowSpec))

silver_constructor_df = silver_constructor_df.filter(col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# rename columns
silver_constructor_df = (
    silver_constructor_df
    .withColumnsRenamed({"constructorid": "constructor_id", "constructorref": "constructor_ref"})
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
        f"describe detail {spark.conf.get('table_path.silver_constructor')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            silver_constructor_df.alias("newData"),
            "oldData.constructor_id = newData.constructor_id") \
        .whenMatchedUpdate(set = { "constructor_id": col("newData.constructor_id") }) \
        .whenNotMatchedInsert(values = { "constructor_id": col("newData.constructor_id") }) \
        .execute()
except AnalysisException:
    print("Table does not exist")
    # table does not exist - create it for the 1st time
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.silver_constructor} (
            constructor_key BIGINT GENERATED ALWAYS AS IDENTITY,
            constructor_id INT,
            constructor_ref STRING,
            name STRING,
            nationality STRING, 
            url STRING,
            updated_at TIMESTAMP
        );
        """
    )

    (silver_constructor_df
        .sort(col("constructor_id").asc())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(spark.conf.get("table_path.silver_constructor"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---