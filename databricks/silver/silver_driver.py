# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer - driver

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

bronze_driver_df = load_data("table_path.bronze_driver", bronze_driver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Tests

# COMMAND ----------

ge_bronze_driver_df = SparkDFDataset(bronze_driver_df)

# not null quality check
pk_not_null_expectation = ge_bronze_driver_df.expect_column_values_to_not_be_null(column="driverid")
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
windowSpec = Window.partitionBy('driverid').orderBy(col("updated_at").desc())
silver_driver_df = bronze_driver_df.withColumn("_row_num", row_number().over(windowSpec))

silver_driver_df = silver_driver_df.filter(col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# clean number and code columns by replacing "\N" entries with null and changing number col to integer
# rename columns
silver_driver_df = (
    silver_driver_df
    .replace(
        {"\\N": None}, subset=["number", "code"]
    )
    .withColumn(
        "number", col("number").cast("int")
    )
    .withColumnsRenamed({"driverid": "driver_id", "driverref": "driver_ref", "dob": "date_of_birth"})
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
        f"describe detail {spark.conf.get('table_path.silver_driver')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            silver_driver_df.alias("newData"),
            "oldData.driver_id = newData.driver_id") \
        .whenMatchedUpdate(set = { "driver_id": col("newData.driver_id") }) \
        .whenNotMatchedInsert(values = { "driver_id": col("newData.driver_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.silver_driver} (
            driver_key BIGINT GENERATED ALWAYS AS IDENTITY,
            driver_id INT,
            driver_ref STRING,
            number INT,
            code STRING,
            forename STRING,
            surname STRING,
            date_of_birth DATE,
            nationality STRING, 
            url STRING,
            updated_at TIMESTAMP
        );
        """
    )

    (silver_driver_df
        .sort(col("driver_id").asc())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(spark.conf.get("table_path.silver_driver"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---