# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer - circuit

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

bronze_circuit_df = load_data("table_path.bronze_circuit", bronze_circuit_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Tests

# COMMAND ----------

ge_bronze_circuit_df = SparkDFDataset(bronze_circuit_df)

# not null quality check
pk_not_null_expectation = ge_bronze_circuit_df.expect_column_values_to_not_be_null(column="circuitid")
if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

# lat-long quality check
lat_expectation = ge_bronze_circuit_df.expect_column_value_lengths_to_be_between(
    column="lat", 
    min_value=-90, 
    max_value=90
)

if not lat_expectation["success"]: 
    raise Exception(lat_expectation)

lng_expectation = ge_bronze_circuit_df.expect_column_value_lengths_to_be_between(
    column="lng", 
    min_value=-180, 
    max_value=180
)

if not lng_expectation["success"]: 
    raise Exception(lng_expectation)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Silver Layer

# COMMAND ----------

# keep only the latest record for each id
windowSpec = Window.partitionBy('circuitid').orderBy(col("updated_at").desc())
silver_circuit_df = bronze_circuit_df.withColumn("_row_num", row_number().over(windowSpec))

silver_circuit_df = silver_circuit_df.filter(col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# clean alt column by replacing "\N" entries with null and changing it to integer
# rename columns
silver_circuit_df = (
    silver_circuit_df
    .replace(
        {"\\N": None}, subset=["alt"]
    )
    .withColumn(
        "alt", col("alt").cast("int")
    )
    .withColumnsRenamed(
        {
            "circuitid": "circuit_id", 
            "circuitref": "circuit_ref",
            "name": "circuit_name",
            "location": "circuit_location",
            "country": "circuit_country",
            "lat": "circuit_lat",
            "lng": "circuit_lng",
            "alt": "circuit_alt"
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
        f"describe detail {spark.conf.get('table_path.silver_circuit')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            silver_circuit_df.alias("newData"),
            "oldData.circuit_id = newData.circuit_id") \
        .whenMatchedUpdate(set = { "circuit_id": col("newData.circuit_id") }) \
        .whenNotMatchedInsert(values = { "circuit_id": col("newData.circuit_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.silver_circuit} (
            circuit_key BIGINT GENERATED ALWAYS AS IDENTITY,
            circuit_id INT,
            circuit_ref STRING,
            circuit_name STRING,
            circuit_location STRING,
            circuit_country STRING,
            circuit_lat FLOAT,
            circuit_lng FLOAT,
            circuit_alt INT, 
            url STRING,
            updated_at TIMESTAMP
        );
        """
    )

    (silver_circuit_df
        .sort(col("circuit_id").asc())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(spark.conf.get("table_path.silver_circuit"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---