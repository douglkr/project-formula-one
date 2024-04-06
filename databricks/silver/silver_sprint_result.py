# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer - sprint result

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

bronze_sprint_result_df = load_data("table_path.bronze_sprint_result", bronze_sprint_result_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Tests

# COMMAND ----------

ge_bronze_sprint_result_df = SparkDFDataset(bronze_sprint_result_df)

# not null quality check
pk_not_null_expectation = ge_bronze_sprint_result_df.expect_column_values_to_not_be_null(column="sprintresultid")
if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

fk_race_not_null_expectation = ge_bronze_sprint_result_df.expect_column_values_to_not_be_null(column="raceid")
if not fk_race_not_null_expectation["success"]: 
    raise Exception(fk_race_not_null_expectation)

fk_driver_not_null_expectation = ge_bronze_sprint_result_df.expect_column_values_to_not_be_null(column="driverid")
if not fk_driver_not_null_expectation["success"]: 
    raise Exception(fk_driver_not_null_expectation)

fk_constructor_not_null_expectation = ge_bronze_sprint_result_df.expect_column_values_to_not_be_null(column="constructorid")
if not fk_constructor_not_null_expectation["success"]: 
    raise Exception(fk_constructor_not_null_expectation)

# check if statusid is in expected set
bronze_status = load_data("table_path.bronze_status", bronze_status_schema)
statusid_set = [row.statusid for row in bronze_status.select(col("statusid")).distinct().collect()]
statusid_set.sort()
contain_set_expectation = ge_bronze_sprint_result_df.expect_column_distinct_values_to_be_in_set(
    column="statusid",
    value_set=statusid_set
)
if not contain_set_expectation["success"]: 
    raise Exception(contain_set_expectation)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare data for Silver Layer

# COMMAND ----------

# keep only the latest record for each id
windowSpec = Window.partitionBy('sprintresultid').orderBy(col("updated_at").desc())
silver_sprint_result_df = bronze_sprint_result_df.withColumn("_row_num", row_number().over(windowSpec))

silver_sprint_result_df = silver_sprint_result_df.filter(col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# clean position, and several time columns by replacing "\N" entries with null
# change type of specific columns
# rename columns
silver_sprint_result_df = (
    silver_sprint_result_df
    .replace(
        {"\\N": None}, subset=[
            "position", "time", "milliseconds", "fastestlap", "fastestlaptime"
        ]
    )
    .withColumns(
        {
            "number": col("number").cast("int"),
            "position": col("position").cast("int"),
            "milliseconds": col("milliseconds").cast("long"),
            "fastestlap": col("fastestlap").cast("int")
        }
    )
    .withColumnsRenamed(
        {
            "sprintresultid": "sprint_result_id",
            "raceid": "race_id",
            "driverid": "driver_id",
            "constructorid": "constructor_id",
            "positiontext": "position_text",
            "positionorder": "position_order",
            "fastestlap": "fastest_lap",
            "fastestlaptime": "fastest_lap_time",
            "statusid": "status_id",
            "number": "car_number"
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
        f"describe detail {spark.conf.get('table_path.silver_sprint_result')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            silver_sprint_result_df.alias("newData"),
            "oldData.sprint_result_id = newData.sprint_result_id") \
        .whenMatchedUpdate(set = { "sprint_result_id": col("newData.sprint_result_id") }) \
        .whenNotMatchedInsert(values = { "sprint_result_id": col("newData.sprint_result_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.silver_sprint_result} (
            sprint_result_key BIGINT GENERATED ALWAYS AS IDENTITY,
            sprint_result_id INT,
            race_id INT,
            driver_id INT,
            constructor_id INT,
            car_number INT,
            grid INT,
            position INT,
            position_text STRING,
            position_order INT,
            points FLOAT,
            laps INTEGER,
            time STRING,
            milliseconds LONG,
            fastest_lap INT,
            fastest_lap_time STRING,
            status_id INT,
            updated_at TIMESTAMP
        );
        """
    )

    (silver_sprint_result_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(spark.conf.get("table_path.silver_sprint_result"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---