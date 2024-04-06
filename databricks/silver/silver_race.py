# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer - race

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

bronze_race_df = load_data("table_path.bronze_race", bronze_race_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Tests

# COMMAND ----------

ge_bronze_race_df = SparkDFDataset(bronze_race_df)

# not null quality check
pk_not_null_expectation = ge_bronze_race_df.expect_column_values_to_not_be_null(column="raceid")
if not pk_not_null_expectation["success"]: 
    raise Exception(pk_not_null_expectation)

fk_not_null_expectation = ge_bronze_race_df.expect_column_values_to_not_be_null(column="circuitid")
if not fk_not_null_expectation["success"]: 
    raise Exception(fk_not_null_expectation)

# data should include all years from 1950 to 2023
available_years = [year for year in range(1950, 2024)]
contain_set_expectation = ge_bronze_race_df.expect_column_distinct_values_to_equal_set(
    column="year",
    value_set=available_years
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
windowSpec = Window.partitionBy('raceid').orderBy(col("updated_at").desc())
silver_race_df = bronze_race_df.withColumn("_row_num", row_number().over(windowSpec))

silver_race_df = silver_race_df.filter(col("_row_num") == 1).drop("_row_num")

# COMMAND ----------

# clean time, fp1, fp2, fp3, quali, and sprint columns by replacing "\N" entries with null, concat them and replace their time
# rename columns
# select only subset of columns
silver_race_df = (
    silver_race_df
    .replace(
        {"\\N": None}, subset=[
            "time", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time",
            "quali_date", "quali_time", "sprint_date", "sprint_time"
        ]
    )
    .withColumns(
        {
            "datetime": when(
                col("time").isNull() & col("date").isNotNull(), 
                to_timestamp(concat(col("date"), lit(" "), lit("00:00:00")))
            ).otherwise(to_timestamp(concat(col("date"), lit(" "), col("time")))),

            "fp1_datetime": when(
                col("fp1_time").isNull() & col("fp1_date").isNotNull(), 
                to_timestamp(concat(col("fp1_date"), lit(" "), lit("00:00:00")))
            ).otherwise(to_timestamp(concat(col("fp1_date"), lit(" "), col("fp1_time")))),

            "fp2_datetime": when(
                col("fp2_time").isNull() & col("fp2_date").isNotNull(), 
                to_timestamp(concat(col("fp2_date"), lit(" "), lit("00:00:00")))
            ).otherwise(to_timestamp(concat(col("fp2_date"), lit(" "), col("fp2_time")))),

            "fp3_datetime": when(
                col("fp3_time").isNull() & col("fp3_date").isNotNull(), 
                to_timestamp(concat(col("fp3_date"), lit(" "), lit("00:00:00")))
            ).otherwise(to_timestamp(concat(col("fp3_date"), lit(" "), col("fp3_time")))),

            "quali_datetime": when(
                col("quali_time").isNull() & col("quali_date").isNotNull(), 
                to_timestamp(concat(col("quali_date"), lit(" "), lit("00:00:00")))
            ).otherwise(to_timestamp(concat(col("quali_date"), lit(" "), col("quali_time")))),

            "sprint_datetime": when(
                col("sprint_time").isNull() & col("sprint_date").isNotNull(), 
                to_timestamp(concat(col("sprint_date"), lit(" "), lit("00:00:00")))
            ).otherwise(to_timestamp(concat(col("sprint_date"), lit(" "), col("sprint_time"))))
        }
    )
    .withColumnsRenamed({"raceid": "race_id", "circuitid": "circuit_id"})
    .select(
        "race_id", "circuit_id", "name", "year", "round", "datetime", "fp1_datetime",
        "fp2_datetime", "fp3_datetime", "quali_datetime", "sprint_datetime", "url", "updated_at"
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
        f"describe detail {spark.conf.get('table_path.silver_race')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            silver_race_df.alias("newData"),
            "oldData.race_id = newData.race_id") \
        .whenMatchedUpdate(set = { "race_id": col("newData.race_id") }) \
        .whenNotMatchedInsert(values = { "race_id": col("newData.race_id") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.silver_race} (
            race_key BIGINT GENERATED ALWAYS AS IDENTITY,
            race_id INT,
            circuit_id INT,
            name STRING,
            year INT,
            round INT,
            datetime TIMESTAMP,
            fp1_datetime TIMESTAMP,
            fp2_datetime TIMESTAMP,
            fp3_datetime TIMESTAMP,
            quali_datetime TIMESTAMP,
            sprint_datetime TIMESTAMP,
            url STRING,
            updated_at TIMESTAMP
        );
        """
    )

    (silver_race_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(spark.conf.get("table_path.silver_race"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---