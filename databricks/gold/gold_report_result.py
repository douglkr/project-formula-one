# Databricks notebook source
# MAGIC %md
# MAGIC # Gold layer - report_result

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
# MAGIC ### Load data from Gold Layer

# COMMAND ----------

gold_report_result_df = spark.sql(
    """
    WITH consolidated_result AS (
        SELECT
            race_result_key AS result_key,
            race_key,
            driver_key,
            constructor_key,
            "full_race" AS race_type,
            position_order,
            points,
            laps,
            milliseconds,
            status_key
        FROM hive_metastore.douglaskkr15.gold_fct_race_result

        UNION ALL

        SELECT
            sprint_result_key AS result_key,
            race_key,
            driver_key,
            constructor_key,
            "sprint" AS race_type,
            position_order,
            points,
            laps,
            milliseconds,
            status_key
        FROM hive_metastore.douglaskkr15.gold_fct_sprint_result
    )

    SELECT
        cr.result_key,
        cr.race_type,
        dr.race_name,
        dr.race_year,
        dr.race_date,
        dr.circuit_name,
        dr.circuit_country,
        dr.circuit_location,
        dr.circuit_lat,
        dr.circuit_lng,
        dr.circuit_alt,
        dd.driver_ref,
        dd.driver_number,
        dd.driver_fullname,
        dd.driver_date_of_birth,
        dd.driver_nationality,
        dco.constructor_ref,
        dco.constructor_name,
        dco.constructor_nationality,
        ds.status_name,
        cr.position_order,
        cr.points,
        cr.laps,
        cr.milliseconds
    FROM consolidated_result cr
    LEFT JOIN hive_metastore.douglaskkr15.gold_dim_race dr
        ON cr.race_key = dr.race_key
    LEFT JOIN hive_metastore.douglaskkr15.gold_dim_driver dd
        ON cr.driver_key = dd.driver_key
    LEFT JOIN hive_metastore.douglaskkr15.gold_dim_constructor dco
        ON cr.constructor_key = dco.constructor_key
    LEFT JOIN hive_metastore.douglaskkr15.gold_dim_status ds
        ON cr.status_key = ds.status_key
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load report table

# COMMAND ----------

# Check if table exists
try:
    table_location = spark.sql(
        f"describe detail {spark.conf.get('table_path.gold_report_result')}"
    ).select("location").collect()[0][0]
    deltaTable = DeltaTable.forPath(spark, table_location)
    
    # upsert merge 
    print("Table already exists")
    deltaTable.alias("oldData") \
        .merge(
            gold_report_result_df.alias("newData"),
            "oldData.result_key = newData.result_key and oldData.race_type = newData.race_type") \
        .whenMatchedUpdate(set = { "result_key": col("newData.result_key") }) \
        .whenNotMatchedInsert(values = { "result_key": col("newData.result_key") }) \
        .execute()
except AnalysisException:
    # table does not exist - create it for the 1st time
    print("Table does not exist")
    spark.sql(
        """
        CREATE OR REPLACE TABLE ${table_path.gold_report_result} (
            result_key BIGINT,
            race_type STRING,
            race_name STRING,
            race_year INT,
            race_date DATE,
            circuit_name STRING,
            circuit_country STRING,
            circuit_location STRING,
            circuit_lat FLOAT,
            circuit_lng FLOAT,
            circuit_alt INT,
            driver_ref STRING,
            driver_number INT,
            driver_fullname STRING,
            driver_date_of_birth DATE,
            driver_nationality STRING,
            constructor_ref STRING,
            constructor_name STRING,
            constructor_nationality STRING,
            status_name STRING,
            position_order INT,
            points FLOAT,
            laps INT,
            milliseconds LONG
        );
        """
    )

    (gold_report_result_df
        .sort(col("result_key").asc())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(spark.conf.get("table_path.gold_report_result"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ---