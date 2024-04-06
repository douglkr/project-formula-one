# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType

# COMMAND ----------

# bronze circuit schema
bronze_circuit_schema = StructType([
    StructField("circuitid", IntegerType(), False),
    StructField("circuitref", StringType(), False),
    StructField("name", StringType(), False),
    StructField("location", StringType(), False),
    StructField("country", StringType(), False),
    StructField("lat", FloatType(), False),
    StructField("lng", FloatType(), False),
    StructField("alt", StringType(), False),
    StructField("url", StringType(), False),
    StructField("updated_at", TimestampType(), False), 
])

# COMMAND ----------

# bronze driver schema
bronze_driver_schema = StructType([
    StructField("driverid", IntegerType(), False),
    StructField("driverref", StringType(), False),
    StructField("number", StringType(), False),
    StructField("code", StringType(), False),
    StructField("forename", StringType(), False),
    StructField("surname", StringType(), False),
    StructField("dob", DateType(), False),
    StructField("nationality", StringType(), False),
    StructField("url", StringType(), False),
    StructField("updated_at", TimestampType(), False), 
])

# COMMAND ----------

# bronze constructor schema
bronze_constructor_schema = StructType([
    StructField("constructorid", IntegerType(), False),
    StructField("constructorref", StringType(), False),
    StructField("name", StringType(), False),
    StructField("nationality", StringType(), False),
    StructField("url", StringType(), False),
    StructField("updated_at", TimestampType(), False), 
])

# COMMAND ----------

# bronze race schema
bronze_race_schema = StructType([
    StructField("raceid", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("round", IntegerType(), False),
    StructField("circuitid", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("date", DateType(), False),
    StructField("time", StringType(), False),
    StructField("url", StringType(), False),
    StructField("fp1_date", StringType(), False),
    StructField("fp1_time", StringType(), False),
    StructField("fp2_date", StringType(), False),
    StructField("fp2_time", StringType(), False),
    StructField("fp3_date", StringType(), False),
    StructField("fp3_time", StringType(), False),
    StructField("quali_date", StringType(), False),
    StructField("quali_time", StringType(), False),
    StructField("sprint_date", StringType(), False),
    StructField("sprint_time", StringType(), False),
    StructField("updated_at", TimestampType(), False), 
])

# COMMAND ----------

# bronze race_result schema
bronze_race_result_schema = StructType([
    StructField("resultid", IntegerType(), False),
    StructField("raceid", IntegerType(), False),
    StructField("driverid", IntegerType(), False),
    StructField("constructorid", IntegerType(), False),
    StructField("number", StringType(), False),
    StructField("grid", IntegerType(), False),
    StructField("position", StringType(), False),
    StructField("positiontext", StringType(), False),
    StructField("positionorder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("milliseconds", StringType(), False),
    StructField("fastestlap", StringType(), False),
    StructField("rank", StringType(), False),
    StructField("fastestlaptime", StringType(), False),
    StructField("fastestlapspeed", StringType(), False),
    StructField("statusid", IntegerType(), False),
    StructField("updated_at", TimestampType(), False), 
])

# COMMAND ----------

# bronze sprint_result schema
bronze_sprint_result_schema = StructType([
    StructField("sprintresultid", IntegerType(), False),
    StructField("raceid", IntegerType(), False),
    StructField("driverid", IntegerType(), False),
    StructField("constructorid", IntegerType(), False),
    StructField("number", StringType(), False),
    StructField("grid", IntegerType(), False),
    StructField("position", StringType(), False),
    StructField("positiontext", StringType(), False),
    StructField("positionorder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("milliseconds", StringType(), False),
    StructField("fastestlap", StringType(), False),
    StructField("fastestlaptime", StringType(), False),
    StructField("statusid", IntegerType(), False),
    StructField("updated_at", TimestampType(), False), 
])

# COMMAND ----------

# bronze status schema
bronze_status_schema = StructType([
    StructField("statusid", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("updated_at", TimestampType(), False), 
])