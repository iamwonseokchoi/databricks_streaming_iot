-- Databricks notebook source
-- Bronze Table

CREATE TABLE IF NOT EXISTS hive_metastore.default.bronze_stream_table
  (Recorded_At TIMESTAMP,
  Device STRING, 
  Signal_Value DECIMAL(5, 3),
  Latitude DECIMAL(10, 7),
  Longitude DECIMAL(10, 7)
  )
COMMENT 'bronze stream table'
PARTITIONED BY (Device)
TBLPROPERTIES ('quality' = 'bronze'); 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC # Silver Tables
-- MAGIC for index in range(0, 10):
-- MAGIC     spark.sql(f"""
-- MAGIC
-- MAGIC     CREATE TABLE IF NOT EXISTS hive_metastore.default.silver_stream_table_{index}
-- MAGIC     (Recorded_At TIMESTAMP,
-- MAGIC     Device STRING, 
-- MAGIC     Signal_Value DECIMAL(5, 3),
-- MAGIC     Latitude DECIMAL(10, 7),
-- MAGIC     Longitude DECIMAL(10, 7)
-- MAGIC     )
-- MAGIC     COMMENT 'bronze stream table'
-- MAGIC     PARTITIONED BY (Device)
-- MAGIC     TBLPROPERTIES ('quality' = 'silver'); 
-- MAGIC
-- MAGIC     """)