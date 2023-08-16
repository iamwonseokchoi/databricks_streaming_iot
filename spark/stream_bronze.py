# Databricks notebook source
# MAGIC %run ./dataset_mount_script

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, TimestampType, DecimalType, StringType

# Match schema to mock data
dataSchema = StructType([
  StructField("Recorded_At", TimestampType(), True),
  StructField("Device", StringType(), True),
  StructField("Signal_Value", DecimalType(5, 3), True),
  StructField("Latitude", DecimalType(10, 7), True),
  StructField("Longitude", DecimalType(10, 7), True)
])

# COMMAND ----------

# For read
dataPath = "/mnt/databricks-stream-eh/path/"

# For write and read
userhome = "dbfs:/mnt/stream_project"
basePath = userhome + "/structured-streaming" 
dbutils.fs.mkdirs(basePath)                                   
outputPathDir = basePath + "/output"                  
checkpointPath = basePath + "/checkpoint"   
table = "hive_metastore.default.bronze_stream_table"               

# COMMAND ----------

df = (spark.readStream
    .option("maxFilesPerTrigger", 1)
    .option("maxBytesPerTrigger", 500000)
    .option("checkpointLocation", checkpointPath)
    .schema(dataSchema)
    .json(dataPath)
)

# COMMAND ----------

# Assert read is streaming before write
assert df.isStreaming

# COMMAND ----------

stream_query = (df.writeStream
    .queryName("stream_1")
    .trigger(processingTime="5 seconds")
    .format("parquet")
    .option("checkpointLocation", checkpointPath)
    .outputMode("append")
    .toTable(table)
)

stream_query.awaitTermination()

# COMMAND ----------

myStream = "mock_data_stream"
display(df, streamName = myStream)