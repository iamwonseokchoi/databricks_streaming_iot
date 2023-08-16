# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, LongType

userhome = "dbfs:/mnt/stream_project"
dbutils.fs.rm(userhome + "/eventhub", True)

activityDataDF = spark.read.json("/mnt/databricks-stream-eh/path/")

tempJson = spark.createDataFrame(activityDataDF.toJSON(), StringType())

tempJson.write.mode("overwrite").format("delta").save(userhome + "/eventhub")

activityStreamDF = (spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .load(userhome + "/eventhub")
)

# COMMAND ----------

event_hub_connection_string = "Endpoint=sb://<Connection String>" 
event_hub_name = "src-streaming"
connection_string = event_hub_connection_string + ";EntityPath=" + event_hub_name

print("Consumer Connection String: {}".format(connection_string))

# COMMAND ----------

ehWriteConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
}

checkpointPath = "/mnt/stream_project/eventhub/checkpoint/"
dbutils.fs.rm(checkpointPath,True)

(activityStreamDF
  .writeStream
  .format("eventhubs")
  .options(**ehWriteConf)
  .option("checkpointLocation", checkpointPath)
  .start())