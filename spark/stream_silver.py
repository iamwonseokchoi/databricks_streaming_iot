# Databricks notebook source
from pyspark.sql.functions import col
import time

bronze_table_path = "dbfs:/user/hive/warehouse/bronze_stream_table"

# Read bronze table as stream
stream_df = spark.readStream.format("delta").load(bronze_table_path)

def write_to_silver_table(batch_df, batch_id):
    devices = [f"Device-{i:03d}" for i in range(0, 10)]
    
    for device in devices:
        filtered_batch = batch_df.filter(col("Device") == device)
        checkpointLocation = f"/mnt/stream_project/structured-streaming/silver_checkpoint/{device}"

        (filtered_batch.write
            .format("delta")
            .mode("append")
            .option("checkpointLocation", checkpointLocation)
            .saveAsTable(f"silver_stream_table_{device[-1:]}"))

# Use forEachBatch
silver_stream = (stream_df.writeStream
            .foreachBatch(write_to_silver_table)
            .outputMode("append")
            .start())

silver_stream.awaitTermination()