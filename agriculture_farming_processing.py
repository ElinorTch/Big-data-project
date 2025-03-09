import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, split, regexp_replace

# Créer une session Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Subscribe to 1 topic
spark = SparkSession \
          .builder \
          .appName("APP") \
          .getOrCreate()

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "agriculture_farming") \
      .option("startingOffsets", "earliest") \
      .load()

# Convertir la valeur du message en String et parser le JSON
df_raw = df.selectExpr("CAST(value AS STRING)")

df_lines = df_raw.withColumn("lines", split(col("value"), "\n"))

df_exploded = df_lines.selectExpr("explode(lines) as line")

df_transformed = df_exploded.withColumn("columns", split(col("line"), ","))

df_final = df_transformed.select(
    col("columns").getItem(0).alias("Farm_ID").cast("string"),
    col("columns").getItem(1).alias("Crop_Type").cast("string"),
    regexp_replace(col("columns").getItem(2), ",", ".").cast("double").alias("Farm_Area(acres)"),
    col("columns").getItem(3).alias("Irrigation_Type").cast("string"),
    regexp_replace(col("columns").getItem(4), ",", ".").cast("double").alias("Fertilizer_Used(tons)"),
    regexp_replace(col("columns").getItem(5), ",", ".").cast("double").alias("Pesticide_Used(kg)"),
    regexp_replace(col("columns").getItem(6), ",", ".").cast("double").alias("Yield(tons)"),
    col("columns").getItem(7).alias("Soil_Type").cast("string"),
    col("columns").getItem(8).alias("Season").cast("string"),
    regexp_replace(col("columns").getItem(9), ",", ".").cast("double").alias("Water_Usage(cubic meters)"),
)
#supprimer les valeurs nulles du datafrasme
df_filtered = df_final.filter(col("Farm_ID").rlike("^F([0-9]+)$"))

# Afficher le flux en continu
query = df_filtered.writeStream.outputMode("update").format("console").option("truncate", "false").start()
query_hdfs = df_filtered.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:8088/user/hadoop/processed_data") \
    .option("checkpointLocation", "/tmp/hdfs-checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
query_hdfs.awaitTermination()