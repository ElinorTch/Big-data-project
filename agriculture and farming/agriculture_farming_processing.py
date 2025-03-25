import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, split, regexp_replace

# Créer une session Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Subscribe to 1 topic
spark = SparkSession \
        .builder \
        .appName("APP") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.catalog.client", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .config("spark.sql.catalog.client.spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "agriculture_farming") \
      .option("startingOffsets", "earliest") \
        .option("failOnDataLoss","false") \
      .load()

# Convertir la valeur du message en String et parser le JSON
df_raw = df.selectExpr("CAST(value AS STRING)")

df_lines = df_raw.withColumn("lines", split(col("value"), "\n"))

df_exploded = df_lines.selectExpr("explode(lines) as line")

df_transformed = df_exploded.withColumn("columns", split(col("line"), ","))

df_final = df_transformed.select(
    col("columns").getItem(0).alias("farm_id").cast("string"),
    col("columns").getItem(1).alias("crop_type").cast("string"),
    regexp_replace(col("columns").getItem(2), ",", ".").cast("double").alias("farm_area_acres"),
    col("columns").getItem(3).alias("irrigation_type").cast("string"),
    regexp_replace(col("columns").getItem(4), ",", ".").cast("double").alias("fertilizer_used_tons"),
    regexp_replace(col("columns").getItem(5), ",", ".").cast("double").alias("pesticide_used_kg"),
    regexp_replace(col("columns").getItem(6), ",", ".").cast("double").alias("yield_tons"),
    col("columns").getItem(7).alias("soil_type").cast("string"),
    col("columns").getItem(8).alias("season").cast("string"),
    regexp_replace(col("columns").getItem(9), ",", ".").cast("double").alias("water_usage_cubic_meters"),
)

#supprimer les valeurs nulles du datafrasme
df_filtered = df_final.filter(col("farm_id").rlike("^F([0-9]+)$"))

# Afficher le flux en continu
query_cassandra = df_filtered.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "data") \
    .option("table", "farm_data") \
    .option("checkpointLocation", "/tmp/checkpoint/") \
    .outputMode("append") \
    .start()
query = df_filtered.writeStream.outputMode("update").format("console").option("truncate", "false").start()

query.awaitTermination()
query_cassandra.awaitTermination()