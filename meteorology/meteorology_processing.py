import os
import sys
import uuid
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, regexp_replace
from pyspark.sql.types import StringType

# Créer une session Spark--packages 
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
    .option("subscribe", "data") \
    .option("startingOffsets", "earliest") \
        .option("failOnDataLoss","false") \
    .load()

# Convertir la valeur du message en String et parser le JSON
df_raw = df.selectExpr("CAST(value AS STRING)")

df_lines = df_raw.withColumn("lines", split(col("value"), "\n"))

df_exploded = df_lines.selectExpr("explode(lines) as line")

df_transformed = df_exploded.withColumn("columns", split(col("line"), ";"))

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

df_final = df_transformed.select(
    col("columns").getItem(0).alias("year").cast("int"),
    col("columns").getItem(1).alias("month").cast("int"),
    col("columns").getItem(2).alias("lat").cast("string"),
    regexp_replace(col("columns").getItem(3), ",", ".").cast("double").alias("lon_175_180w"),
    regexp_replace(col("columns").getItem(4), ",", ".").cast("double").alias("lon_170_175w"),
    regexp_replace(col("columns").getItem(5), ",", ".").cast("double").alias("lon_165_170w"),
    regexp_replace(col("columns").getItem(6), ",", ".").cast("double").alias("lon_160_165w"),
    regexp_replace(col("columns").getItem(7), ",", ".").cast("double").alias("lon_155_160w"),
    regexp_replace(col("columns").getItem(8), ",", ".").cast("double").alias("lon_150_155w"),
    regexp_replace(col("columns").getItem(9), ",", ".").cast("double").alias("lon_145_150w"),
    regexp_replace(col("columns").getItem(10), ",", ".").cast("double").alias("lon_140_145e"),
    regexp_replace(col("columns").getItem(11), ",", ".").cast("double").alias("lon_145_150e"),
    regexp_replace(col("columns").getItem(12), ",", ".").cast("double").alias("lon_150_155e"),
    regexp_replace(col("columns").getItem(13), ",", ".").cast("double").alias("lon_155_160e"),
    regexp_replace(col("columns").getItem(14), ",", ".").cast("double").alias("lon_160_165e"),
    regexp_replace(col("columns").getItem(15), ",", ".").cast("double").alias("lon_165_170e"),
    regexp_replace(col("columns").getItem(16), ",", ".").cast("double").alias("lon_170_175e"),
    regexp_replace(col("columns").getItem(17), ",", ".").cast("double").alias("lon_175_180e"),
).withColumn("id", uuid_udf())

query_cassandra = df_final.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "projeti2") \
    .option("table", "historical_climate") \
    .option("checkpointLocation", "/tmp/checkpoint/") \
    .outputMode("append") \
    .start()

# Afficher le flux en continu
query = df_final.writeStream.outputMode("update").format("console").option("truncate", "false").start()
# query2 = df_summary.writeStream.outputMode("update").format("console").option("truncate", "false").start()

query.awaitTermination()
# query2.awaitTermination()
query_cassandra.awaitTermination()