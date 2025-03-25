import os
import sys
import uuid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, classification_report
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
      .load()

# Convertir la valeur du message en String et parser le JSON
df_raw = df.selectExpr("CAST(value AS STRING)")

df_lines = df_raw.withColumn("lines", split(col("value"), "\n"))

df_exploded = df_lines.selectExpr("explode(lines) as line")

df_transformed = df_exploded.withColumn("columns", split(col("line"), ";"))

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

df_final = df_transformed.select(
    col("columns").getItem(0).alias("n").cast("int"),
    col("columns").getItem(1).alias("p").cast("int"),
    col("columns").getItem(2).alias("k").cast("int"),
    regexp_replace(col("columns").getItem(3), ",", ".").cast("double").alias("temperature"),
    regexp_replace(col("columns").getItem(4), ",", ".").cast("double").alias("humidity"),
    regexp_replace(col("columns").getItem(5), ",", ".").cast("double").alias("ph"),
    regexp_replace(col("columns").getItem(6), ",", ".").cast("double").alias("rainfall"),
    col("columns").getItem(7).alias("label")
).withColumn("id", uuid_udf())

#Nettoyage
df_filtered = df_final.filter(col("N").rlike("^[0-9]+$"))
# df_filtered.replace(',', '.', regex=True, inplace=True)
# cols_to_convert = ['temperature', 'humidity', 'ph', 'rainfall']
# df_filtered.dropna(inplace=True)
# df_filtered.drop_duplicates(inplace=True)


#Show the summary of the dataframe
df_summary = df_filtered.select("N", "P", "K").describe()

#Afficher les lignes avec des null
df_null = df_final.filter(
    col("N").isNull() | 
    col("P").isNull() | 
    col("K").isNull() | 
    col("temperature").isNull() | 
    col("humidity").isNull() | 
    col("ph").isNull() | 
    col("rainfall").isNull() | 
    col("label").isNull()
)

query_cassandra = df_filtered.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "data") \
    .option("table", "informations") \
    .option("checkpointLocation", "/tmp/checkpoint/") \
    .outputMode("append") \
    .start()

# Afficher le flux en continu
query = df_filtered.writeStream.outputMode("update").format("console").option("truncate", "false").start()
query2 = df_summary.writeStream.outputMode("update").format("console").option("truncate", "false").start()

query.awaitTermination()
query2.awaitTermination()
query_cassandra.awaitTermination()