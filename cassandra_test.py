# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import uuid
import os
import sys
from pyspark import SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Spark-Cassandra") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.sql.catalog.client", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .config("spark.sql.catalog.client.spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Définir explicitement le schéma avec un UUID pour 'id'
schema = StructType([
    StructField("id", StringType(), False),  # UUID est un String ici
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Créer des données d'exemple
data = [(str(uuid.uuid4()), "Alice", 30), (str(uuid.uuid4()), "Bob", 25)]
columns = ["id", "name", "age"]

# Créer un DataFrame en utilisant le schéma défini
df = spark.createDataFrame(data, schema)

# Enregistrer le DataFrame dans Cassandra
df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="users", keyspace="test_keyspace") \
    .save()