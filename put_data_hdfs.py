from pyspark.sql import SparkSession

sparkSession = SparkSession \
          .builder \
          .appName("APP") \
          .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8088") \
          .getOrCreate()

# Create a DataFrame from the provided data.
data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
df = sparkSession.createDataFrame(data)

# Write a CSV file in HDFS.
df.write.csv("hdfs://localhost:8088/hdfs/test/example.csv")