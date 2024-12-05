from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col

spark = SparkSession.builder.appName("StructExample").getOrCreate()

data = [("Alice", 25, "New York"), ("Bob", 30, "Los Angeles")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# Create a struct column combining name and age
df = df.withColumn("person_info", struct(col("name").alias("first"), col("age").alias("old")))

df.show()
df.printSchema()

header_info = [("SBDL-Contract", 1, 0), ]
spark.createDataFrame(header_info) \
        .toDF("eventType", "majorSchemaVersion", "minorSchemaVersion").show()