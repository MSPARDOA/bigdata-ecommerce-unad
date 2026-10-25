from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType
import logging

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingSales") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("category", StringType()),
    StructField("price", FloatType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", LongType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_data") \
    .load()

raw_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

parsed_df = raw_parsed.withColumn("timestamp", col("timestamp").cast("timestamp"))

windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "category") \
    .agg({"price": "avg", "quantity": "sum"})

query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
