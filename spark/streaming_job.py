from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("RealTimeTraffic") \
    .getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("page", StringType()),
    StructField("event_type", StringType()),
    StructField("response_time_ms", IntegerType()),
    StructField("status_code", IntegerType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web_events") \
    .option("startingOffsets", "latest") \
    .load()

events_df = raw_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

agg_df = events_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        count("*").alias("requests_per_min"),
        avg("response_time_ms").alias("avg_response_time"),
        approx_count_distinct("user_id").alias("unique_users"),
        (sum(when(col("status_code") >= 400, 1).otherwise(0)) / count("*")).alias("error_rate")
    )

result_df = agg_df.select(
    col("window.start").alias("window_start"),
    "requests_per_min",
    "error_rate",
    "avg_response_time",
    "unique_users",
    (col("requests_per_min") > 100).alias("is_anomaly")
)

def write_to_postgres(df, batch_id):
    if df.rdd.isEmpty():
        return

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/traffic") \
        .option("dbtable", "traffic_metrics") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()

query = result_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/traffic") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()
