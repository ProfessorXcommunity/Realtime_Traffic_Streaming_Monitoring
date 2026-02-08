from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
    .appName("RealTimeTraffic") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "4")
# print("=== RUNNING MANUAL JDBC TEST ===")

# test_df = spark.createDataFrame(
#     [
#         ("2026-02-08 12:30:00", 99, 0.25, 150.0, 10, False)
#     ],
#     ["window_start", "requests_per_min", "error_rate", "avg_response_time", "unique_users", "is_anomaly"]
# ).withColumn(
#     "window_start",
#     to_timestamp(col("window_start"))
# )


# test_df.show(truncate=False)

# test_df.write \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://postgres:5432/traffic") \
#     .option("dbtable", "traffic_metrics") \
#     .option("user", "postgres") \
#     .option("password", "postgres") \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("append") \
#     .save()

# print("=== MANUAL JDBC WRITE COMPLETED ===")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("page", StringType()),
    StructField("event_type", StringType()),
    StructField("response_time_ms", IntegerType()),
    StructField("status_code", IntegerType())
])

# Wait for Kafka to be ready
time.sleep(10)

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web_events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.session.timeout.ms", "30000") \
    .load()

events_df = raw_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX")) \
    .filter(col('timestamp').isNotNull())

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
    col("window.start").cast("timestamp").alias("window_start"),
    col("requests_per_min").cast("long"),
    col("error_rate").cast("double"),
    col("avg_response_time").cast("double"),
    col("unique_users").cast("long"),
    (col("requests_per_min") > 100).cast("boolean").alias("is_anomaly")
)

def write_to_postgres(df, batch_id):

    # print("\n==============================")
    # print(f"FOREACH BATCH CALLED → batch_id={batch_id}")
    # print(f"Row count in batch = {df.count()}")
    # df.show(truncate=False)
    # print("==============================\n")

    if df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping write")
        return
    print(f"BATCH {batch_id}, rows={df.count()}")
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/traffic") \
            .option("dbtable", "traffic_metrics") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option('driver','org.postgresql.Driver')\
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote batch {batch_id} to PostgreSQL")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {e}")

query = result_df.writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/traffic_v3") \
    .foreachBatch(write_to_postgres) \
    .start()

print("Streaming query started successfully")
query.awaitTermination()