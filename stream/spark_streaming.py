# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, window
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# # -------------------------------
# # Spark Session
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("SmartCityTrafficStream") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # -------------------------------
# # Kafka Configuration
# # -------------------------------
# KAFKA_BROKER = "localhost:9092"
# RAW_TOPIC = "traffic_raw"
# ALERT_TOPIC = "critical_traffic"
# OUTPUT_PATH = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/processed/traffic_aggregates/"

# # -------------------------------
# # Schema for Kafka JSON
# # -------------------------------
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("vehicle_count", IntegerType(), True),
#     StructField("avg_speed", DoubleType(), True)
# ])

# # -------------------------------
# # Read Stream from Kafka
# # -------------------------------
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", RAW_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # -------------------------------
# # Parse JSON and Convert Timestamp
# # -------------------------------
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json("json_str", schema).alias("data")) \
#     .select(
#         col("data.sensor_id"),
#         col("data.vehicle_count"),
#         col("data.avg_speed"),
#         col("data.timestamp").cast(TimestampType())
#     )

# # -------------------------------
# # Real-time Alert Stream
# # -------------------------------
# alerts = df_parsed.filter(col("avg_speed") < 10)

# alerts.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", ALERT_TOPIC) \
#     .outputMode("append") \
#     .start()

# # -------------------------------
# # Aggregation: 5-minute Tumbling Window
# # -------------------------------

# #Handle late events
# agg_df = df_parsed.withWatermark("timestamp", "10 minutes") \
#     .groupBy(
#         window("timestamp", "5 minutes"),
#         "sensor_id"
#     ).agg(
#         {"vehicle_count": "sum", "avg_speed": "avg"}
#     ).select(
#         col("window.start").alias("window_start"),
#         col("window.end").alias("window_end"),
#         col("sensor_id"),
#         col("sum(vehicle_count)").alias("total_vehicles"),
#         col("avg(avg_speed)").alias("avg_speed")
#     )


# # -------------------------------
# # Write Aggregated Data to Parquet
# # -------------------------------
# query = agg_df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", OUTPUT_PATH) \
#     .option("checkpointLocation", "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/checkpoints/traffic_streaming/") \
#     .start()

# query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, window, to_json, struct
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# # -------------------------------
# # Spark Session
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("SmartCityTrafficStream") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # -------------------------------
# # Kafka Configuration
# # -------------------------------
# KAFKA_BROKER = "localhost:9092"
# RAW_TOPIC = "traffic_raw"
# ALERT_TOPIC = "critical_traffic"

# # Output path for aggregated data
# OUTPUT_PATH = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/processed/traffic_aggregates/"

# # Checkpoint directories
# AGG_CHECKPOINT = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/checkpoints/traffic_streaming/"
# ALERT_CHECKPOINT = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/checkpoints/alerts_streaming/"

# # -------------------------------
# # Schema for Kafka JSON
# # -------------------------------
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("vehicle_count", IntegerType(), True),
#     StructField("avg_speed", DoubleType(), True)
# ])

# # -------------------------------
# # Read Stream from Kafka
# # -------------------------------
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", RAW_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # -------------------------------
# # Parse JSON and Convert Timestamp
# # -------------------------------
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json("json_str", schema).alias("data")) \
#     .select(
#         col("data.sensor_id"),
#         col("data.vehicle_count"),
#         col("data.avg_speed"),
#         col("data.timestamp").cast(TimestampType()).alias("timestamp")
#     )

# # -------------------------------
# # Real-time Alert Stream
# # -------------------------------
# alerts = df_parsed.filter(col("avg_speed") < 10)

# # Kafka requires a 'value' column as STRING
# alerts_kafka = alerts.select(to_json(struct("*")).alias("value"))

# alerts_query = alerts_kafka.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", ALERT_TOPIC) \
#     .option("checkpointLocation", ALERT_CHECKPOINT) \
#     .outputMode("append") \
#     .start()

# # -------------------------------
# # Aggregation: 5-minute Tumbling Window with Watermark
# # -------------------------------
# agg_df = df_parsed.withWatermark("timestamp", "10 minutes") \
#     .groupBy(
#         window("timestamp", "5 minutes"),
#         "sensor_id"
#     ).agg(
#         {"vehicle_count": "sum", "avg_speed": "avg"}
#     ).select(
#         col("window.start").alias("window_start"),
#         col("window.end").alias("window_end"),
#         col("sensor_id"),
#         col("sum(vehicle_count)").alias("total_vehicles"),
#         col("avg(avg_speed)").alias("avg_speed")
#     )

# # -------------------------------
# # Write Aggregated Data to Parquet
# # -------------------------------
# agg_query = agg_df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", OUTPUT_PATH) \
#     .option("checkpointLocation", AGG_CHECKPOINT) \
#     .start()

# # -------------------------------
# # Await Termination for Both Streams
# # -------------------------------
# spark.streams.awaitAnyTermination()




# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     from_json, col, window, count, avg, to_timestamp
# )
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# # -------------------------------
# # PostgreSQL Configuration
# # -------------------------------
# POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
# POSTGRES_USER = "postgres"
# POSTGRES_PASSWORD = "abc"   
# POSTGRES_TABLE = "traffic_aggregates"

# # -------------------------------
# # Kafka Configuration
# # -------------------------------
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "traffic_raw"

# # -------------------------------
# # Initialize Spark Session
# # -------------------------------
# spark = SparkSession.builder \
#     .appName("TrafficStreamingToPostgreSQL") \
#     .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # -------------------------------
# # Define Schema for Kafka Messages
# # -------------------------------
# traffic_schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("vehicle_count", IntegerType(), True),
#     StructField("avg_speed", DoubleType(), True)
# ])

# # -------------------------------
# # Read from Kafka Stream
# # -------------------------------
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", TOPIC_NAME) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # -------------------------------
# # Parse JSON and Extract Fields
# # -------------------------------
# parsed_df = kafka_df.select(
#     from_json(col("value").cast("string"), traffic_schema).alias("data")
# ).select("data.*")

# # Convert timestamp string to timestamp type
# parsed_df = parsed_df.withColumn(
#     "timestamp", 
#     to_timestamp(col("timestamp"))
# )

# # -------------------------------
# # Windowed Aggregation (5-minute windows)
# # -------------------------------
# aggregated_df = parsed_df \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy(
#         window(col("timestamp"), "5 minutes"),
#         col("sensor_id")
#     ) \
#     .agg(
#         count("vehicle_count").alias("total_vehicles"),
#         avg("avg_speed").alias("avg_speed")
#     ) \
#     .select(
#         col("window.start").alias("window_start"),
#         col("window.end").alias("window_end"),
#         col("sensor_id"),
#         col("total_vehicles"),
#         col("avg_speed")
#     )

# # -------------------------------
# # Write to PostgreSQL
# # -------------------------------
# def write_to_postgres(batch_df, batch_id):
#     """Write each micro-batch to PostgreSQL"""
#     if batch_df.count() > 0:
#         batch_df.write \
#             .format("jdbc") \
#             .option("url", POSTGRES_URL) \
#             .option("dbtable", POSTGRES_TABLE) \
#             .option("user", POSTGRES_USER) \
#             .option("password", POSTGRES_PASSWORD) \
#             .option("driver", "org.postgresql.Driver") \
#             .mode("append") \
#             .save()
        
#         print(f"✅ Batch {batch_id}: Written {batch_df.count()} records to PostgreSQL")
#         batch_df.show(truncate=False)

# # -------------------------------
# # Start Streaming Query
# # -------------------------------
# query = aggregated_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("update") \
#     .trigger(processingTime="30 seconds") \
#     .start()

# print("🚀 Spark Streaming Job Started")
# print(f"📊 Aggregating traffic data in 5-minute windows")
# print(f"💾 Writing to PostgreSQL: {POSTGRES_TABLE}")
# print("-" * 60)

# query.awaitTermination()



from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    count,
    avg,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# =================================================
# PostgreSQL Configuration
# =================================================
POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "abc"
POSTGRES_TABLE = "traffic_aggregates"

# =================================================
# Parquet Configuration
# =================================================
PARQUET_PATH = (
    "/home/avishka/data/projects/mini-project-traffic/"
    "traffic-v1/airflow-astro/include/processed/traffic_aggregates_new"
)

# =================================================
# Kafka Configuration
# =================================================
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "traffic_raw"

# =================================================
# Initialize Spark Session
# =================================================
spark = SparkSession.builder \
    .appName("TrafficStreamingToPostgresAndParquet") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint/traffic") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =================================================
# Define Schema for Kafka Messages
# =================================================
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True)
])

# =================================================
# Read Kafka Stream
# =================================================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# =================================================
# Parse JSON
# =================================================
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), traffic_schema).alias("data")
).select("data.*")

# Convert timestamp string → TimestampType
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# =================================================
# Windowed Aggregation (5-minute windows)
# =================================================
aggregated_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        count("vehicle_count").alias("total_vehicles"),
        avg("avg_speed").alias("avg_speed")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("total_vehicles"),
        col("avg_speed")
    )

# =================================================
# Write Each Micro-batch to PostgreSQL & Parquet
# =================================================
def write_to_postgres_and_parquet(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        print(f"⚠️ Batch {batch_id}: No data")
        return

    # ---------- PostgreSQL ----------
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # ---------- Parquet ----------
    batch_df.write \
        .mode("append") \
        .partitionBy("sensor_id") \
        .parquet(PARQUET_PATH)

    print(f"✅ Batch {batch_id}: Written to PostgreSQL & Parquet")
    batch_df.show(truncate=False)

# =================================================
# Start Streaming Query
# =================================================
query = aggregated_df.writeStream \
    .foreachBatch(write_to_postgres_and_parquet) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

print("🚀 Spark Streaming Job Started")
print("📊 5-minute window aggregation")
print("💾 Writing to PostgreSQL & Parquet")
print("=" * 60)

query.awaitTermination()
