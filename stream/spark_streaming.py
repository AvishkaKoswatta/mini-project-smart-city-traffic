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


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# -------------------------------
# Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("SmartCityTrafficStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# Kafka Configuration
# -------------------------------
KAFKA_BROKER = "localhost:9092"
RAW_TOPIC = "traffic_raw"
ALERT_TOPIC = "critical_traffic"

# Output path for aggregated data
OUTPUT_PATH = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/processed/traffic_aggregates/"

# Checkpoint directories
AGG_CHECKPOINT = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/checkpoints/traffic_streaming/"
ALERT_CHECKPOINT = "/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/checkpoints/alerts_streaming/"

# -------------------------------
# Schema for Kafka JSON
# -------------------------------
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True)
])

# -------------------------------
# Read Stream from Kafka
# -------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# -------------------------------
# Parse JSON and Convert Timestamp
# -------------------------------
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select(
        col("data.sensor_id"),
        col("data.vehicle_count"),
        col("data.avg_speed"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp")
    )

# -------------------------------
# Real-time Alert Stream
# -------------------------------
alerts = df_parsed.filter(col("avg_speed") < 10)

# Kafka requires a 'value' column as STRING
alerts_kafka = alerts.select(to_json(struct("*")).alias("value"))

alerts_query = alerts_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", ALERT_TOPIC) \
    .option("checkpointLocation", ALERT_CHECKPOINT) \
    .outputMode("append") \
    .start()

# -------------------------------
# Aggregation: 5-minute Tumbling Window with Watermark
# -------------------------------
agg_df = df_parsed.withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "sensor_id"
    ).agg(
        {"vehicle_count": "sum", "avg_speed": "avg"}
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("sum(vehicle_count)").alias("total_vehicles"),
        col("avg(avg_speed)").alias("avg_speed")
    )

# -------------------------------
# Write Aggregated Data to Parquet
# -------------------------------
agg_query = agg_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", AGG_CHECKPOINT) \
    .start()

# -------------------------------
# Await Termination for Both Streams
# -------------------------------
spark.streams.awaitAnyTermination()
