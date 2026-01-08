from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import Row

spark = SparkSession.builder.appName("TestPostgresWrite").getOrCreate()

data = [
    Row(sensor_id="JUNCTION_1", timestamp="2025-12-17 00:00:00", vehicle_count=10, avg_speed=20.28),
    Row(sensor_id="JUNCTION_2", timestamp="2025-12-17 00:00:00", vehicle_count=71, avg_speed=8.79),
]

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True)
])

df = spark.createDataFrame(data, schema)

PG_URL = "jdbc:postgresql://localhost:5432/postgres"
PG_TABLE = "traffic_aggregates"
PG_USER = "postgres"
PG_PASSWORD = "abc"

df.write \
    .format("jdbc") \
    .option("url", PG_URL) \
    .option("dbtable", PG_TABLE) \
    .option("user", PG_USER) \
    .option("password", PG_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
