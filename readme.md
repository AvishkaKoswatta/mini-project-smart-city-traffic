KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo $KAFKA_CLUSTER_ID

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --topic traffic_raw --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic critical_traffic --bootstrap-server localhost:9092

bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic traffic_raw 
{"sensor_id":"JUNCTION_1","timestamp":"2025-12-17T16:55:00","vehicle_count":45,"avg_speed":12.5}

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic traffic_raw --from-beginning

<!-- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 stream/spark_streaming.py -->
spark-submit \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.6.0 \
  stream/spark_streaming.py


psql -h localhost -U postgres -d postgres


<!-- pkill -f airflow
airflow scheduler
airflow api-server -->
