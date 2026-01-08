# import json
# import time
# import random
# from datetime import datetime
# from kafka import KafkaProducer

# # -------------------------------
# # Kafka Configuration
# # -------------------------------
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "traffic_raw"

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # -------------------------------
# # Sensor Configuration
# # -------------------------------
# SENSORS = [
#     "JUNCTION_1",
#     "JUNCTION_2",
#     "JUNCTION_3",
#     "JUNCTION_4"
# ]

# # Probability of critical traffic (10%)
# CRITICAL_PROBABILITY = 0.1


# def generate_traffic_event(sensor_id: str) -> dict:
#     """
#     Generate a single traffic event.
#     Occasionally produces critical traffic with low speed.
#     """

#     is_critical = random.random() < CRITICAL_PROBABILITY

#     if is_critical:
#         avg_speed = round(random.uniform(5, 9), 2)       # Critical speed
#         vehicle_count = random.randint(60, 120)
#     else:
#         avg_speed = round(random.uniform(20, 45), 2)     # Normal speed
#         vehicle_count = random.randint(10, 60)

#     return {
#         "sensor_id": sensor_id,
#         "timestamp": datetime.utcnow().isoformat(),
#         "vehicle_count": vehicle_count,
#         "avg_speed": avg_speed
#     }


# def main():
#     print("Smart City Traffic Producer Started")
#     print("Sending data to Kafka topic:", TOPIC_NAME)

#     try:
#         while True:
#             for sensor in SENSORS:
#                 event = generate_traffic_event(sensor)

#                 producer.send(TOPIC_NAME, value=event)

#                 # Clean, self-explainable logs
#                 print(
#                     f"[{event['timestamp']}] "
#                     f"{event['sensor_id']} | "
#                     f"Vehicles: {event['vehicle_count']} | "
#                     f"Avg Speed: {event['avg_speed']} km/h"
#                 )

#             time.sleep(8)

#     except KeyboardInterrupt:
#         print("\nProducer stopped by user.")

#     finally:
#         producer.flush()
#         producer.close()


# if __name__ == "__main__":
#     main()




# import json
# import time
# import random
# from datetime import datetime, timedelta
# from kafka import KafkaProducer

# # -------------------------------
# # Kafka Configuration
# # -------------------------------
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "traffic_raw"

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # -------------------------------
# # Sensor Configuration
# # -------------------------------
# SENSORS = [
#     "JUNCTION_1",
#     "JUNCTION_2",
#     "JUNCTION_3",
#     "JUNCTION_4"
# ]

# # Probability of critical traffic (10%)
# CRITICAL_PROBABILITY = 0.1

# # -------------------------------
# # Simulation Configuration
# # -------------------------------
# SIMULATION_START = datetime(2025, 12, 17, 0, 0, 0)  # start at midnight
# MINUTES_PER_LOOP = 1  # each loop simulates 1 minute
# TOTAL_HOURS = 24      # simulate 24 hours
# TOTAL_ITERATIONS = TOTAL_HOURS * 60 // MINUTES_PER_LOOP  # 1440 iterations

# def generate_traffic_event(sensor_id: str, sim_time: datetime) -> dict:
#     """
#     Generate a single traffic event with a simulated timestamp.
#     Occasionally produces critical traffic with low speed.
#     """
#     is_critical = random.random() < CRITICAL_PROBABILITY

#     if is_critical:
#         avg_speed = round(random.uniform(5, 9), 2)       # Critical speed
#         vehicle_count = random.randint(60, 120)
#     else:
#         avg_speed = round(random.uniform(20, 45), 2)     # Normal speed
#         vehicle_count = random.randint(10, 60)

#     return {
#         "sensor_id": sensor_id,
#         "timestamp": sim_time.isoformat(),
#         "vehicle_count": vehicle_count,
#         "avg_speed": avg_speed
#     }

# def main():
#     print("Smart City Traffic Producer Started (Simulated 24h)")
#     print("Sending data to Kafka topic:", TOPIC_NAME)

#     sim_time = SIMULATION_START

#     try:
#         for _ in range(TOTAL_ITERATIONS):
#             for sensor in SENSORS:
#                 event = generate_traffic_event(sensor, sim_time)
#                 producer.send(TOPIC_NAME, value=event)

#                 # Clean, self-explainable logs
#                 print(
#                     f"[{event['timestamp']}] "
#                     f"{event['sensor_id']} | "
#                     f"Vehicles: {event['vehicle_count']} | "
#                     f"Avg Speed: {event['avg_speed']} km/h"
#                 )

#             # Increment simulated time
#             sim_time += timedelta(minutes=MINUTES_PER_LOOP)

#             # Optional: very short sleep to avoid CPU overload
#             time.sleep(0.01)  # 10ms

#     except KeyboardInterrupt:
#         print("\nProducer stopped by user.")

#     finally:
#         producer.flush()
#         producer.close()
#         print("Simulation completed.")

# if __name__ == "__main__":
#     main()





import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# -------------------------------
# Kafka Configuration
# -------------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "traffic_raw"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    acks="all",
    retries=5,
    linger_ms=50,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -------------------------------
# Sensor Configuration
# -------------------------------
SENSORS = [
    "JUNCTION_1",
    "JUNCTION_2",
    "JUNCTION_3",
    "JUNCTION_4"
]

# Probability of critical traffic (10%)
CRITICAL_PROBABILITY = 0.1

# -------------------------------
# Simulation Configuration
# -------------------------------
SIMULATION_START = datetime(2025, 12, 17, 0, 0, 0)
MINUTES_PER_LOOP = 1          # 1 event batch per minute
TOTAL_HOURS = 24
TOTAL_ITERATIONS = TOTAL_HOURS * 60 // MINUTES_PER_LOOP  # 1440 iterations

# -------------------------------
# Traffic Event Generator
# -------------------------------
def generate_traffic_event(sensor_id: str, sim_time: datetime) -> dict:
    is_critical = random.random() < CRITICAL_PROBABILITY

    if is_critical:
        avg_speed = round(random.uniform(5, 9), 2)
        vehicle_count = random.randint(60, 120)
    else:
        avg_speed = round(random.uniform(20, 45), 2)
        vehicle_count = random.randint(10, 60)

    return {
        "sensor_id": sensor_id,
        "timestamp": sim_time.isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": avg_speed
    }

# -------------------------------
# Main Producer Loop (REAL TIME)
# -------------------------------
def main():
    print("🚦 Smart City Traffic Producer Started")
    print("⏱️ Running in REAL TIME for 24 hours")
    print("📤 Kafka Topic:", TOPIC_NAME)
    print("-" * 60)

    sim_time = SIMULATION_START

    try:
        for minute in range(TOTAL_ITERATIONS):
            loop_start = time.time()

            for sensor in SENSORS:
                event = generate_traffic_event(sensor, sim_time)
                producer.send(TOPIC_NAME, value=event)

                print(
                    f"[{event['timestamp']}] "
                    f"{event['sensor_id']} | "
                    f"Vehicles: {event['vehicle_count']} | "
                    f"Avg Speed: {event['avg_speed']} km/h"
                )

            producer.flush()

            # Advance simulated time
            sim_time += timedelta(minutes=MINUTES_PER_LOOP)

            # -------------------------------
            # REAL-TIME pacing (sync to wall clock)
            # -------------------------------
            elapsed = time.time() - loop_start
            sleep_time = max(0, (MINUTES_PER_LOOP * 60) - elapsed)

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nProducer stopped manually.")

    finally:
        producer.close()
        print("24-hour traffic simulation completed.")

# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    main()
