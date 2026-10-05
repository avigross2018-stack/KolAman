from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import redis as rd

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_RAW_GROUP_ID")

REDIS_HOST= os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
# ==== Kafka Service ====
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)


# ==== Redis Service ====
redis = rd.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=REDIS_PASSWORD)
