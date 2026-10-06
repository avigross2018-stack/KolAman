from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import redis as rd
import geopandas as gpd
from shapely.geometry import Point
import pika
from elasticsearch8 import Elasticsearch


load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_GROUP_ID = os.getenv("KAFKA_RAW_GROUP_ID")

REDIS_HOST= os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

RABBIT_HOST = os.getenv("RABBITMQ_HOST")
RABBIT_PORT = os.getenv("RABBIT_PORT")
RABBIT_USER = os.getenv("RABBITMQ_DEFAULT_USER")
RABBIT_PASSWORD = os.getenv("RABBITMQ_DEFAULT_PASS")

ELASTIC_BASE_URL = os.getenv("ELASTIC_BASE_URL")

# ==== Kafka Service ====
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)


# ==== Redis Service ====
redis = rd.Redis(host=REDIS_HOST, port=int(REDIS_PORT), password=REDIS_PASSWORD)


# ==== Calc Polygon ===-
def get_region(file_path:str, lon:float, lat:str) -> str:
    gdf = gpd.read_file(file_path)
    point = Point(lon, lat)
    matched = gdf[gdf.geometry.contains(point)]
    if not matched.empty:
        return matched.iloc[0]["region"]
    return "OVERSEAS"


# ==== Rabbit Service ===
rabbit_con = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD),
    )
)
channel = rabbit_con.channel()


# ==== elastic service ===
client = Elasticsearch(ELASTIC_BASE_URL)
