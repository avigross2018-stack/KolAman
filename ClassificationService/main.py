import services, os, time, json
from dotenv import load_dotenv


load_dotenv()

KAFKA_RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW_ALERTS")

def main():
    # Trying to subscribe to topic
    while True:
        try:
            services.consumer.subscribe([KAFKA_RAW_TOPIC])
            break
        except:
            # Case topic not found where waiting for another try.
            time.sleep(3)
            continue
    try:
        while True:
            msg = services.consumer.poll(1.0)
            if msg is None:
                print("Waiting...")

            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Case the message received successfully.
                try:
                    dict_model = json.loads(msg.value())
                    if not check_exists_msg(dict_model):
                        # Case msg not in redis.
                        import_to_redis(dict_model)
                        # check_exists_msg(dict_model)
                    else:
                        # Case msg already in redis we are skipping the msg.
                        pass
                except json.decoder.JSONDecodeError:
                    # Check if the json message is corrupt.
                    pass
    except KeyboardInterrupt:
        print("System shutdown...")
        pass
    finally:
        services.consumer.close()


def import_to_redis(msg:dict):
    key = msg["alert_id"]
    # Config the ttl time per msg
    text_msg = json.dumps(msg)
    services.redis.expire(key, 60)
    services.redis.set(key, text_msg)

def get_from_redis(key:str):
    redis_text = services.redis.get(key)
    redis_dict = json.loads(redis_text)
    return redis_dict

def check_exists_msg(msg:dict):
    # Checking if the nessage exists in redis and if the fields the same.
    key = msg["alert_id"]
    keys_in_redis = services.redis.keys("*")
    for redis_key in keys_in_redis:
        redis_value = get_from_redis(redis_key)

        # Check if the important fields are the same.
        check_lon = msg["lon"] == redis_value["lon"]
        check_lat = msg["lat"] == redis_value["lat"]
        check_time = msg["timestamp"] == redis_value["timestamp"]
        check_title = msg["title"] == redis_value["title"]

        if check_lon and check_lat and check_time and check_title:
            return False

    return True


if __name__ == "__main__":
    main()
