from kafka import KafkaProducer
from kafka.serializer import Serializer
from pathlib import Path
import json
import uuid
import time
import random


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        if data is None:
            return None
        return json.dumps(data).encode("utf-8")


config_path = Path(__file__).parent / "API.json"
with open(config_path, encoding="utf-8") as f:
    kafka_config = json.load(f)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=JsonSerializer(),
    key_serializer=JsonSerializer(),
)


my_name = "serhii"
sensors_topic = f"{my_name}_iot_sensors"


sensor_id = random.randint(1000, 9999)
num_messages = 120

temperature = random.uniform(25, 45)
humidity = random.uniform(15, 85)

print(f"Sensor {sensor_id} started")

try:
    for i in range(num_messages):
        temperature = min(max(temperature + random.uniform(-2, 2), 10), 60)
        humidity = min(max(humidity + random.uniform(-4, 4), 0), 100)
        data = {
            "id": sensor_id,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "timestamp": time.time(),
        }
        producer.send(sensors_topic, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"Message {i} sent to topic '{sensors_topic}' successfully. Data: {data}")
        time.sleep(2)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
