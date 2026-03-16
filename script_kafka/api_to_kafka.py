import time
import json
import requests
from kafka import KafkaProducer

# CONFIGURACIÓN
API_KEY = "696e04fb47dff426affcab8759e9ef0d"
LAT = "-41.8101"
LON = "-68.9063"

URL = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&appid={API_KEY}"

KAFKA_BROKER = "172.31.36.50:9092"
TOPIC = "weather_stream"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("Iniciando envio de datos a Kafka...")

while True:
    try:
        response = requests.get(URL)

        if response.status_code == 200:
            data = response.json()

            producer.send(TOPIC, value=data)
            producer.flush()

            print("Dato enviado a Kafka correctamente")

        else:
            print("Error API:", response.status_code)

    except Exception as e:
        print("Error:", e)

    time.sleep(60)