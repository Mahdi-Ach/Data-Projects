import json
import os
import time
import requests
from kafka import KafkaProducer

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Coordonnées de quelques villes (latitude, longitude)
villes = {
    "Paris": (48.8566, 2.3522),
    "Lyon": (45.7640, 4.8357),
    "Marseille": (43.2965, 5.3698)
}

for nom, (lat, lon) in villes.items():
    # Appel à l'API Open-Meteo (gratuit, sans clé)
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
    response = requests.get(url)
    data = response.json()

    event = {
        "id": int(time.time() * 1000) + hash(nom),  # ID unique
        "ville": nom,
        "temperature": data["current"]["temperature_2m"],
        "humidite": data["current"]["relative_humidity_2m"],
        "vent": data["current"]["wind_speed_10m"],
        "timestamp": time.time()
    }

    producer.send('events', value=event)
    print(f"Envoyé : {event}")

producer.flush()
producer.close()