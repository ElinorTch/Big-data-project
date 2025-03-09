import time
import random
import json
from confluent_kafka import Producer

# 🔗 Configuration Kafka
KAFKA_BROKER = "localhost:9092"  # Adresse du broker Kafka
TOPIC = "agriculture-sensors"

# 📡 Configurer le producteur Kafka
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER
})

# 📡 Simuler les capteurs agricoles
def generate_sensor_data():
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(10, 40), 2),  # °C
        "humidity_soil": round(random.uniform(20, 80), 2),  # %
        "air_quality": round(random.uniform(50, 150), 2),  # AQI
    }
    return data

print("📡 Simulation des capteurs en cours...")

# 🔄 Envoyer des données en continu
def delivery_report(err, msg):
    """Retour d'information sur l'envoi du message Kafka."""
    if err is not None:
        print(f"Erreur lors de l'envoi du message : {err}")
    else:
        print(f"Message envoyé à {msg.topic()} [{msg.partition()}]")

while True:
    sensor_data = generate_sensor_data()
    producer.produce(TOPIC, json.dumps(sensor_data).encode('utf-8'), callback=delivery_report)
    print(f"📤 Envoyé : {sensor_data}")

    producer.poll(0)  # Poll pour traiter les messages en attente
    time.sleep(2)  # Simule un envoi toutes les 2 secondes

