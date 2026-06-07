import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092' 
TOPIC_NAME = 'strava_activites'
fake = Faker('fr_FR')

# ID correspondant au fichier Excel pour que les jointures marchent
EMPLOYEES = [
    {"id": 43015, "nom": "Juliette Mendes"},
    {"id": 43542, "nom": "Laurence Morvan"},
    {"id": 66425, "nom": "Thomas Durand"},
    {"id": 91916, "nom": "Sofiane Benali"},
    {"id": 35731, "nom": "Claire Dupont"}
]

ACTIVITES = ["Course à pied", "Vélo", "Marche", "Natation"]

def main():
    print("🚀 Démarrage du Simulateur Strava (Producer)...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Erreur connexion Kafka : {e}")
        print("💡 Vérifie que le conteneur 'redpanda' tourne bien (docker ps).")
        return

    print("📜 Envoi de l'historique (Batch)...")
    for _ in range(4000): # 4000 activités passées
        emp = random.choice(EMPLOYEES)
        data = {
            "id_salarie": emp["id"],
            "nom": emp["nom"],
            # Remplacement de date_debut par date_activite pour matcher avec Airflow
            "date_activite": fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
            "sport": random.choice(ACTIVITES),
            "distance_m": random.randint(2000, 15000),
            "duree_s": random.randint(1800, 5000),
            "commentaire": "Historique importé"
        }
        producer.send(TOPIC_NAME, data)
    print("✅ Historique envoyé.")

    print("🔴 Mode LIVE activé (Ctrl+C pour arrêter)...")
    while True:
        emp = random.choice(EMPLOYEES)
        sport = random.choice(ACTIVITES)
        data = {
            "id_salarie": emp["id"],
            "nom": emp["nom"],
            # Remplacement de date_debut par date_activite
            "date_activite": datetime.now().isoformat(),
            "sport": sport,
            "distance_m": random.randint(3000, 20000),
            "duree_s": random.randint(1200, 7200),
            "commentaire": fake.sentence()
        }
        
        print(f"   🏃 {data['nom']} termine : {sport} ({data['distance_m']}m)")
        producer.send(TOPIC_NAME, data)
        time.sleep(10) # Une activité toutes les 10 secondes

if __name__ == "__main__":
    main()