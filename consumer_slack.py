import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import requests

# Load environment variables from .env file
load_dotenv()

# Fix pour le bug d'encodage Windows avec psycopg2 (évite le crash sur les accents)
os.environ["PGCLIENTENCODING"] = "utf-8"

# ================= CONFIGURATION =================
DB_STR = "postgresql+psycopg2://juliette:sport@127.0.0.1:5433/sport_db?client_encoding=utf8" 
KAFKA_BROKER = "127.0.0.1:19092"
TOPIC_NAME = "strava_activites"

# Lien optionnel pour l'alerte temps réel
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
# =================================================

def send_slack_alert(nom, sport, distance):
    """Envoie une notification dans Slack pour féliciter l'employé."""
    if not SLACK_WEBHOOK:
        return
    
    distance_km = distance / 1000
    message = {
        "text": f"🎉 *Nouvelle performance !* {nom} vient de terminer une session de {sport} de {distance_km} km. Félicitations !"
    }
    try:
        requests.post(SLACK_WEBHOOK, json=message)
    except Exception as e:
        print(f"⚠️ Erreur lors de l'envoi Slack : {e}")

def main():
    print("🚀 Démarrage du Consumer (Écoute Temps Réel)...")
    
    # 1. Connexion à PostgreSQL et préparation de la table
    engine = create_engine(DB_STR)
    try:
        with engine.begin() as conn:
            # --- LA SOLUTION RADICALE : ON ÉCRASE L'ANCIENNE TABLE ---
            print("🧹 Nettoyage de l'ancienne table en cours...")
            conn.execute(text("DROP TABLE IF EXISTS activites_strava;"))
            
            # --- CRÉATION DE LA NOUVELLE TABLE AVEC LA BONNE COLONNE ---
            conn.execute(text("""
                CREATE TABLE activites_strava (
                    id_salarie INTEGER,
                    nom VARCHAR(100),
                    date_activite TIMESTAMP,
                    sport VARCHAR(50),
                    distance_m INTEGER,
                    duree_s INTEGER,
                    commentaire TEXT
                )
            """))
        print("✅ Base de données connectée et table TOUTE NEUVE créée.")
    except Exception as e:
        print(f"❌ Impossible de se connecter à la base de données PostgreSQL.")
        print(f"Détail de l'erreur : {e}")
        print("💡 Astuce : Vérifie sur Docker Desktop que ton conteneur 'sport_postgres_db' est bien en cours d'exécution.")
        return 

    # 2. Connexion à Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest' 
        )
        print("✅ Connecté à Redpanda/Kafka. En attente de nouvelles activités...")
    except Exception as e:
        print(f"❌ Erreur de connexion à Kafka : {e}")
        return

    # 3. Boucle d'écoute infinie
    for message in consumer:
        data = message.value
        print(f"📥 Réception : {data['nom']} - {data['sport']} ({data['distance_m']}m)")
        
        # Insertion dans PostgreSQL
        insert_query = text("""
            INSERT INTO activites_strava 
            (id_salarie, nom, date_activite, sport, distance_m, duree_s, commentaire) 
            VALUES (:id_salarie, :nom, :date_activite, :sport, :distance_m, :duree_s, :commentaire)
        """)
        
        try:
            with engine.begin() as conn:
                conn.execute(insert_query, data)
                
            # Bonus : Déclenchement d'une alerte Slack pour les activités > 10 km
            if data['distance_m'] >= 10000:
                send_slack_alert(data['nom'], data['sport'], data['distance_m'])
                print("   🔔 Alerte Slack déclenchée !")
                
        except Exception as e:
            print(f"❌ Erreur d'insertion en base : {e}")

if __name__ == "__main__":
    main()