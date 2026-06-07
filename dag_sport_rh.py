from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import googlemaps
import os

# ================= CONFIGURATION GLOBALE =================
# Récupération des variables d'environnement définies dans le docker-compose
DATA_FOLDER = os.getenv("DATA_FOLDER", "/opt/airflow/data")
GOOGLE_KEY = os.getenv("GOOGLE_MAPS_KEY") 

DB_STR = os.getenv("DB_CONNECTION")
COMPANY_ADDRESS = os.getenv("COMPANY_ADDRESS", "1362 Av. des Platanes, 34970 Lattes")

FILE_RH = os.path.join(DATA_FOLDER, "Donnees_RH.xlsx")
# =========================================================

def func_verify_distances():
    """
    Vérifie la cohérence Domicile-Travail via l'API Google Maps.
    Compare la distance réelle avec les règles métiers définies pour les modes de transport.
    """
    print("Démarrage de la vérification des distances...")
    
    try:
        df_rh = pd.read_excel(FILE_RH)
    except FileNotFoundError:
        print(f"Erreur : Le fichier source {FILE_RH} est introuvable.")
        return

    if not GOOGLE_KEY:
        print("Erreur : Clé API Google Maps manquante dans l'environnement.")
        return
    
    try:
        gmaps = googlemaps.Client(key=GOOGLE_KEY)
    except ValueError:
        print("Erreur : Clé API Google Maps invalide.")
        return
    
    anomalies = []
    current_date = datetime.now().date()

    for index, row in df_rh.iterrows():
        user_address = f"{row['Adresse du domicile']}"
        transport = row["Moyen de déplacement"]
        
        # Ignorer les lignes sans moyen de transport renseigné
        if pd.isna(transport): continue

        try:
            # Mapping des modes de transport pour l'API Google Maps
            mode = "driving"
            if "Marche" in str(transport): mode = "walking"
            if "Vélo" in str(transport): mode = "bicycling"

            result = gmaps.distance_matrix(origins=[user_address], destinations=[COMPANY_ADDRESS], mode=mode)
            
            if result['rows'][0]['elements'][0]['status'] == 'OK':
                # Conversion de la distance de mètres en kilomètres
                distance_km = result['rows'][0]['elements'][0]['distance']['value'] / 1000
            else:
                continue
            
            # Application des règles métiers (Note de cadrage)
            is_suspect = False
            limit_km = 100 
            
            if "Marche" in str(transport) or "Running" in str(transport):
                limit_km = 15
                if distance_km > limit_km: is_suspect = True
            elif "Vélo" in str(transport) or "Trottinette" in str(transport):
                limit_km = 25
                if distance_km > limit_km: is_suspect = True
                
            if "Voiture" in str(transport) and distance_km > limit_km:
                is_suspect = True

            # Enregistrement des données suspectes
            if is_suspect:
                anomalies.append({
                    "ID": row["ID salarié"],
                    "Nom": row["Nom"],
                    "Transport": transport,
                    "Distance_Reelle": distance_km,
                    "Status": "Erreur Déclarative",
                    "Date_Calcul": current_date
                })
        except Exception as e:
            # En production, on loggerait l'erreur exacte ici
            pass

    # Sauvegarde des résultats dans la base de données PostgreSQL
    engine = create_engine(DB_STR)
    if anomalies:
        df_anomalies = pd.DataFrame(anomalies)
        try:
            # Nettoyage des calculs précédents pour la journée en cours (idempotence)
            with engine.begin() as connection:
                connection.execute(text("DELETE FROM reporting_anomalies_rh WHERE \"Date_Calcul\" = :date"), {"date": current_date})
        except Exception:
            pass 
            
        df_anomalies.to_sql('reporting_anomalies_rh', engine, if_exists='append', index=False)
        print(f"{len(anomalies)} anomalies enregistrées pour la date du {current_date}.")
    else:
        print("Aucune anomalie détectée ce jour.")
    engine.dispose()


def func_calcul_kpi(**kwargs):
    """
    Agrège les données RH et Strava pour calculer l'éligibilité aux primes et congés bien-être.
    Restitution prévue pour tableau de bord Power BI.
    """
    print("Démarrage du calcul des KPI...")
    
    # Récupération du taux de prime depuis les paramètres dynamiques du DAG
    pct_prime = float(kwargs.get('params', {}).get('prime_pct', 0.05))
    engine = create_engine(DB_STR)
    
    # Chargement du référentiel employés
    try:
        df_rh = pd.read_excel(FILE_RH)
    except FileNotFoundError:
        return
    
    # Extraction des activités sportives sur les 12 derniers mois (Périmètre du POC)
    try:
        query_incremental = "SELECT * FROM activites_strava WHERE date_activite >= CURRENT_DATE - INTERVAL '1 year'"
        df_strava = pd.read_sql(query_incremental, engine)
    except Exception:
        df_strava = pd.DataFrame(columns=["id_salarie"])
        
    # Standardisation des types pour sécuriser la jointure Pandas
    df_rh["ID salarié"] = df_rh["ID salarié"].astype(str)
    df_strava["id_salarie"] = df_strava["id_salarie"].astype(str)

    # Agrégation des activités et jointure
    if not df_strava.empty:
        activites_par_salarie = df_strava.groupby("id_salarie").size().reset_index(name="nb_activites")
        df_final = pd.merge(df_rh, activites_par_salarie, left_on="ID salarié", right_on="id_salarie", how="left")
        df_final["nb_activites"] = df_final["nb_activites"].fillna(0)
    else:
        df_final = df_rh.copy()
        df_final["nb_activites"] = 0
    
    # Règle métier 1 : 5 jours de congés accordés si >= 15 activités physiques à l'année
    df_final["Jours_Bien_Etre"] = df_final["nb_activites"].apply(lambda x: 5 if x >= 15 else 0)
    
    # Règle métier 2 : Éligibilité à la prime de 5% selon le moyen de déplacement
    moyens_propres = df_final["Moyen de déplacement"].astype(str).str.lower()
    mots_cles_sport = ["marche", "vélo", "velo", "running", "trottinette"]
    df_final["Eligible_Prime"] = moyens_propres.apply(lambda x: any(mot in x for mot in mots_cles_sport))
    
    # Gestion dynamique du nom de colonne pour le salaire brut et calcul du montant de la prime
    col_salaire = "Salaire brut" if "Salaire brut" in df_final.columns else "Salaire Annuel Brut"
    df_final["Montant_Prime"] = df_final.apply(lambda x: x.get(col_salaire, 0) * pct_prime if x["Eligible_Prime"] else 0, axis=1)
    
    # Ajout du timestamp d'exécution pour traçabilité
    df_final["Date_Calcul"] = datetime.now().date()
    
    # Écriture en base (Truncate/Load) pour garantir un reporting BI propre
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE reporting_rh_final"))
    
    df_final.to_sql('reporting_rh_final', engine, if_exists='append', index=False)
    engine.dispose()
    print("Mise à jour de la table reporting_rh_final terminée.")


# ================= DEFINITION DU DAG AIRFLOW =================
default_args = {
    'owner': 'data-engineer', 
    'retries': 0, 
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'pipeline_sport_rh_v3_complet',
    default_args=default_args,
    description='ETL RH/Strava : Validation Google Maps, Data Quality SODA & KPI',
    schedule_interval='0 8 * * *', # Exécution quotidienne à 8h00
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={"prime_pct": 0.05} # Paramètre de prime modifiable via l'UI Airflow
) as dag:

    task_verify = PythonOperator(
        task_id='verify_google_maps', 
        python_callable=func_verify_distances
    )
    
    task_dq_soda = BashOperator(
        task_id='data_quality_soda',
        bash_command='soda scan -d postgres_sport -c /opt/airflow/dags/configuration.yml /opt/airflow/dags/checks.yml'
    )
    
    task_kpi = PythonOperator(
        task_id='calcul_kpi', 
        python_callable=func_calcul_kpi
    )

    # Ordonnancement des tâches
    task_verify >> task_dq_soda >> task_kpi