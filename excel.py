import pandas as pd

# 1. Définition des données pour l'onglet RAW (Activités Strava)
data_raw = [
    ["id", "INTEGER (PK)", "Auto-incrément", "Identifiant technique unique.", "1024"],
    ["id_salarie", "INTEGER", "Flux Kafka", "Identifiant salarié (clé de jointure).", "43015"],
    ["nom", "VARCHAR", "Flux Kafka", "Nom complet du salarié.", "Juliette Mendes"],
    ["date_debut", "TIMESTAMP", "Flux Kafka", "Date et heure de l'activité.", "2026-02-12 14:30:00"],
    ["sport", "VARCHAR", "Flux Kafka", "Type de sport.", "Course à pied"],
    ["distance_m", "INTEGER", "Flux Kafka", "Distance brute (mètres).", "5400"],
    ["duree_s", "INTEGER", "Flux Kafka", "Durée (secondes).", "1800"],
    ["commentaire", "TEXT", "Flux Kafka", "Métadonnée / Message.", "Entraînement matinal"]
]
df_raw = pd.DataFrame(data_raw, columns=["Champ", "Type", "Source", "Description", "Exemple"])

# 2. Définition des données pour l'onglet QUALITY (Anomalies)
data_quality = [
    ["id_salarie", "INTEGER (PK)", "Excel RH", "Identifiant salarié.", "Unique par snapshot"],
    ["nom", "VARCHAR", "Excel RH", "Nom complet.", "-"],
    ["transport_declare", "VARCHAR", "Excel RH", "Moyen de transport déclaré.", "Marche, Vélo..."],
    ["distance_reelle_km", "FLOAT", "API Google Maps", "Distance calculée domicile-travail.", "Trajet optimal"],
    ["statut", "VARCHAR", "Airflow", "Type d'erreur.", "Toujours 'Erreur Déclarative'"]
]
df_quality = pd.DataFrame(data_quality, columns=["Champ", "Type", "Source", "Description", "Règle/Exemple"])

# 3. Définition des données pour l'onglet GOLD (Reporting Final)
data_gold = [
    ["id_salarie", "INTEGER (PK)", "Excel RH", "Clé primaire.", "-"],
    ["nom_prenom", "VARCHAR", "Excel RH", "Nom complet.", "-"],
    ["salaire_brut", "FLOAT", "Excel RH", "Salaire annuel brut.", "-"],
    ["nb_activites", "INTEGER", "Agg SQL", "Nombre total d'activités.", "COUNT(*)"],
    ["jours_bien_etre", "INTEGER", "Airflow", "Jours offerts.", "SI activites >= 15 ALORS 5"],
    ["eligible_prime", "BOOLEAN", "Airflow", "Éligibilité bonus.", "VRAI si sport écolo"],
    ["montant_prime", "FLOAT", "Airflow", "Montant versé (€).", "5% du brut si éligible"]
]
df_gold = pd.DataFrame(data_gold, columns=["Champ", "Type", "Source", "Description", "Formule de Calcul"])

# 4. Création du fichier Excel avec les 3 onglets
nom_fichier = "Dictionnaire_Donnees_Sport_RH.xlsx"
with pd.ExcelWriter(nom_fichier, engine='openpyxl') as writer:
    df_raw.to_excel(writer, sheet_name='RAW - Activites Strava', index=False)
    df_quality.to_excel(writer, sheet_name='QUALITY - Anomalies', index=False)
    df_gold.to_excel(writer, sheet_name='GOLD - Reporting RH', index=False)

print(f"✅ Fichier '{nom_fichier}' généré avec succès !")