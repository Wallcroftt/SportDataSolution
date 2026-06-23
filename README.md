```markdown
# 🏅 Sport Data Solution - Architecture Hybride (Temps Réel & Batch)

Ce projet propose une infrastructure de données complète (Data Engineering) visant à automatiser le calcul des primes sportives et des congés "Bien-être" pour les salariés d'une entreprise. 

Le pipeline ingère des données de performances sportives en temps réel, garantit leur qualité, valide les données déclaratives RH via des services tiers, et croise l'ensemble pour une restitution financière claire.

## 🏗️ Architecture Technique

L'architecture repose sur un modèle hybride combinant traitement en streaming (temps réel) et traitement 
par lots (Batch) :

* **Ingestion (Temps Réel) :** `Python (Producer/Consumer)` sur `Redpanda` (Architecture compatible 
API Apache Kafka)
* **Stockage (Zone d'atterrissage & Data Warehouse) :** `PostgreSQL`
* **Qualité des Données (Data Quality) :** `SODA Core` (Pushdown computation)
* **Validation Géospatiale :** `API Google Maps`
* **Orchestration & Calcul métier :** `Apache Airflow`
* **Restitution & BI :** `Microsoft Power BI`
* **Alerting :** `API Slack`
* **Infrastructure :** `Docker` & `Docker-Compose`


## ✨ Fonctionnalités Clés

* **Streaming & Alerting :** Capture des événements sportifs en temps réel avec Redpanda/Kafka et envoi 
d'une notification Slack instantanée pour les performances supérieures à 10 km.
* **Fenêtre Glissante (Rolling Window) :** Le calcul des KPI orchestré par Airflow agrège dynamiquement 
les activités des **12 derniers mois** pour valider l'éligibilité aux jours de congés, conformément aux 
règles métiers.
* **Validation des Déclarations RH :** Vérification automatisée des distances Domicile-Travail via
l'API Google Maps pour détecter les anomalies déclaratives. La règle stricte bloque tout trajet supérieur 
à 15 km pour la marche/course, et supérieur à 25 km pour le vélo/trottinette.
* **Idempotence & Truncate/Load :** Le pipeline sécurise les insertions en base de données (`TRUNCATE` de la table finale et `DELETE` des anomalies) pour éviter toute duplication en cas de relance du traitement.
* **Tests de Qualité Automatisés :** Validation des données entrantes avec SODA Core (règles YAML) pour bloquer les données aberrantes avant le calcul des primes.

## 📂 Structure du Projet

```text
📁 projet_12/
├── 📁 airflow/
│   └── 📁 dags/
│       └── dag_sport_rh.py                 # L'orchestrateur principal de l'ETL
├── 📁 kafka/
│   ├── producer.py                         # Génération des activités sportives (Mock Strava)
│   └── consumer_slack.py                   # Ingestion temps réel, filtrage et alerting
├── 📁 soda/
│   ├── configuration.yml                   # Connexion à PostgreSQL
│   └── checks.yml                          # Règles métier de qualité des données
├── 📁 powerbi/
│   └── dashboard_rh.pbix                   # Tableaux de bord de restitution financière et RH
├── docker-compose.yml                      # Déploiement de l'infrastructure
├── requirements.txt                        # Dépendances Python
├── excel.py                                # Script de génération du dictionnaire de données
└── Dictionnaire_Donnees_Sport_RH.xlsx      # Dictionnaire de données métier et technique

```

## 🚀 Installation et Démarrage

### Prérequis

* Docker et Docker-Compose installés.
* Python 3.9+
* Un token d'API Slack (pour l'alerting temps réel).
* Une clé d'API Google Maps (pour la validation des distances).

### Lancement de l'infrastructure

1. Cloner le dépôt et configurer le `.env` avec les clés API (Google Maps, Slack) et les accès BDD.

```bash
git clone [https://github.com/](https://github.com/)[votre_compte]/sport-data-solution.git
cd sport-data-solution

```

2. Démarrer les conteneurs Docker (Redpanda, PostgreSQL, Airflow) :

```bash
docker-compose up -d --build

```

### Exécution du flux de données

1. **Lancer le Consumer Kafka :**

```bash
python kafka/consumer_slack.py

```

2. **Générer des données (Producer) :**
Dans un autre terminal, simuler le flux d'activités sportives des salariés :

```bash
python kafka/producer.py

```

3. **Orchestration Batch :**
Se connecter à l'interface Airflow (`http://localhost:8080`), activer le `pipeline_sport_rh_v3_complet` et vérifier le passage des tests (Google Maps, SODA Core, KPI).

## 📖 Documentation et Dictionnaire de Données

Pour faciliter la compréhension des règles métiers (calcul des primes, éligibilité) et retracer la provenance des données, un dictionnaire de données complet accompagne ce projet (`Dictionnaire_Donnees_Sport_RH.xlsx`).

Si l'architecture de la base de données est amenée à évoluer, ce dictionnaire peut être mis à jour en exécutant le script utilitaire suivant :

```bash
python excel.py

```

## 📊 Restitution

Le fichier Power BI se connecte directement à la table finale `reporting_rh_final` de PostgreSQL. Il restitue une photographie (Snapshot) à l'instant T de l'éligibilité des salariés. Le taux de la prime sportive (5% par défaut) est géré dynamiquement via des paramètres DAX pour permettre des simulations budgétaires instantanées.
