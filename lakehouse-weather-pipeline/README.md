
# 🌦️ Lakehouse Pipeline Météo — Kafka → MinIO → PostgreSQL → dbt → Grafana

Pipeline de données de bout en bout (architecture **Lakehouse** : Bronze → Silver → Gold) alimenté par l'API météo **Open-Meteo**, orchestré par **Apache Airflow**, avec visualisation dans **Grafana**.

---

## 📌 Architecture

```
┌─────────────┐     ┌─────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐     ┌─────────┐
│  Open-Meteo │────▶│  Kafka  │────▶│ MinIO Bronze │────▶│ MinIO Silver │────▶│ PostgreSQL │────▶│ Grafana │
│  (API)      │     │ (topic) │     │  (Parquet)   │     │  (Parquet)   │     │  (analytics)│     │(dashboard)│
└─────────────┘     └─────────┘     └──────────────┘     └──────────────┘     └────────────┘     └─────────┘
                                                                                    │
                                                                                    ▼
                                                                              ┌──────────┐
                                                                              │   dbt    │
                                                                              │gold_meteo│
                                                                              └──────────┘
```

**Orchestration** : Apache Airflow (DAG `lakehouse_pipeline`)

---

## 🛠️ Stack technique

| Outil | Rôle | Couche |
|---|---|---|
| **Open-Meteo** | API météo gratuite (sans clé) | Source |
| **Kafka** | File d'attente fiable entre production et consommation | Ingestion |
| **MinIO** | Stockage objet S3-compatible (données brutes + nettoyées) | Bronze + Silver |
| **PostgreSQL** | Entrepôt de données relationnel | Silver + Gold |
| **dbt** | Transformation SQL et modélisation métier | Gold |
| **Grafana** | Visualisation et dashboards | Restitution |
| **Airflow** | Orchestration du pipeline | Orchestration |
| **Docker** | Conteneurisation de tous les services | Infrastructure |

---

## 📂 Structure du projet

```
lakehouse-weather-pipeline/
├── docker-compose.yml
├── init.sql                    # ← AJOUT (création auto de la base analytics)
├── requirements.txt            # ← AJOUT
├── .gitignore                  # ← AJOUT
├── producer.py
├── consumer_to_bronze.py
├── silver_to_minio.py
├── load_postgres.py
├── dags/
│   └── pipeline_dag.py
│   models/
│   └── gold_events.sql
├── gold_project/
│   ├── dbt_project.yml
│   ├── profiles.yml            # ← dans gold_project/
└── README.md
```

---

## 🚀 Lancement du projet

### 1. Prérequis

- Docker Desktop
- Python 3.12 (avec `venv`)
- Les bibliothèques Python : `kafka-python`, `minio`, `pandas`, `pyarrow`, `sqlalchemy`, `psycopg2-binary`, `requests`, `dbt-postgres`

### 2. Démarrer les services

```bash
docker compose up -d
```

Vérifier que tout tourne :

```bash
docker ps
```

Les conteneurs doivent être en `Up` : `minio`, `kafka-lakehouse`, `postgres`, `airflow-webserver`, `airflow-scheduler`,`airflow-init (terminé)`,`grafana`.

### 3. Lancer le pipeline manuellement (Windows)

```bash
# 1. Récupérer les données météo depuis Open-Meteo → Kafka
python producer.py

# 2. Lire Kafka → MinIO Bronze
python consumer_to_bronze.py

# 3. Nettoyer → MinIO Silver
python silver_to_minio.py

# 4. Charger → PostgreSQL
python load_postgres.py

# 5. Créer la vue Gold avec dbt
dbt run --profiles-dir .
```

### 4. Lancer le pipeline via Airflow (automatisé)

Ouvrir `http://localhost:8080` (admin / admin), activer le DAG `lakehouse_pipeline`, puis cliquer sur **Trigger DAG**.

Le DAG exécute automatiquement les 5 étapes dans l'ordre : `produce` → `consume` → `silver` → `load` → `dbt`.

---

## 🌐 Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **MinIO Console** | http://localhost:9003 | minioadmin / minioadmin123 |
| **Kafka** | localhost:9092 | — |

---

## 📊 Visualisation dans Grafana

1. Ouvrir `http://localhost:3000` (admin / admin)
2. Ajouter une source de données **PostgreSQL** :
   - Host : `postgres:5432`
   - Database : `analytics`
   - User : `airflow`
   - Password : `airflow`
   - SSL mode : `disable`
3. Créer un panel avec la requête :
   ```sql
   SELECT ville, temp_moyenne, humidite_moyenne, vent_max FROM gold_meteo;
   ```

Résultat attendu : un graphique montrant la température moyenne par ville (Paris, Lyon, Marseille).

---

## 🔄 Le flux de données, étape par étape

1. **`producer.py`** interroge l'API **Open-Meteo** pour plusieurs villes (Paris, Lyon, Marseille) et envoie les données météo (température, humidité, vent) dans le topic Kafka `events`.
2. **`consumer_to_bronze.py`** lit Kafka et écrit les données brutes en Parquet dans `bronze/events/`.
3. **`silver_to_minio.py`** lit Bronze, nettoie (dédoublonnage) et écrit dans `silver/events/`.
4. **`load_postgres.py`** lit Silver et charge les données dans PostgreSQL (`events_clean`).
5. **`dbt run`** transforme `events_clean` en vue `gold_meteo` (agrégats métier : température moyenne par ville, etc.).
6. **Grafana** affiche `gold_meteo` dans un dashboard.

---

## 🌡️ Données météo (Open-Meteo)

L'API Open-Meteo est **gratuite et ne nécessite aucune clé**. Le producteur récupère :

- **Température** (`temperature_2m`)
- **Humidité relative** (`relative_humidity_2m`)
- **Vitesse du vent** (`wind_speed_10m`)

Pour plusieurs villes : Paris, Lyon, Marseille.

Exemple de données envoyées dans Kafka :

```json
{
  "id": 5387350821178740268,
  "ville": "Paris",
  "temperature": 13.1,
  "humidite": 60,
  "vent": 18.8,
  "timestamp": 1792598441.35
}
```

---

## 🎯 Compétences démontrées

- **Data Engineering** : pipeline ETL/ELT complet, ingestion, stockage, transformation
- **Big Data** : Kafka, MinIO (S3-compatible), Parquet
- **Orchestration** : Airflow (DAG, dépendances, retries)
- **Modélisation** : dbt (SQL versionné, tests, documentation)
- **Visualisation** : Grafana
- **Infrastructure** : Docker Compose (réseau, volumes, variables d'environnement)
- **API** : consommation d'une API REST (Open-Meteo)
- **Résolution de problèmes** : configuration réseau Docker (listeners Kafka, DNS entre conteneurs), compatibilité de versions (pandas, SQLAlchemy, dbt)

---

## ⚠️ Difficultés rencontrées et solutions

| Difficulté | Solution |
|---|---|
| **Kafka inaccessible depuis Airflow** | Configuration de deux listeners (`PLAINTEXT://localhost:9092` pour Windows, `INTERNAL://kafka:29092` pour Docker) + ajout de Kafka au réseau `lakehouse-net` |
| **MinIO inaccessible depuis Airflow** | Ajout de MinIO au réseau `lakehouse-net` (nom de service résolu par Docker) |
| **dbt ne trouvait pas `profiles.yml`** | Montage du fichier dans le conteneur via un volume Docker |
| **Conflit de port PostgreSQL** | Changement du port exposé (`5434` au lieu de `5432`, déjà occupé par PostgreSQL local) |
| **`if_exists='replace'` échouait** | Suppression de la vue `gold_meteo` avant de recréer la table (`DROP VIEW CASCADE`) |
| **`delete_rows` non supporté dans Airflow** | Utilisation de `DELETE FROM` + `append` (compatible avec toutes les versions de pandas) |
| **`conn.commit()` non supporté dans SQLAlchemy 1.4** | Utilisation de `engine.begin()` (transaction automatique) |
| **Kafka accumulait des milliers de messages** | Suppression du volume Kafka (`kafka_kafka-data`) et recréation du topic `events` |


## 📝 Licence

Projet personnel — libre d'utilisation.
