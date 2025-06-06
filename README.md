# Big Data Framework

Ce projet propose une architecture modulaire pour construire une plateforme Big Data complète : ingestion, traitement, stockage, analyse et visualisation. Il repose sur un environnement Spark/Hadoop simulé avec Docker, et une orchestration de traitements via des scripts Python.

---

## 📁 Structure du projet

```bash
big_data_framework/
├── api/
├── datalake/
│   ├── datanode1/
│   └── namenode/
├── datamart/
├── datasource/
│   ├── dont_read/
│   └── to_db/
│       └── global_file/
├── dataviz/
│   └── transaction_dashboard.pbix
├── datawarehouse/
├── hdfs-docker-cluster/
├── pipeline/
│   ├── logs/
│   └── task/
│       └── utils/
├── .gitignore
├── explore.ipynb
├── requirements.txt
├── schema_infra.png
└── TP_explication.docx
```

## 🏗️ Architecture

Ce projet implémente une architecture Big Data complète comprenant :
- **Data Lake** : Stockage distribué avec HDFS
- **Data Warehouse** : Données consolidées pour l'analytique
- **Data Mart** : Vues métiers orientées utilisateur
- **Pipeline de traitement** : ETL/ELT avec Apache Spark
- **Visualisation** : Dashboards Power BI interactifs
- **API** : API Flask pour accéder aux données du DataMart

## 📂 Structure du projet

### `api/`
Scripts ou endpoints permettant d'exposer ou d'interagir avec les données via des APIs.

### `datalake/`
Contient la structure du lac de données simulé avec HDFS :
- `namenode/` : métadonnées HDFS
- `datanode1/` : stockage distribué

### `datamart/`
Stocke les données transformées, prêtes pour les analyses métiers (vue orientée utilisateur) - PostgreSQL.

### `datasource/`
Sources de données initiales :
- `dont_read/` : fichiers ignorés
- `to_db/` : fichiers destinés à l'injection dans PostgreSQL (simulation du db en source) > HDFS
  - `global_file/` : fichiers utilisés globalement pour l'intégration
- `./` : fichiers destinés à l'injestion dans hdfs

### `dataviz/`
Fichiers liés à la visualisation des données. Inclut un rapport Power BI (`transaction_dashboard.pbix`).

### `datawarehouse/`
Contient les données consolidées destinées à des traitements analytiques de plus haut niveau (HIVE)

### `hdfs-docker-cluster/`
Déploie l'infrastructure Big Data (Spark, Hadoop, Hive, YARN…) via Docker Compose.

### `pipeline/`
Logique métier et scripts de traitement (déploiement via docker compose) :
- `task/` : étapes de la pipeline (ingestion, transformation, export)
  - `utils/` : fonctions utilitaires et requêtes SQL
- `logs/` : stockage des fichiers de log, montés depuis les conteneurs Spark

## ⚙️ Installation et démarrage

### Prérequis
- Python 3.8+
- Docker et Docker Compose
- Git

### Mise en route rapide

```bash
# Cloner le repository
git clone https://github.com/JulienDira/big_data_framework.git
cd big_data_framework

# Configurer l'environnement Python
python -m venv .venv
source .venv/bin/activate  # ou .venv\Scripts\activate sous Windows
pip install -r requirements.txt

# Déployer le cluster Big Data
cd hdfs-docker-cluster
docker-compose up -d

# Déployer la pipeline
cd pipeline
docker-compose up <app_name> -d
```

### Vérification du déploiement

Une fois le cluster démarré, vous pouvez accéder aux interfaces web :
- **Hadoop NameNode** : http://localhost:9870
- **YARN ResourceManager** : http://localhost:8088
- **Spark History Server** : http://localhost:8080

## 🔄 Pipeline de traitement

### Exécution Spark
Les scripts sont exécutés via `spark-submit`. Les logs sont redirigés vers le dossier `pipeline/logs` (visible depuis la machine hôte grâce au volume Docker).

Le traitement peut aussi être testé sur un cluster YARN (mode client) :

```bash
# Exemple d'exécution Spark
spark-submit --master yarn --deploy-mode client pipeline/task/your_script.py
```

(script dans /pipeline/entrypoint.sh)

### Monitoring
- Les logs d'exécution sont disponibles dans `pipeline/logs/`
- Un script de validation des partitions Spark est intégré pour garantir la cohérence des données
- Les transformations sont centralisées dans des fonctions Python et des requêtes SQL isolées

## 📊 Visualisation et analyse

### Power BI Dashboard
Les données finales sont chargées dans Power BI via le fichier `dataviz/transaction_dashboard.pbix`.

**Fonctionnalités du dashboard :**
- Vue de synthèse des KPIs principaux
- Tableau self-service pour l'exploration ponctuelle
- Mesures DAX avancées (filtrage dynamique, titres conditionnels)
- Analyses temporelles et géographiques

## 🛠️ Technologies utilisées

- **Big Data** : Apache Hadoop, Apache Spark, Apache Hive
- **Stockage** : HDFS, PostgreSQL
- **Orchestration** : Docker, Docker Compose, YARN
- **Développement** : Python, SQL, PySpark
- **Visualisation** : Power BI

## 📋 Fichiers de référence

- `schema_infra.png` : schéma d'architecture du projet
- `requirements.txt` : dépendances Python du projet
