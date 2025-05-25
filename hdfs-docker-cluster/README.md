# hdfs-docker-cluster
## Démarer le conteneur 
docker compose up
## Interfaces graphiques disponibles : 
yarn : http://localhost:8088 (localhost à remplacer par ip de la machine host)
spark-master : http://localhost:8080
spark-worker : http://localhost:8081
hdfs (visualiser data lake - bronze) : http://localhost:9870
hue (visualiser data warehouse - silver) : http://localhost:8888
pgadmin (visualiser data mart - gold) : http://localhost:5050 localhost à remplacer par ip de la machine host)
## Créer le répertoire bronze
docker exec -it namenode bash
hdfs dfs -mkdir -p /bronze
## Créer le répertoire bronze
docker exec -it hive-metastore bash
/opt/hive/bin/hive
CREATE DATABASE silver;
