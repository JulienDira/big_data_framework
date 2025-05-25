#!/bin/bash

# Vérification et définition de l'URL du maître
if [ -z "$SPARK_MASTER_URL" ]; then
    echo "ERREUR : La variable SPARK_MASTER_URL n'est pas définie. Exemple attendu : spark://spark-master:7077"
    exit 1
fi

# Démarrage du Spark Master
if [ "$SPARK_MODE" == "master" ]; then
    echo "Démarrage du Spark Master sur $SPARK_MASTER_URL"
    /opt/bitnami/spark/sbin/start-master.sh
fi

# Démarrage du Spark Worker
if [ "$SPARK_MODE" == "worker" ]; then
    echo "Démarrage du Spark Worker, connexion au Master : $SPARK_MASTER_URL"
    /opt/bitnami/spark/sbin/start-worker.sh "$SPARK_MASTER_URL"
fi
