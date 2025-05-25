#!/bin/bash

echo "Initializing Hive warehouse directory in HDFS..."

# Vérifier si le répertoire Hive existe déjà dans HDFS
if hdfs dfs -test -d /user/hive/warehouse; then
  echo "Hive warehouse directory already exists in HDFS."
else
  echo "Creating Hive warehouse directory in HDFS..."
  hdfs dfs -mkdir -p /user/hive/warehouse
  hdfs dfs -chmod -R 777 /user/hive/warehouse
  echo "Hive warehouse directory initialized successfully."
fi
