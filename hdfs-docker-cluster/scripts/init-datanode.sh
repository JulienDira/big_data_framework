#!/bin/bash

# Vérifier si le DataNode est déjà formaté
if [ ! -f /opt/hadoop/data/dataNode/current/VERSION ]; then
  echo "===> Formatting DataNode..."
  hdfs datanode -format
fi

echo "===> Starting HDFS DataNode..."
hdfs --daemon start datanode

echo "===> Starting YARN NodeManager..."
yarn --daemon start nodemanager

# Empêche le conteneur de s'arrêter immédiatement :
tail -f /dev/null
