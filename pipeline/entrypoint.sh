#!/bin/bash
set -e

export SPARK_JAVA_OPTS="-Xmx1g -Xms512m"

MASTER=yarn
# local[*] ou yarn
MODE=client

SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_OVERHEAD=512m
SPARK_DRIVER_MEMORY=2g
SPARK_DRIVER_OVERHEAD=512m

SPARK_EXECUTOR_CORES=1
SPARK_DRIVER_CORES=1
SPARK_EXECUTOR_INSTANCES=2

# Configuration HDFS et Hive
HDFS_DEFAULT_FS="hdfs://namenode:9000"
HIVE_METASTORE_URIS="thrift://metastore:9083"
SPARK_SQL_WAREHOUSE_DIR="hdfs://namenode:9000/user/hive/warehouse"
SPARK_JARS_PACKAGES="org.apache.spark:spark-avro_2.12:3.4.0,org.postgresql:postgresql:42.7.5"
SPARK_SQL_CASE_SENSITIVE="true"

JARS_PATH="/spark/jars"
JARS=$(find "$JARS_PATH" -name "*.jar" | paste -sd "," -)

echo "=== Configuration Spark optimisée ==="
echo "Ressources disponibles: ~5GB RAM, 6-7 coeurs CPU"
echo "Configuration:"
echo "  - Driver: ${SPARK_DRIVER_MEMORY} + ${SPARK_DRIVER_OVERHEAD} overhead"
echo "  - Executors: ${SPARK_EXECUTOR_INSTANCES}x (${SPARK_EXECUTOR_MEMORY} + ${SPARK_EXECUTOR_OVERHEAD} overhead)"
echo "  - CPU: ${SPARK_DRIVER_CORES} (driver) + ${SPARK_EXECUTOR_INSTANCES}x${SPARK_EXECUTOR_CORES} (executors) = $((SPARK_DRIVER_CORES + SPARK_EXECUTOR_INSTANCES * SPARK_EXECUTOR_CORES)) coeurs total"
echo "  - Mémoire totale estimée: ~4GB (dans les limites des 5GB disponibles)"

mkdir -p /app/log/writer

echo "Démarrage de $PY_FILE avec configuration optimisée"
/opt/spark/bin/spark-submit \
  --master ${MASTER} \
  --deploy-mode ${MODE} \
  --jars "$JARS" \
  --conf spark.ui.port=4040 \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" \
  --conf "spark.driver.memoryOverhead=${SPARK_DRIVER_OVERHEAD}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_OVERHEAD}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES}" \
  --conf "spark.driver.cores=${SPARK_DRIVER_CORES}" \
  \
  --conf "spark.hadoop.fs.defaultFS=${HDFS_DEFAULT_FS}" \
  --conf "spark.sql.catalogImplementation=hive" \
  --conf "hive.metastore.uris=${HIVE_METASTORE_URIS}" \
  --conf "spark.sql.warehouse.dir=${SPARK_SQL_WAREHOUSE_DIR}" \
  --conf "spark.sql.hive.metastore.jars=builtin" \
  --conf "spark.jars.packages=${SPARK_JARS_PACKAGES}" \
  --conf "spark.sql.caseSensitive=${SPARK_SQL_CASE_SENSITIVE}" \
  \
  --conf "spark.sql.shuffle.partitions=8" \
  --conf "spark.memory.fraction=0.6" \
  --conf "spark.memory.storageFraction=0.3" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  \
  "/app/application/application/$PY_FILE"   >> "/app/application/logs/$(basename $PY_FILE)_master_$MASTER.log" 2>&1

echo "=== Job terminé ==="
tail -f /dev/null