#!/bin/bash
set -e

export SPARK_JAVA_OPTS="-Xmx3g -Xms1g"

MASTER=local[*]
MODE='client'
# master 'local[*]' ou 'yarn' 

SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_CORES=4
SPARK_EXECUTOR_INSTANCES=1
SPARK_DRIVER_CORES=1

HDFS_DEFAULT_FS="hdfs://namenode:9000"
HIVE_METASTORE_URIS="thrift://metastore:9083"
SPARK_SQL_WAREHOUSE_DIR="hdfs://namenode:9000/user/hive/warehouse"
SPARK_JARS_PACKAGES="org.apache.spark:spark-avro_2.12:3.4.0,org.postgresql:postgresql:42.7.5"
SPARK_SQL_CASE_SENSITIVE="true"

JARS_PATH="/spark/jars"
JARS=$(find "$JARS_PATH" -name "*.jar" | paste -sd "," -)

echo "=== Configuration de l'environnement Spark ==="
echo "Fichier Python $PY_FILE"
echo "JARs: $JARS"

mkdir -p /app/log/writer

echo "Démarrage de $PY_FILE"
/opt/spark/bin/spark-submit \
  --master ${MASTER} \
  --deploy-mode ${MODE} \
  --jars "$JARS" \
  --conf spark.ui.port=4040 \
  --conf "spark.driver.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.executor.extraClassPath=${JARS_PATH}/*" \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES}" \
  --conf "spark.driver.cores=${SPARK_DRIVER_CORES}" \
  --conf "spark.hadoop.fs.defaultFS=${HDFS_DEFAULT_FS}" \
  --conf "spark.sql.catalogImplementation=hive" \
  --conf "hive.metastore.uris=${HIVE_METASTORE_URIS}" \
  --conf "spark.sql.warehouse.dir=${SPARK_SQL_WAREHOUSE_DIR}" \
  --conf "spark.sql.hive.metastore.jars=builtin" \
  --conf "spark.jars.packages=${SPARK_JARS_PACKAGES}" \
  --conf "spark.sql.caseSensitive=${SPARK_SQL_CASE_SENSITIVE}" \
  --conf "spark.sql.shuffle.partitions=50" \
  --conf "spark.memory.fraction=0.5" \
  --conf "spark.memory.storageFraction=0.3" \
  "/app/application/application/$PY_FILE"  >> "/app/application/logs/$(basename $PY_FILE).log" 2>&1

echo "=== Job terminé (ou en streaming continu) ==="

tail -f /dev/null

