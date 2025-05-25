-- docker compose exec -it postgres bash
-- psql -h postgres -U hive -d metastore

DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO hive;
GRANT ALL ON SCHEMA public TO public;

# Crée le dossier warehouse avec les bons droits
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chown -R hive:hive /user/hive
hadoop fs -chmod 1777 /user/hive/warehouse

hdfs dfs -chown -R hive:hive /
hdfs fs -chmod 1777 /
hdfs dfs -mkdir /user/hive/warehouse
hdfs dfs -chown -R hive:hive /user/hive
hdfs fs -chmod 1777 /user/hive/warehouse


