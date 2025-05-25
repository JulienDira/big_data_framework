from utils.define_repartition import get_file_size_bytes, calculate_partitions
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp, current_timestamp, date_sub
import os
import logging

class BronzeStep:
    def __init__(self):
        
        self.local_path  = os.getenv('DATA_SOURCE_PATH')
        self.raw_path = os.getenv('RAW_PATH')
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialisation de la session Spark avec support Hive
        self.spark = SparkSession.builder \
            .appName("Feeder_Source_to_HDFS") \
            .enableHiveSupport() \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("✅ SparkSession initialisée avec succès.")

    def read_source_data(self, table_name, jdbc_url, db_properties):
        """
        Lecture des données source depuis PostgreSQL via JDBC.
        Ajoute une colonne 'ingestion_date' avec le timestamp actuel.
        Retourne un DataFrame Spark.
        
        :param table_name: Nom de la table PostgreSQL à lire (ex: "public.users")
        :param jdbc_url: URL JDBC PostgreSQL (ex: "jdbc:postgresql://host:port/dbname")
        :param db_properties: Dictionnaire avec les propriétés de connexion : user, password, driver
        :param n_partition: Nombre de partitions Spark pour paralléliser la lecture
        """
        try:
            self.logger.info(f"Lecture des données JDBC depuis : {table_name}")
            ingestion_time = date_sub(current_timestamp(), 2)
            df = (
                self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"public.{table_name}")
                .option("user", db_properties["user"])
                .option("password", db_properties["password"])
                .option("driver", db_properties.get("driver", "org.postgresql.Driver"))
                .load()
                .coalesce(10)
                .withColumn("ingestion_date", ingestion_time)
                .withColumn("year", year(ingestion_time))
                .withColumn("month", month(ingestion_time))
                .withColumn("day", dayofmonth(ingestion_time))
            )

            
            self.logger.info(f"Données lues avec succès depuis : {table_name}")
            return df

        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture JDBC : {e}")
            raise RuntimeError(f"Erreur lors de la lecture JDBC : {e}")

    def write_to_parquet(self, df, full_table_path):
        """
        Écrit les données dans le catalogue Hive (BRONZE) en format Parquet,
        partitionnées par année, mois, jour à partir d'une colonne timestamp.
        """

        try:
            self.logger.info(f"Ecriture des données vers HDFS en cours > '{full_table_path}'")
            # Écriture en Parquet dans Hive avec partitions
            (
                df
                .write
                .mode("append")
                .format("parquet")
                .partitionBy("year", "month", "day")
                .save(full_table_path)
            )

            self.logger.info(f"Fichier Parquet écrit dans HDFS à : '{full_table_path}'")
        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture vers Parquet dans HDFS : {e}")
            raise RuntimeError(f"Erreur lors de l'écriture vers Parquet dans HDFS : {e}")

    def run_pipeline(self):
        """
        Exécute la pipeline complète pour transformer les fichiers dans un dossier local.

        Args:
            crypto_name (str): Nom de la cryptomonnaie (peut être utilisé comme nom de table).
        """
        try:
            jdbc_url = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/source_data")

            db_properties = {
                "user": os.getenv("POSTGRES_USER", "hive"),
                "password": os.getenv("POSTGRES_PASSWORD", "hive"),
                "driver": "org.postgresql.Driver"
            }
            
            table_name = "transactions_data"

            self.logger.info(f"\n--- Traitement de la table : {table_name} ---")

            jdbc_table = self.read_source_data(table_name, jdbc_url, db_properties)
            jdbc_table.cache()
            
            target_table_path = f"{self.raw_path}{table_name}"
            
            self.write_to_parquet(jdbc_table, target_table_path)

            self.logger.info(f"✔ Pipeline exécutée avec succès pour '{table_name}'.")

        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
            
        finally:
            self.logger.info("🛑 Fermeture de la session Spark.")
            self.spark.stop()

if __name__ == "__main__":
    BronzeStep().run_pipeline()
