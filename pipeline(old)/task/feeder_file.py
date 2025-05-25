from utils.define_repartition import get_file_size_bytes, calculate_partitions
from utils.schema_and_transformations import get_spark_schema
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

    def read_source_data(self, full_table_path, schem):
        """
        Lecture des données source depuis le système de fichiers sans schéma explicite.
        Ajoute une colonne 'ingestion_date' avec le timestamp actuel.
        Retourne un DataFrame Spark.
        """
        try:
            if full_table_path.endswith(".csv"):
                self.logger.info(f"Lecture du CSV depuis : {full_table_path}")
                
                ingestion_time = date_sub(current_timestamp(), 0)
                return (
                    self.spark.read
                    .schema(schem)
                    .option("header", True)
                    .option("inferSchema", True)
                    .csv(full_table_path)
                    .withColumn("ingestion_date", ingestion_time)
                    # .withColumn("year", year(ingestion_time))
                    # .withColumn("month", month(ingestion_time))
                    # .withColumn("day", dayofmonth(ingestion_time))
                )

            elif full_table_path.endswith(".json"):
                file_size_bytes = get_file_size_bytes(full_table_path)
                n_partition = calculate_partitions(file_size_bytes, target_partition_size_mb=40)
                
                self.logger.info(f"Lecture du JSON depuis : {full_table_path}")
                df = (
                    self.spark
                    .read
                    .schema(schem)                # <-- tu peux fournir un schema explicite ici
                    .option("multiLine", True)
                    .json(full_table_path)
                    .repartition(n_partition)
                    .withColumn("ingestion_date", current_timestamp())
                )
                
                self.logger.info(f"Données lues avec succès depuis : {full_table_path}")
                
                df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                
                return df
            else:
                raise ValueError("Format de fichier non supporté (CSV ou JSON attendu).")

        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture des données sources : {e}")
            raise RuntimeError(f"Erreur lors de la lecture des données sources : {e}")

    def write_to_parquet(self, df, full_table_path):
        """
        Écrit les données dans le catalogue Hive (BRONZE) en format Parquet,
        partitionnées par année, mois, jour à partir d'une colonne timestamp.
        """

        try:
            # Écriture en Parquet dans Hive avec partitions
            (
                df
                .write
                .mode("overwrite") # or append
                .format("parquet")
                # .partitionBy("year", "month", "day")
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
            datasource = self.local_path
            files = [f for f in os.listdir(datasource) if os.path.isfile(os.path.join(datasource, f))]

            if not files:
                self.logger.warning("Aucun fichier trouvé dans le dossier source.")
                return

            for file_name in files:
                source_file_path = f"file://{os.path.join(datasource, file_name)}"
                # source_file_path = f"{datasource}{file_name}"
                table_name = os.path.splitext(file_name)[0]

                table_schem = get_spark_schema(table_name)
                target_table_path = f"{self.raw_path}{table_name}"
                
                self.logger.info(f"\n--- Traitement du fichier : {source_file_path} ---")

                raw_data = self.read_source_data(source_file_path, table_schem)

                self.write_to_parquet(raw_data, target_table_path)

                self.logger.info(f"✔ Pipeline exécutée avec succès pour '{file_name}'.")

        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
            
        finally:
            self.logger.info("🛑 Fermeture de la session Spark.")
            self.spark.stop()

if __name__ == "__main__":

    BronzeStep().run_pipeline()
