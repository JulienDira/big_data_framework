from utils.define_repartition import get_file_size_bytes, calculate_partitions
from utils.create_postgres_table import create_table_if_not_exists
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp, current_timestamp
import os
import logging

class SourceToDb:
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

    def read_source_data(self, full_table_path):
        """
        Lecture des données source depuis le système de fichiers sans schéma explicite.
        Ajoute une colonne 'ingestion_date' avec le timestamp actuel.
        Retourne un DataFrame Spark.
        """
        try:
            self.logger.info(f"Lecture du CSV depuis : {full_table_path}")
            return (
                self.spark
                .read
                .option("header", True)
                .option("inferSchema", True)
                .csv(full_table_path)
            )
        
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture des données sources : {e}")
            raise RuntimeError(f"Erreur lors de la lecture des données sources : {e}")

    def write_to_postgresql(self, df, table_name):
        """
        Écrit les données dans une table PostgreSQL via JDBC en mode append.
        Remplace la connexion avec les variables d'environnement ou ta config.
        """
        try:
            jdbc_url = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/source_data")
            jdbc_user = os.getenv("POSTGRES_USER", "hive")
            jdbc_password = os.getenv("POSTGRES_PASSWORD", "hive")
            jdbc_driver = "org.postgresql.Driver"
            
            create_table_if_not_exists(df, table_name, jdbc_url, jdbc_user, jdbc_password, host="postgres", port=5432)

            self.logger.info(f"Début de l'écriture de la table PostgreSQL '{table_name}'.")
            
            (
                df.write
                .format("jdbc")
                .mode("overwrite")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .option("driver", jdbc_driver)
                .save()
            )

            self.logger.info(f"Données ajoutées à la table PostgreSQL '{table_name}'.")
        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture vers PostgreSQL : {e}")
            raise RuntimeError(f"Erreur lors de l'écriture vers PostgreSQL : {e}")


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
                self.logger.info(f"\n--- Traitement du fichier : {source_file_path} ---")

                raw_data = self.read_source_data(source_file_path)

                # Utilise le nom de fichier sans extension comme nom de table Hive
                table_name = os.path.splitext(file_name)[0]
                
                table_name = 'transactions_data' if table_name.startswith('transactions_data') else table_name
                
                self.write_to_postgresql(raw_data, table_name)

                self.logger.info(f"✔ Pipeline exécutée avec succès pour '{file_name}'.")

        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
            
        finally:
            self.logger.info("🛑 Fermeture de la session Spark.")
            self.spark.stop()

if __name__ == "__main__":
    SourceToDb().run_pipeline()
