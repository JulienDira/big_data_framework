from utils.define_repartition import get_file_size_bytes, calculate_partitions
from pyspark.sql.types import FloatType
from collections import Counter
from pyspark.sql.functions import (
    to_timestamp,regexp_replace, col, broadcast, current_timestamp, date_sub,
    year, month, dayofmonth
)
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import os
import logging

class SparkPipeline:
    
    def __init__(self, app_name):
        
        self.local_path  = os.getenv('DATA_SOURCE_PATH')
        self.raw_path = os.getenv('RAW_PATH')
        self.warehouse_path = os.getenv('WAREHOUSE_PATH')
        self.warehouse_db_name = os.getenv('WAREHOUSE_DB')
        self.warehouse_db_path = os.getenv('WAREHOUSE_DB_PATH')
        self.postgres_jdbc_url = os.getenv('POSTGRES_JDBC_URL')
        self.jdbc_password = os.getenv('JDBC_PASSWORD')
        self.jdbc_user = os.getenv('JDBC_USER')
        self.db_properties = {
            "user": self.jdbc_user,
            "password": self.jdbc_password,
            "driver": "org.postgresql.Driver"
        }
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialisation de la session Spark avec support Hive
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")
        self.logger.info("✅ SparkSession initialisée avec succès.")
        
    def read_csv_data_with_schema(self, full_table_path, schema)-> DataFrame:
        """
        Lecture des données depuis le file system avec un schéma explicite.
        """
        try:
            self.logger.info(f"Lecture du CSV depuis : {full_table_path}")         
            df = (
                self.spark.read
                .schema(schema)
                .option("header", True)
                .option("inferSchema", True)
                .csv(full_table_path)
            )   
            return df
        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture des données : {e}")
        
    def read_csv_data_without_schema(self, full_table_path)-> DataFrame:
        """
        Lecture des données depuis le file system avec un schéma explicite.
        """
        try:
            self.logger.info(f"Lecture du CSV depuis : {full_table_path}")         
            df = (
                self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(full_table_path)
            )   
            return df
        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture des données : {e}")

    def read_json_data_with_schema(self, full_table_path, schema)-> DataFrame:
        """
        Lecture des données depuis le file system avec un schéma explicite.
        """
        try:
            file_size_bytes = get_file_size_bytes(full_table_path, self.spark)
            n_partition = calculate_partitions(file_size_bytes, target_partition_size_mb=40)
            
            self.logger.info(f"Lecture du JSON depuis : {full_table_path}")
            df = (
                self.spark
                .read
                .schema(schema)                # <-- tu peux fournir un schema explicite ici
                .option("multiLine", True)
                .json(full_table_path)
                .repartition(n_partition)
            )
            self.logger.info(f"Données lues avec succès depuis : {full_table_path}")
            return df
        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture des données : {e}")

    def read_parquet_data_without_schema(self, table_name, partitioned=False) -> DataFrame:
        """
        Lecture des données brutes depuis HDFS avec un schéma explicite.
        """
        try:
            data_path = f"{self.raw_path}{table_name}"
            if partitioned == True:
                data_path = self._data_path_latest_partition(table_name)
            df = self.spark.read.format("parquet").load(data_path)

            return df
        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture des données brutes : {e}")
        
    def read_parquet_data_with_schema(self, table_name, schema, partitioned=False) -> DataFrame:
        """
        Lecture des données brutes depuis HDFS avec un schéma explicite.
        """
        try:
            data_path = f"{self.raw_path}{table_name}"
            if partitioned == True:
                data_path = self._data_path_latest_partition(table_name)
            df = self.spark.read.format("parquet").schema(schema).load(data_path)

            return df
        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture des données brutes : {e}")

    def read_jdbc_data(
        self,
        table_name: str,
        jdbc_url: str,
        db_properties: str
    ):
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
            )
                 
            self.logger.info(f"Données lues avec succès depuis : {table_name}")
            return df
        except Exception as e:
            self.logger.error(f"Erreur lecture JDBC depuis {table_name} : {e}")
            raise RuntimeError(f"Erreur lecture JDBC depuis {table_name} : {e}")

    def read_from_hive_with_sql(self, sql_query: str) -> DataFrame:
        """
        Exécute une requête SQL sur la base Hive et retourne un DataFrame Spark.
        
        Args:
            sql_query (str): Requête SQL valide (ex: "SELECT * FROM db_name.table_name WHERE ...")
        
        Returns:
            DataFrame: Résultat de la requête SQL sous forme de DataFrame Spark.
        """
        try:
            # self.spark.catalog.setCurrentDatabase("process_data")

            self.logger.info("=== DIAGNOSTIC HIVE ===")

            databases = self.spark.sql("SHOW DATABASES")
            self.logger.info("Bases disponibles :")
            self.logger.info(databases.collect())  # Collect pour affichage log

            warehouse_location = self.spark.sql("SET hive.metastore.warehouse.dir")
            self.logger.info("Localisation du warehouse :")
            self.logger.info(warehouse_location.collect())  # Collect pour affichage log

            self.logger.info(f"🔍 Exécution de la requête Hive : {sql_query}")
            df = self.spark.sql(sql_query)

            self.logger.info("Aperçu du résultat de la requête :")
            df.limit(2).show()

            self.logger.info("✅ Requête exécutée avec succès.")
            return df
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la lecture depuis Hive avec SQL : {e}")
            raise RuntimeError(f"Erreur lors de la lecture depuis Hive avec SQL : {e}")

    def _data_path_latest_partition(self, table_name: str) :
        """
        Lecture optimisée des données Parquet partitionnées par year/month/day.
        Seule la dernière partition (date la plus récente) est lue.
        """
        try:
            raw_data_path = f"{self.raw_path}{table_name}"

            # Lecture des partitions disponibles (lecture légère du schema uniquement)
            df = self.spark.read.format("parquet").load(raw_data_path)

            # Extraire la dernière date partitionnée
            latest_partition = (
                df.select("year", "month", "day")
                .distinct()
                .withColumn("date", F.expr("make_date(year, month, day)"))
                .orderBy(F.col("date").desc())
                .limit(1)
                .collect()[0]
            )

            year = latest_partition["year"]
            month = latest_partition["month"]
            day = latest_partition["day"]

            self.logger.info(f"Lecture de la dernière partition : {year}-{month:02d}-{day:02d}")

            # Chargement ciblé de la dernière partition
            latest_path = os.path.join(raw_data_path, f"year={year}/month={month}/day={day}")

            return latest_path

        except Exception as e:
            raise RuntimeError(f"Erreur lors de la lecture de la dernière partition : {e}")
      
    def create_hive_database(self):
        
        self.spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {self.warehouse_db_name}
            LOCATION '{self.warehouse_db_path}'
        """)
        
    def drop_table_if_exists(self, table_name):
        self.spark.sql(f"DROP TABLE IF EXISTS {self.warehouse_db_name}.{table_name}")  

    def create_hive_table(self, table_name, schema):
        """
        Crée une table Hive partitionnée si elle n'existe pas.

        Args:
            table_name (str): Nom de la table.
            schema (str): Schéma Hive sous forme de chaîne (ex: "id INT, name STRING").
        """
        try:
            full_table_name = f"{self.warehouse_db_name}.{table_name}"
            table_location = f"{self.warehouse_db_path}{table_name}"

            # Utilisation de CREATE TABLE IF NOT EXISTS avec LOCATION
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {schema}
                )
                PARTITIONED BY (ingestion_date TIMESTAMP)
                STORED AS PARQUET
                LOCATION '{table_location}'
            """)
            self.logger.info(f"✅ Table Hive '{full_table_name}' vérifiée ou créée avec succès à l'emplacement : {table_location}")
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la création de la table Hive '{table_name}': {e}")
    
    def write_to_hive(self, df, table_name):
        """
        Écrit les données dans Hive en créant une vraie table queryable.
        """
        try:
            df.write \
            .mode("overwrite") \
            .option("path", f"{self.warehouse_db_path}{table_name}") \
            .saveAsTable(f"process_data.{table_name}")
            
            self.logger.info(f"✅ Table Hive créée avec succès : process_data.{table_name}")
            
        except Exception as e:
            raise RuntimeError(f"❌ Erreur lors de l'écriture de la table Hive : {e}")
        
    def write_to_postgresql(self, dataframe, table_name, write_mode: str='overwrite'):
        """
        Écriture des données enrichies dans une table PostgreSQL spécifique.
        """
        try:
            self.logger.info(f"Écriture des données dans la table : {table_name}")
            (
                dataframe
                .write.format("jdbc")
                .option("url", f"{self.postgres_jdbc_url}")
                .option("dbtable", table_name)
                .option("user", self.jdbc_user)
                .option("password", self.jdbc_password)
                .option("driver", "org.postgresql.Driver")
                .mode(write_mode) \
                .save()
            )
            self.logger.info(f"Données écrites avec succès dans la table {table_name}.")
        except Exception as e:
            raise RuntimeError(f"Erreur lors de l'écriture des données dans Gold : {e}")

    def write_to_parquet(
        self,
        df,
        full_table_path: str,
        write_mode: str='overwrite',
        partitioned_columns: List = None
    ):
        """
        Écrit les données dans le catalogue Hive (BRONZE) en format Parquet,
        partitionnées si une liste de colonnes est fournie.
        """
        try:
            self.logger.info(f"Ecriture des données vers HDFS en cours > '{full_table_path}'")

            writer = df.write.mode(write_mode).format("parquet")

            if partitioned_columns is None:
                writer.save(full_table_path)
            else:
                writer.partitionBy(partitioned_columns).save(full_table_path)

            self.logger.info(f"Fichier Parquet écrit dans HDFS à : '{full_table_path}'")

        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture vers Parquet dans HDFS : {e}")
            raise RuntimeError(f"Erreur lors de l'écriture vers Parquet dans HDFS : {e}")

class Transformation(SparkPipeline):
    def __init__(self, app_name):
        super().__init__(app_name)
        self.logger.info("✅ Classe Transformation initialisée avec succès.")
        
        self.DfMCC = self.read_parquet_data_without_schema(f"mcc_codes")
        self.dfClient = self.read_parquet_data_without_schema(f"users_data")
        self.dfCard = self.read_parquet_data_without_schema(f"cards_data")
        self.dfFraud = self.read_parquet_data_without_schema(f"train_fraud_labels")
        
    def CleaningTransactionsData(self, transactions_data: DataFrame) -> DataFrame:
        """Applique les transformations nécessaires à la table des transactions"""
        try:
            self.logger.info("🔄 Début du cleaning des transactions...")

            df = transactions_data.withColumn("date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
            df = df.withColumn("mcc", col("mcc").cast("string"))
            df = df.withColumn("client_id", col("client_id").cast("string"))

            money_cols = ["amount"]
            for col_name in money_cols:
                df = df.withColumn(col_name, regexp_replace(col(col_name), "[$]", "").cast(FloatType()))

            self.logger.info("✅ Cleaning terminé avec succès.")
            return df

        except Exception as e:
            self.logger.error(f"❌ Erreur lors du prétraitement des transactions : {e}")
            raise
        
    def CleaningCardsData(self, cards_data: DataFrame) -> DataFrame:
        """Applique les transformations nécessaires à la table des transactions"""
        try:
            self.logger.info("🔄 Début du cleaning des cards...")

            money_cols = ["credit_limit"]
            df = cards_data

            for col_name in money_cols:
                df = df.withColumn(
                    col_name,
                    regexp_replace(col(col_name), "\\$", "").cast(FloatType())
                )

            self.logger.info("✅ Cleaning terminé avec succès.")
            return df

        except Exception as e:
            self.logger.error(f"❌ Erreur lors du prétraitement des transactions : {e}")
            raise
        
    def CleaningUsersData(self, users_data: DataFrame) -> DataFrame:
        """Applique les transformations nécessaires à la table des transactions"""
        try:
            self.logger.info("🔄 Début du cleaning des users...")

            money_cols = ["per_capita_income", "yearly_income", "total_debt"]
            df = users_data

            for col_name in money_cols:
                df = df.withColumn(
                    col_name,
                    regexp_replace(col(col_name), "\\$", "").cast(FloatType())
                )

            self.logger.info("✅ Cleaning terminé avec succès.")
            return df

        except Exception as e:
            self.logger.info(f"❌ Erreur lors du prétraitement des transactions : {e}")
            raise
         
    def PreProcessingTransaction(self, transactions_data_cleaned: DataFrame) -> DataFrame:
        """Applique les transformations nécessaires à la table des transactions"""
        try:
            self.logger.info("🔄 Début du prétraitement des transactions...")

            dfClient_Modified = self.dfClient \
                                .withColumnRenamed("id", "users_id_client") \
                                .drop("ingestion_date")
                                
            dfCard_Modified =   self.dfCard \
                                .withColumnRenamed("id", "cards_id_card") \
                                .drop("ingestion_date", "client_id")
                                
            DfMCC_Modified =    self.DfMCC.drop("ingestion_date")
            
            dfFraud_Modified  = self.dfFraud.drop("ingestion_date")
            
            df = transactions_data_cleaned

            df =    transactions_data_cleaned \
                    .join(DfMCC_Modified, on="id", how="left") \
                    .join(dfClient_Modified, df.client_id == dfClient_Modified.users_id_client) \
                    .join(dfCard_Modified, df.card_id == dfCard_Modified.cards_id_card) \
                    .join(dfFraud_Modified, on="id") \
                    .drop("cards_id_card", "cards_client_id", "users_id_client")
                    
            money_cols = ["per_capita_income", "yearly_income", "total_debt", "credit_limit", "amount"]
            for col_name in money_cols:
                df = df.withColumn(
                    col_name,
                    regexp_replace(col(col_name), "\\$", "").cast(FloatType())
                )

            self.logger.info("✅ Prétraitement terminé avec succès.")
            return df

        except Exception as e:
            self.logger.error(f"❌ Erreur lors du prétraitement des transactions : {e}")
            raise
    