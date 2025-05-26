from utils.schema_and_transformations import get_spark_schema
from utils.common_function import SparkPipeline
from pyspark.sql.functions import date_sub, current_timestamp
import os

def list_files(datasource: str, spark) -> list:
    if datasource.startswith("hdfs://") or datasource.startswith("wasb://") or datasource.startswith("s3a://"):
        # Utilisation du FileSystem Hadoop via SparkContext
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(datasource)
        files_status = fs.listStatus(path)
        files = [file.getPath().toString() for file in files_status if file.isFile()]
    else:
        # Système local
        datasource = datasource[len("file://"):]
        files = [os.path.join(datasource, f) for f in os.listdir(datasource) if os.path.isfile(os.path.join(datasource, f))]
        files = ["file://" + f for f in files]
    return files

def run_pipeline(app_name: str='BronzeStepFromFileSys'):

    try:
        SparkPipelineInit = SparkPipeline(app_name)
        SparkPipelineInit.logger.info(f"---------- Démarrage de l'étape {app_name} ----------")
        
        datasource  = SparkPipelineInit.local_path
        SparkPipelineInit.logger.info(f"📁 Datasource utilisé : {datasource}")
        files = list_files(datasource, SparkPipelineInit.spark)
        raw_path = SparkPipelineInit.raw_path
        # files = [f for f in os.listdir(datasource) if os.path.isfile(os.path.join(datasource, f))]
        ingestion_time = date_sub(current_timestamp(), 0)
        
        if not files:
            SparkPipelineInit.logger.warning("Aucun fichier trouvé dans le dossier source.")
            return

        for source_file_path in files:
            file_name = os.path.basename(source_file_path)
            table_name = os.path.splitext(file_name)[0]

            table_schem = get_spark_schema(table_name)
            target_table_path = f"{raw_path}{table_name}"
            
            SparkPipelineInit.logger.info(f"\n--- Traitement du fichier : {source_file_path} ---")
            
            if source_file_path.endswith(".csv"):
                raw_data =  SparkPipelineInit \
                            .read_csv_data_with_schema(source_file_path, table_schem) \
                            .withColumn("ingestion_date", ingestion_time)
                
            elif source_file_path.endswith(".json"):
                raw_data =  SparkPipelineInit \
                            .read_json_data_with_schema(source_file_path, table_schem) \
                            .withColumn("ingestion_date", ingestion_time)
            else: 
                SparkPipelineInit \
                .logger.error(f"❌ Erreur lors de l'exécution de la pipeline extension inconnue : {table_name}")

            SparkPipelineInit \
            .write_to_parquet(raw_data, target_table_path)

            SparkPipelineInit.logger.info(f"✔ Pipeline exécutée avec succès pour '{file_name}'.")

    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
        
    finally:
        SparkPipelineInit.logger.info("🛑 Fermeture de la session Spark.")
        SparkPipelineInit.spark.stop()

if __name__ == "__main__":

    run_pipeline()
