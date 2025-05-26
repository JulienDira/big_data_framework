from utils.create_postgres_table import create_table_if_not_exists
from utils.common_function import SparkPipeline
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

def run_pipeline(app_name: str='SourceToDataBase'):

    try:
        SparkPipelineInit = SparkPipeline(app_name)
        SparkPipelineInit.logger.info(f"---------- Démarrage de l'étape {app_name} ----------")
        
        datasource  = SparkPipelineInit.local_path  # ex: 'hdfs://namenode:9000/...' ou '/app/data/...'
        files = list_files(datasource, SparkPipelineInit.spark)

        if not files:
            SparkPipelineInit.logger.warning("Aucun fichier trouvé dans le dossier source.")
            return
        
        for source_file_path in files:
            file_name = os.path.basename(source_file_path)
            table_name = os.path.splitext(file_name)[0]
            
            SparkPipelineInit.logger.info(f"\n--- Traitement du fichier : {source_file_path} ---")
            
            if source_file_path.endswith(".csv"):
                raw_data =  SparkPipelineInit.read_csv_data_without_schema(source_file_path)
            else: 
                SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline extension inconnue : {table_name}")
                continue  # Passe au fichier suivant

            jdbc_url = SparkPipelineInit.postgres_jdbc_url
            jdbc_user = SparkPipelineInit.jdbc_user
            jdbc_password = SparkPipelineInit.jdbc_password

            create_table_if_not_exists(raw_data, table_name, jdbc_url, jdbc_user, jdbc_password, host="postgres", port=5432)
            
            SparkPipelineInit.write_to_postgresql(
                dataframe=raw_data,
                table_name=table_name,
                write_mode='append'
            )

            SparkPipelineInit.logger.info(f"✔ Pipeline exécutée avec succès pour '{file_name}'.")

    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
        
    finally:
        SparkPipelineInit.logger.info("🛑 Fermeture de la session Spark.")
        SparkPipelineInit.spark.stop()

if __name__ == "__main__":
    run_pipeline()
