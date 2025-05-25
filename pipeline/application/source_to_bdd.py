
from utils.create_postgres_table import create_table_if_not_exists
from utils.common_function import SparkPipeline
import os

def run_pipeline(app_name: str='SourceToDataBase'):

    try:
        SparkPipelineInit = SparkPipeline(app_name)
        SparkPipelineInit.logger.info(f"---------- Démarrage de l'étape {app_name} ----------")
        
        datasource  = SparkPipelineInit.local_path
        files = [f for f in os.listdir(datasource) if os.path.isfile(os.path.join(datasource, f))]

        if not files:
            SparkPipelineInit.logger.warning("Aucun fichier trouvé dans le dossier source.")
            return

        for file_name in files:
            source_file_path = f"file://{os.path.join(datasource, file_name)}"
            # source_file_path = f"{datasource}{file_name}"
            table_name = os.path.splitext(file_name)[0]
            
            SparkPipelineInit.logger.info(f"\n--- Traitement du fichier : {source_file_path} ---")
            
            if source_file_path.endswith(".csv"):
                raw_data =  SparkPipelineInit \
                            .read_csv_data_without_schema(source_file_path)
            else: 
                SparkPipelineInit \
                .logger.error(f"❌ Erreur lors de l'exécution de la pipeline extension inconnue : {table_name}")


            jdbc_url = SparkPipelineInit.postgres_jdbc_url
            jdbc_user = SparkPipelineInit.jdbc_user
            jdbc_password = SparkPipelineInit.jdbc_password

            create_table_if_not_exists(raw_data, table_name, jdbc_url, jdbc_user, jdbc_password, host="postgres", port=5432)
            
            SparkPipelineInit \
            .write_to_postgresql(
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

