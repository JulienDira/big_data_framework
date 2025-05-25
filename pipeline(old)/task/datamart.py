from utils.datamart_sql_queries import get_sql_query
from utils.create_postgres_table import create_table_if_not_exists
from utils.common_function import SparkPipeline, Transformation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp, current_timestamp
import os
import logging

def run_pipeline():
    """
    Exécute la pipeline complète pour transformer les fichiers dans un dossier local.

    Args:
        crypto_name (str): Nom de la cryptomonnaie (peut être utilisé comme nom de table).
    """
    try:
        
        SparkPipelineInit = SparkPipeline('Preprocessing')
        source_data_path = SparkPipelineInit.raw_path
        all_sql_queries = get_sql_query()
        
        
        for output_table_name, sql_query in all_sql_queries.items():
            
            # source_file_path = f"{source_data_path}{table_name}"
            SparkPipelineInit.logger.info(f"\n--- Traitement de la table de sortie : {output_table_name} ---")
            # partitioned = True if table_name == 'transactions_data' else False
            
            current_dataframe = SparkPipelineInit.read_from_hive_with_sql(
                sql_query=sql_query
            )
            
            create_table_if_not_exists(
                df=current_dataframe,
                table_name=output_table_name,
                jdbc_url=SparkPipelineInit.postgres_jdvc_url,
                user=SparkPipelineInit.jdbc_user,
                password=SparkPipelineInit.jdbc_password,
                host="postgres",
                port=5432
            )
            
            SparkPipelineInit.save_to_postgresql(
                dataframe=current_dataframe,
                table_name=output_table_name
            )    

            SparkPipelineInit.logger.info(f"✔ Pipeline exécutée avec succès pour '{output_table_name}'.")

    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
        
    finally:
        SparkPipelineInit.logger.info("🛑 Fermeture de la session Spark.")
        SparkPipelineInit.spark.stop()

if __name__ == "__main__":
    
    run_pipeline()
