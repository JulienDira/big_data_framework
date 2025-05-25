from utils.common_function import SparkPipeline
from pyspark.sql.functions import year, month, dayofmonth, current_timestamp, date_sub

def run_pipeline(app_name: str='BronzeStepFromJDBC'):

    try:
        SparkPipelineInit = SparkPipeline(app_name=app_name)
        SparkPipelineInit.logger.info(f"---------- Démarrage de l'étape {app_name} ----------")
        
        jdbc_url = SparkPipelineInit.postgres_jdbc_url
        raw_path  = SparkPipelineInit.raw_path
        ingestion_time = date_sub(current_timestamp(), 0)
        
        db_properties = SparkPipelineInit.db_properties
        
        table_name = "transactions_data"

        SparkPipelineInit.logger.info(f"\n--- Traitement de la table : {table_name} ---")

        jdbc_table = SparkPipelineInit \
                    .read_jdbc_data(table_name, jdbc_url, db_properties) \
                    .coalesce(10) \
                    .withColumn("ingestion_date", ingestion_time) \
                    .withColumn("year", year(ingestion_time)) \
                    .withColumn("month", month(ingestion_time)) \
                    .withColumn("day", dayofmonth(ingestion_time)) \
        
        jdbc_table.cache()
        
        target_table_path = f"{raw_path}{table_name}"
        partitioned_columns = ['year', 'month', 'day']
        
        (
            SparkPipelineInit
            .write_to_parquet(
                df=jdbc_table,
                full_table_path=target_table_path,
                write_mode='append',
                partitioned_columns=partitioned_columns
            )
        )
            

        SparkPipelineInit.logger.info(f"✔ Pipeline exécutée avec succès pour '{table_name}'.")

    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
        
    finally:
        SparkPipelineInit.logger.info("🛑 Fermeture de la session Spark.")
        SparkPipelineInit.spark.stop()

if __name__ == "__main__":
    
    run_pipeline()
    
    

