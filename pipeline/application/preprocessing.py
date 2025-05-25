from utils.schema_and_transformations import get_hive_schema
from utils.common_function import SparkPipeline, Transformation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp, current_timestamp
import os
import logging
from typing import List

def table_with_transformation(
    app_name: str,
    table_name: List,
    prefix: str,
    partitioned: bool,
    transformation: Transformation
):
    
    try:
        
        SparkPipelineInit = SparkPipeline(app_name)
        
        df = SparkPipelineInit \
            .read_parquet_data_without_schema(
            table_name=table_name,
            partitioned=partitioned
        )
        
        df_transformed = transformation(df)
        
        schema = get_hive_schema(table_name)
                                        
        SparkPipelineInit.create_hive_database()
        SparkPipelineInit.create_hive_table(table_name=f'{table_name}{prefix}', schema=schema)
        SparkPipelineInit.write_to_hive(df_transformed, f'{table_name}{prefix}') 
        
    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
        
def table_without_transformation(app_name: str, table_name: str):  
    
    try:
        
        SparkPipelineInit = SparkPipeline(app_name)    
        source_data_path = SparkPipelineInit.raw_path
        hive_schema = get_hive_schema(table_name)
            
        source_file_path = f"{source_data_path}{table_name}"
        SparkPipelineInit.logger.info(f"\n--- Traitement du fichier : {source_file_path} ---")
        
        current_dataframe = SparkPipelineInit.read_parquet_data_with_schema(
            table_name=table_name,
            schema=hive_schema
        )
                                
        SparkPipelineInit.create_hive_database()
        SparkPipelineInit.create_hive_table(table_name=table_name, schema=hive_schema)
        # SparkPipelineInit.drop_table_if_exists(table_name)
        SparkPipelineInit.write_to_hive(current_dataframe, table_name)    

        SparkPipelineInit.logger.info(f"✔ Pipeline exécutée avec succès pour '{table_name}'.")

    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}") 

def run_pipeline(app_name: str = 'Preprocessing'):
    
    TransformationInit = Transformation(app_name)    
    TransformationInit.logger.info(f"---------- Démarrage de l'étape {app_name} ----------")
    
    table_partitioned = ['transactions_data']

    cleaned_mapping = {
        'transactions_data': TransformationInit.CleaningTransactionsData,
        'cards_data': TransformationInit.CleaningCardsData,
        'users_data': TransformationInit.CleaningUsersData
    }
    
    for table_name, transformation in cleaned_mapping.items():
        partitioned = True if table_name in table_partitioned else False
        table_with_transformation(
            app_name=app_name,
            table_name=table_name,
            prefix='_cleaned',
            partitioned=partitioned,
            transformation=transformation
        )
        
    transformation_mapping = {
        'transactions_data': TransformationInit.PreProcessingTransaction
    }
    
    for table_name, transformation in transformation_mapping.items():
        partitioned = True if table_name in table_partitioned else False
        table_with_transformation(
            app_name=app_name,
            table_name=table_name,
            prefix='_processed',
            partitioned=partitioned,
            transformation=transformation
        )
        

    table_list_without_transfo = ['mcc_codes', 'train_fraud_labels']    
    for table_name in table_list_without_transfo :
        table_without_transformation(
            app_name=app_name,
            table_name=table_name
        )


if __name__ == "__main__":
    
    run_pipeline()
       
#---------------------- VERIFICATION PARTITIONNEMENT ----------------------#     
                    
        # first_path = os.path.join(df_transactions_data, f"year=2025/month=5/day=19")
        # first_partition = SparkPipelineInit.read_parquet_data_without_schema(
        #     table_name=first_path,
        #     partitioned=False
        # )
        # second_path = os.path.join(df_transactions_data, f"year=2025/month=5/day=20")
        # second_partition = SparkPipelineInit.read_parquet_data_without_schema(
        #     table_name=second_path,
        #     partitioned=False
        # )
        
        # third_path = os.path.join(df_transactions_data, f"year=2025/month=5/day=21")
        # third_partition = SparkPipelineInit.read_parquet_data_without_schema(
        #     table_name=third_path,
        #     partitioned=False
        # )
        
        # count_first = first_partition.count()
        # SparkPipelineInit.logger.info(f"Nombre de lignes dans la première partition : {count_first}")
        # count_second = second_partition.count()
        # SparkPipelineInit.logger.info(f"Nombre de lignes dans la deuxième partition : {count_second}")
        # count_third = third_partition.count()
        # SparkPipelineInit.logger.info(f"Nombre de lignes dans la troisième partition : {count_third}")
        # count_current_dataframe = current_dataframe.count()
        # SparkPipelineInit.logger.info(f"Nombre de lignes dans la dernière partition : {count_current_dataframe}")
            
#---------------------- FIN VERIFICATION PARTITIONNEMENT ----------------------#  
        