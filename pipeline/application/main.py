import source_to_bdd
import feeder_jdbc
import feeder_file
import preprocessing
import datamart
import ml
import os
import logging

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.info("---------- Démarrage de la pipeline ----------")
    
    os.environ['DATA_SOURCE_PATH'] = '/app/datasource/to_db/global_file/'
    os.environ['POSTGRES_JDBC_URL'] = 'jdbc:postgresql://postgres:5432/source_data'
    
    source_to_bdd.run_pipeline(app_name='SourceToDataBase')
    feeder_jdbc.run_pipeline(app_name='BronzeStepFromJDBC')

    os.environ['DATA_SOURCE_PATH'] = '/app/datasource/'
    os.environ['POSTGRES_JDBC_URL'] = 'jdbc:postgresql://postgres:5432/datamart'
    
    feeder_file.run_pipeline(app_name='BronzeStepFromFileSys')
    preprocessing.run_pipeline(app_name='Preprocessing')
    datamart.run_pipeline(app_name='DataMart')
    
    # ml.run_pipeline() # Activer qu'en cas de ressources machines suffisantes
    