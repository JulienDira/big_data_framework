import os
import logging

from utils.datamart_sql_queries import get_sql_query
from utils.create_postgres_table import create_table_if_not_exists
from utils.common_function import SparkPipeline, Transformation

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp, current_timestamp, when, length, substring

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    RegressionEvaluator,
    MulticlassClassificationEvaluator
)
from pyspark.ml.regression import GBTRegressor

import logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, col

def train_time_series_amount_model(df, date_col="date", label_col="amount"):

    df = df.withColumn("year", year(col(date_col))) \
           .withColumn("month", month(col(date_col))) \
           .withColumn("day", dayofmonth(col(date_col))) \
           .withColumn("day_of_week", dayofweek(col(date_col)))

    # Remplacer les valeurs nulles
    df = df.fillna({label_col: 0})

    feature_cols = ["year", "month", "day", "day_of_week"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Préparer les données avec les features
    df = assembler.transform(df)

    # Split en train/test
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    if train_data.count() == 0 or test_data.count() == 0:
        raise ValueError("Pas assez de données pour entraîner ou tester le modèle.")

    # Modèle GBT pour la régression
    gbt = GBTRegressor(featuresCol="features", labelCol=label_col)

    # Entraînement
    model = gbt.fit(train_data)

    # Prédictions sur le test
    predictions = model.transform(test_data)

    # Évaluation RMSE
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Performance du modèle (RMSE) : {rmse:.2f}")

    return model, predictions

def train_transaction_amount_model(df):
   
    df = df.withColumn("credit_score", col("credit_score").cast("float")) \
           .withColumn("num_credit_cards", col("num_credit_cards").cast("int")) \
           .withColumn("has_chip", col("has_chip").cast("int")) \
           .withColumn("card_on_dark_web", col("card_on_dark_web").cast("int")) \
           .withColumn("amount", col("amount").cast("float")) \
           .withColumn("yearly_income", col("yearly_income").cast("float")) \
           .withColumn("total_debt", col("total_debt").cast("float")) \
           .withColumn("credit_limit", col("credit_limit").cast("float")) \
           .withColumn("per_capita_income", col("per_capita_income").cast("float"))

    df = df.fillna({
        "credit_score": 600,
        "num_credit_cards": 1,
        "has_chip": 1,
        "card_on_dark_web": 0,
        "yearly_income": 30000,
        "total_debt": 1000,
        "credit_limit": 5000,
        "per_capita_income": 25000,
        "gender": "unknown",
        "card_brand": "unknown",
        "card_type": "unknown"
    })


    if df.count() == 0:
        raise ValueError("Le DataFrame est vide après traitement !")


    string_cols = ["gender", "card_brand", "card_type"]
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        for col in string_cols
    ]

   
    feature_cols = [
        "credit_score", "num_credit_cards", "yearly_income", "total_debt",
        "credit_limit", "per_capita_income", "has_chip", "card_on_dark_web",
        "gender_index", "card_brand_index", "card_type_index"
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

   
    gbt = GBTRegressor(labelCol="amount", featuresCol="features")

    pipeline = Pipeline(stages=indexers + [assembler, gbt])

  
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    if train_data.count() == 0 or test_data.count() == 0:
        raise ValueError("Pas assez de données pour entraîner ou tester le modèle.")

    model = pipeline.fit(train_data)

    
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="amount", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    logging.info(f"RMSE sur test : {rmse:.2f}")

    return model

def train_card_brand_classifier(df):

    df = df.filter(col("card_brand").isin("Visa", "Mastercard","Discover","Amex"))

   
    df = df.withColumn("prefix1", substring("card_number", 1, 1).cast("int")) \
           .withColumn("prefix2", substring("card_number", 1, 2).cast("int")) \
           .withColumn("prefix4", substring("card_number", 1, 4).cast("int")) \
           .withColumn("length", length("card_number"))

 
    label_indexer = StringIndexer(inputCol="card_brand", outputCol="label")
    label_model = label_indexer.fit(df)
    df = label_model.transform(df)

 
    feature_cols = ["prefix1", "prefix2", "prefix4", "length"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")


    train_df, test_df = df.randomSplit([0.8, 0.2], seed=12345)

    classifier = RandomForestClassifier(labelCol="label", featuresCol="features")
    pipeline = Pipeline(stages=[assembler, classifier])

   
    model = pipeline.fit(train_df)

   
    predictions = model.transform(test_df)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    logging.info(f"Accuracy: {accuracy:.2%}")

    return model

def run_pipeline(app_name: str = 'ModelTrain'):

    try:
        SparkPipelineInit = SparkPipeline(app_name)
        SparkPipelineInit.logger.info(f"---------- Démarrage de l'étape {app_name} ----------")
        
        input_table_name = 'transactions_data_processed'
                
        SparkPipelineInit.logger.info(f"\n--- Entrainement de modèles sur la table : {input_table_name} ---")
        
        input_dataframe = SparkPipelineInit.read_from_hive_with_sql(
            sql_query= "SELECT * FROM process_data.transactions_data_processed"
        )
        
        train_time_series_amount_model(input_dataframe)
        # train_transaction_amount_model(input_dataframe)
        # train_card_brand_classifier(input_dataframe)
        
        SparkPipelineInit.logger.info(f"✔ Pipeline exécutée avec succès pour '{output_table_name}'.")

    except Exception as e:
        SparkPipelineInit.logger.error(f"❌ Erreur lors de l'exécution de la pipeline : {e}")
        
    finally:
        SparkPipelineInit.logger.info("🛑 Fermeture de la session Spark.")
        SparkPipelineInit.spark.stop()

if __name__ == "__main__":
    
    run_pipeline()
