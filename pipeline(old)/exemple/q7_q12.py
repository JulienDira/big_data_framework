from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import year, sum as spark_sum
from pyspark.sql.functions import month, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, max as spark_max, count, last, month
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# 7 - Top 3 clients par mois en 2010 (plus dépensé)
def top3_clients_per_month_2010(transactions_df):
    df_2010 = transactions_df.filter(year("transaction_date") == 2010) \
                            .withColumn("month", month("transaction_date"))
    
    windowSpec = Window.partitionBy("month").orderBy(col("total_spent").desc())
    monthly_totals = df_2010.groupBy("month", "client_id") \
                            .agg(spark_sum("amount").alias("total_spent"))
    
    top3 = monthly_totals.withColumn("rank", row_number().over(windowSpec)) \
                         .filter(col("rank") <= 3) \
                         .orderBy("month", "rank")
    return top3

# 8 - Créer datamart métriques clients 2010, stocker dans BDD, API FastAPI avec pagination
def create_client_metrics_datamart(spark: SparkSession, transactions_df):
    df_2010 = transactions_df.filter(year("transaction_date") == 2010) \
                            .withColumn("month", month("transaction_date"))
    
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    metrics_df = df_2010.groupBy("client_id", year("transaction_date").alias("year")) \
                        .agg(
                            count("*").alias("nb_transactions"),
                            avg("amount").alias("avg_spent"),
                            spark_max("amount").alias("max_spent"),
                            last("transaction_date").alias("last_transaction_date")
                        )
    
    # Mois avec le plus de dépenses par client (approche via window)
    windowSpec = Window.partitionBy("client_id").orderBy(col("total_monthly_spent").desc())
    monthly_spent = df_2010.groupBy("client_id", "month") \
                          .agg(spark_sum("amount").alias("total_monthly_spent"))
    best_month = monthly_spent.withColumn("rank", row_number().over(windowSpec)) \
                             .filter(col("rank") == 1) \
                             .select("client_id", col("month").alias("best_month"))
    
    result = metrics_df.join(best_month, "client_id")
    
    return result

# Sauvegarder dans PostgreSQL via JDBC
def save_to_postgres(df, jdbc_url, table_name, properties):
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
    
# 9 - Entraîner modèle fraud detection avec 3 algos et validation
def train_and_validate_fraud_model(spark, transactions_df):
    # Split train/test
    train_df, test_df = transactions_df.randomSplit([0.7, 0.3], seed=42)
    
    # Features et label
    feature_cols = ["amount", "some_other_feature"]  # adapte avec tes colonnes
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    label_indexer = StringIndexer(inputCol="is_fraud", outputCol="label")
    
    models = {
        "LogisticRegression": LogisticRegression(featuresCol="features", labelCol="label"),
        "RandomForest": RandomForestClassifier(featuresCol="features", labelCol="label"),
        "GBT": GBTClassifier(featuresCol="features", labelCol="label")
    }
    
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    
    results = {}
    for name, algo in models.items():
        pipeline = Pipeline(stages=[label_indexer, assembler, algo])
        model = pipeline.fit(train_df)
        preds = model.transform(test_df)
        auc = evaluator.evaluate(preds)
        results[name] = auc
        print(f"{name} AUC: {auc:.4f}")
    return results

# 10 - Partitionner les transactions par mois dans dossier bronze_transactions_by_month
def partition_transactions_by_month(transactions_df, output_dir):
    transactions_df.withColumn("month", month("transaction_date")) \
                   .write.mode("overwrite") \
                   .partitionBy("month") \
                   .parquet(output_dir + "/bronze_transactions_by_month")

# 11 - Moyenne des dépenses en ligne et sur place + optimisation
from pyspark.sql.functions import avg

def avg_online_vs_onsite(transactions_df):
    avg_df = transactions_df.groupBy("transaction_type") \
                            .agg(avg("amount").alias("avg_amount"))
    return avg_df

# Optimisation possible : 
# - Assurer que "transaction_type" est bien une colonne catégorielle (indexée si ML)
# - Partitionner ou filtrer sur les types avant l'agrégation pour réduire shuffle
# - Cacher dataframe si réutilisé

# 12 - Stocker résultat question 8 en fichiers avec 5 partitions
def save_client_metrics_partitioned(metrics_df, output_path):
    metrics_df.repartition(5).write.mode("overwrite").parquet(output_path)
