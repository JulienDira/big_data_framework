from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import year, sum as spark_sum

# 1 - Compter le nombre de transactions des 3 derniers mois, avec et sans cache, et comparer
def count_transactions_last_3_months(spark: SparkSession, transactions_df):
    # Filtrer les 3 derniers mois
    cutoff_date = date_sub(current_date(), 90) # A REMPLACER PAR MAX DATE 
    df_filtered = transactions_df.filter(col("transaction_date") >= cutoff_date)
    
    # Sans cache
    import time
    start = time.time()
    count_no_cache = df_filtered.count()
    duration_no_cache = time.time() - start
    
    # Avec cache
    df_filtered.cache()
    start = time.time()
    count_cache = df_filtered.count()
    duration_cache = time.time() - start
    df_filtered.unpersist()
    
    print(f"Count no cache: {count_no_cache}, duration: {duration_no_cache:.2f}s")
    print(f"Count with cache: {count_cache}, duration: {duration_cache:.2f}s")
    
    # Pour analyse avancée tu peux utiliser Spark UI ou library comme pyspark.pandas, sparkmeasure etc
    
    return {
        "count_no_cache": count_no_cache,
        "duration_no_cache": duration_no_cache,
        "count_cache": count_cache,
        "duration_cache": duration_cache
    }

# 2 - Supprimer cache, jointure sans optimisation puis avec optimisation, comparer
def join_with_clients(spark: SparkSession, transactions_df, clients_df):
    # Supprimer cache si existant
    transactions_df.unpersist(blocking=True)
    
    # Jointure sans optimisation
    import time
    start = time.time()
    joined_no_opt = transactions_df.join(clients_df, "client_id")
    count_no_opt = joined_no_opt.count()
    duration_no_opt = time.time() - start
    
    # Jointure avec optimisation broadcast (si clients_df est petit)
    start = time.time()
    joined_opt = transactions_df.join(broadcast(clients_df), "client_id")
    count_opt = joined_opt.count()
    duration_opt = time.time() - start
    
    print(f"Join no optimization: count={count_no_opt}, duration={duration_no_opt:.2f}s")
    print(f"Join with broadcast: count={count_opt}, duration={duration_opt:.2f}s")
    
    return {
        "no_optimization": {"count": count_no_opt, "duration": duration_no_opt},
        "broadcast_optimization": {"count": count_opt, "duration": duration_opt}
    }

# 3 - Associer à transactions les labels selon le code "mcc"  
def join_mcc_labels(transactions_df, mcc_labels_df):
    # On suppose transactions_df a colonne "mcc"
    labeled_df = transactions_df.join(mcc_labels_df, on="mcc", how="left")
    return labeled_df

# 4 - Créer 2 vues temporaires : frauduleuses et non-frauduleuses
def create_temp_views(spark: SparkSession, transactions_df):
    fraud_df = transactions_df.filter(col("is_fraud") == True)
    non_fraud_df = transactions_df.filter(col("is_fraud") == False)
    
    fraud_df.createOrReplaceTempView("fraud_transactions")
    non_fraud_df.createOrReplaceTempView("non_fraud_transactions")

# 5 - Enrichir transactions avec données clients + info fraude puis sauvegarder désérialisé
def enrich_and_save(transactions_df, clients_df, fraud_info_df, output_path):
    enriched = transactions_df.join(clients_df, "client_id", "left") \
                              .join(fraud_info_df, "transaction_id", "left")
    # Sauvegarder en format parquet (désérialisé)
    enriched.write.mode("overwrite").parquet(output_path)
    
# 6 - Top 10 clients 2010 (plus dépensé) stocké dans table Hive
def top10_clients_2010(spark: SparkSession, transactions_df):
    df_2010 = transactions_df.filter(year("transaction_date") == 2010)
    top10 = df_2010.groupBy("client_id") \
                   .agg(spark_sum("amount").alias("total_spent")) \
                   .orderBy(col("total_spent").desc()) \
                   .limit(10)
    # Sauvegarde dans Hive
    top10.write.mode("overwrite").saveAsTable("top10_clients_2010")




 

