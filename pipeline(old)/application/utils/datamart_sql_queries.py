def get_sql_query(db_name: str='process_data', output_table_name: str=None):
    
    sql_queries = {
        'transaction_validated': f"""
            SELECT *
            FROM {db_name}.transactions_data_processed
            WHERE info = 'Yes'
        """,

        'transaction_refused': f"""
            SELECT *
            FROM {db_name}.transactions_data_processed
            WHERE info = 'No'
        """,
        'top_10_consu': f"""
            SELECT client_id, current_age, gender, COUNT(amount) AS total_amount
            FROM {db_name}.transactions_data_processed
            GROUP BY client_id, current_age, gender
            ORDER BY total_amount DESC
            LIMIT 10
        """,

        'kpi_by_consu_id': f"""
            SELECT client_id, current_age, gender, 
            COUNT(amount) AS total_amount ,COUNT(*) AS transaction_count,
            AVG(amount) AS avg_spent,MAX(amount) AS max_spent
            FROM {db_name}.transactions_data_processed
            GROUP BY client_id, current_age, gender
        """,
        
        'cards_data': f"""
            SELECT *
            FROM {db_name}.cards_data_cleaned
        """,
        
        'mcc_codes': f"""
            SELECT *
            FROM {db_name}.mcc_codes
        """,
        
        'train_fraud_labels': f"""
            SELECT *
            FROM {db_name}.train_fraud_labels
        """,
        
        'transactions_data': f"""
            SELECT *
            FROM {db_name}.transactions_data_cleaned
        """,
        
        'transactions_data_processed': f"""
            SELECT *
            FROM {db_name}.transactions_data_processed
        """,
        
        'users_data': f"""
            SELECT *
            FROM {db_name}.users_data_cleaned
        """
    }
    return sql_queries.get(output_table_name, sql_queries)

    