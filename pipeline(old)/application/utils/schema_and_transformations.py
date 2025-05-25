from pyspark.sql.types import (
    StructType, StructField, FloatType,
    IntegerType, StringType, DateType,
    DoubleType, TimestampType
)
from collections import Counter
from pyspark.sql.functions import to_timestamp,regexp_replace, col,broadcast

def get_spark_schema(table_name: str=None):
    schema = {
        'train_fraud_labels': StructType([
            StructField("id", IntegerType(), True),
            StructField("info", StringType(), True),
            StructField("ingestion_date", DateType(), True)
        ]),

        'users_data': StructType([
            StructField("id", IntegerType(), True),
            StructField("current_age", IntegerType(), True),
            StructField("retirement_age", IntegerType(), True),
            StructField("birth_year", IntegerType(), True),
            StructField("birth_month", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("per_capita_income", StringType(), True),
            StructField("yearly_income", StringType(), True),
            StructField("total_debt", StringType(), True),
            StructField("credit_score", IntegerType(), True),
            StructField("num_credit_cards", IntegerType(), True),
            StructField("ingestion_date", DateType(), True)
        ]),

        'transactions_data': StructType([
            StructField("id", IntegerType(), True),
            StructField("date", TimestampType(), True),
            StructField("client_id", IntegerType(), True),
            StructField("card_id", IntegerType(), True),
            StructField("amount", StringType(), True),
            StructField("use_chip", StringType(), True),
            StructField("merchant_id", IntegerType(), True),
            StructField("merchant_city", StringType(), True),
            StructField("merchant_state", StringType(), True),
            StructField("zip", DoubleType(), True),
            StructField("mcc", IntegerType(), True),
            StructField("errors", StringType(), True),
            StructField("ingestion_date", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True)
        ]),

        'mcc_codes': StructType([
            StructField("id", StringType(), True),
            StructField("item", StringType(), True),
            StructField("ingestion_date", DateType(), True)
        ]),

        'cards_data': StructType([
            StructField("id", IntegerType(), True),
            StructField("client_id", IntegerType(), True),
            StructField("card_brand", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("card_number", StringType(), True),
            StructField("expires", StringType(), True),
            StructField("cvv", IntegerType(), True),
            StructField("has_chip", StringType(), True),
            StructField("num_cards_issued", IntegerType(), True),
            StructField("credit_limit", StringType(), True),
            StructField("acct_open_date", StringType(), True),
            StructField("year_pin_last_changed", IntegerType(), True),
            StructField("card_on_dark_web", StringType(), True),
            StructField("ingestion_date", DateType(), True)
        ])
    }

    return schema.get(table_name, schema)

def get_hive_schema(table_name: str=None):
    schema = {
        'train_fraud_labels': "id INT, info STRING",
        'users_data': """id INT, current_age INT, retirement_age INT, birth_year INT, birth_month INT,
                         gender STRING, address STRING, latitude DOUBLE, longitude DOUBLE,
                         per_capita_income STRING, yearly_income STRING, total_debt STRING,
                         credit_score INT, num_credit_cards INT""",
        'transactions_data': """id INT, date TIMESTAMP, client_id INT, card_id INT, amount STRING,
                                use_chip STRING, merchant_id INT, merchant_city STRING, merchant_state STRING,
                                zip DOUBLE, mcc INT, errors STRING,
                                year INT, month INT, day INT""",
        'mcc_codes': "id STRING, item STRING",
        'cards_data': """id INT, client_id INT, card_brand STRING, card_type STRING, card_number STRING,
                         expires STRING, cvv INT, has_chip STRING, num_cards_issued INT,
                         credit_limit STRING, acct_open_date STRING, year_pin_last_changed INT,
                         card_on_dark_web STRING"""
    }

    return schema.get(table_name, schema)
