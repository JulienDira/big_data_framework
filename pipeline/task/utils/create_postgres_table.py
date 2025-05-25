import psycopg2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType, DateType

def spark_to_postgres_type(spark_type):
    """Mappe un type Spark SQL en type PostgreSQL."""
    if isinstance(spark_type, StringType):
        return "TEXT"
    elif isinstance(spark_type, IntegerType):
        return "INTEGER"
    elif isinstance(spark_type, LongType):
        return "BIGINT"
    elif isinstance(spark_type, DoubleType):
        return "DOUBLE PRECISION"
    elif isinstance(spark_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(spark_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(spark_type, DateType):
        return "DATE"
    else:
        # Par défaut on peut mettre TEXT ou lever une exception
        return "TEXT"

def create_table_if_not_exists(df, table_name, jdbc_url, user, password, host="localhost", port=5432, database=None):
    """
    Crée une table PostgreSQL si elle n'existe pas, en se basant sur le schéma du DataFrame Spark.
    """
    # Extrait le schema Spark
    schema = df.schema

    # Construire la partie colonnes SQL
    columns = []
    for field in schema.fields:
        col_name = field.name
        col_type = spark_to_postgres_type(field.dataType)
        columns.append(f'"{col_name}" {col_type}')
    columns_sql = ", ".join(columns)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """

    # Extraire la db name si elle n'est pas passée
    if database is None:
        # suppose que jdbc_url est du style jdbc:postgresql://host:port/dbname
        database = jdbc_url.split("/")[-1]

    try:
        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(create_table_sql)
        cur.close()
        conn.close()
        print(f"Table '{table_name}' créée ou déjà existante.")
    except Exception as e:
        print(f"Erreur lors de la création de la table : {e}")
        raise
