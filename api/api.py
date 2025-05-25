from flask import Flask, request, jsonify
import pandas as pd
from sqlalchemy import create_engine, text

# Connexion à la base (ajuste selon ton moteur : PostgreSQL, MySQL, etc.)
DATABASE_URL = "postgresql+psycopg2://hive:hive@localhost:5432/datamart"
engine = create_engine(DATABASE_URL)

app = Flask(__name__)

# Fonction utilitaire pour la pagination
def get_paginated_query(table_name, page, size):
    offset = (page - 1) * size
    query = text(f"SELECT * FROM {table_name} LIMIT :limit OFFSET :offset")
    return pd.read_sql(query, con=engine, params={"limit": size, "offset": offset})

@app.route("/fraud-transactions", methods=["GET"])
def get_fraud_transactions():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 20))
    df = get_paginated_query("train_fraud_labels", page, size)
    return jsonify(df.to_dict(orient="records"))

@app.route("/legit-transactions", methods=["GET"])
def get_legit_transactions():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 20))
    df = get_paginated_query("transaction_validated", page, size)
    return jsonify(df.to_dict(orient="records"))

@app.route("/top10-clients", methods=["GET"])
def get_top10_clients():
    # Pas besoin de paginer car il y a un max de 10 lignes
    df = pd.read_sql("SELECT * FROM top_10_consu", con=engine)
    return jsonify(df.to_dict(orient="records"))

@app.route("/client-info", methods=["GET"])
def get_client_info():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 20))
    df = get_paginated_query("users_data", page, size)
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(debug=True)
