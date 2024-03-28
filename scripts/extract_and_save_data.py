from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import requests

def connect_mongo(uri):

    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client


def create_connect_db(client, db_name):

    db = client[db_name]
    return db


def create_connect_collection(db, col_name):
    
    collection = db[col_name]
    return collection


def extract_api_data(url):

    return requests.get(url).json() # Extraindo dados do Endpoint


def insert_data(col, data):

    result = col.insert_many(data)
    n_docs_inseridos = len(result.inserted_ids)
    return n_docs_inseridos


if __name__ == "__main__":

    uri = "mongodb+srv://mathgatto:O0o7PyRPLLnOI5Iw@cluster-pipeline.yng9pbu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline"
    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos_desafio")
    
    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraídos: {len(data)}")

    n_docs = insert_data(col,data)
    print(f"\nQuantidade de documentos inseridos: {n_docs}")

    client.close()
