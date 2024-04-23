from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests 


class elt_produto():

    def __init__(self):
        self.uri = "mongodb+srv://enricoasc:4H9IPKZcyE6qApQ9@cluster-pipeline.qvp7tpq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline"
        self.url = 'https://labdados.com/produtos'
        self.stats = False



    def connect_mongo(self):
        clientMongo = MongoClient(self.uri, server_api=ServerApi('1'))

        # Send a ping to confirm a successful connection
        try:
            clientMongo.admin.command('ping')
            self.stats = True
            return clientMongo
        except Exception as e:
            self.stats = False
            print(e)


    def create_connect_db(self,client, db_name):
        db = client[db_name]
        return db



    def create_connect_collection(self,db, col_name):
        collection = db[col_name]
        return collection



    def extract_api_data(self):
        response = requests.get(self.url)
        return response.json()



    def insert_data(self, col, data):
         docs = col.insert_many(data)
         qtd_inserted = len(docs.inserted_ids)
         return qtd_inserted


    def close_connect_db(self,client):
        client.close()
        self.stats= False
        print(f'Status conexão => {self.stats}')

    def drop_database(self, client ,database):
        if self.stats:
            client.drop_database(name_or_database=database)
            print(f'Banco {database} foi excluido com sucesso !')
        else:
            print('Conexão esta fechada')