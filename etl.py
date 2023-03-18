import pymongo as pm
import psycopg2

class ConectaBanco:
    def __init__(self, mongo_uri, mongo_db, mongo_collection_name ,pg_uri,pg_host, pg_port, pg_db_name,pg_user,pg_password):
        self.mongodb = mongo_db
        self.mongo_uri = mongo_uri
        self.mongo_collection_name = mongo_collection_name
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db_name = pg_db_name
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_uri = pg_uri
        self.mongo_client = pm.MongoClient(self.mongo_uri)
        self.mongodb = self.mongo_client[self.mongodb]

        self.postgress_con = psycopg2.connect(
            host = self.pg_uri,
            database = self.pg_db_name,
            user = self.pg_user,
            senha = self.pg_password
-+
85207410
        )
        self.postgress_con = self.postgress_con.cursor()

    def extract(self , collection_name):
        return self.mongodb[collection_name].find()
    

    def tranforma(self, mongo_data):
        postgress_data = []
        for data in mongo_data:
            tranforma_data = {
                'lista1': data["lista1"],
                'lista2': data['lista2'],
            }
            postgress_data.append(tranforma_data)

        return  postgress_data
    
    
    def load(self, table_name , postgress_data):
        for data in postgress_data:
            self.postgress_con.execute(
                """INSERT INTO {}(lista1 , lista2)
                    VALUES(%s,%s)
                """
            ).format(table_name), (data['lista1'], data['lista2'])
            self.postgress_con.commit()

    
    def run(self, colletion_name, table_name):
        mongo_Data = self.extract(colletion_name)
        post_data = self.tranforma(mongo_Data)
        self.load(table_name, post_data)



etl = ConectaBanco(
    mongo_uri="mongodb://localhost:27017/",
    mongo_db="estoque",
    pg_uri= "localhost",
    pg_db_name="test",
    pg_user="meu_user",
    pg_password="123456"
)

etl.run(colletion_name="estoque" , table_name="lista1")




    