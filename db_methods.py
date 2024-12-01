from pymongo.mongo_client import MongoClient
import json
import pandas as pd
from db_template import db_template
from dotenv import load_dotenv
import os

load_dotenv()

class DbMethods:
    def __init__(self):
        self.client = MongoClient(os.getenv('MONGO_DB_URL_READ_WRITE'))
        self.db = self.client['einsatztracker']
        self.collection = self.db['einsaetze']
    

    def dbPost(self, data: dict):
        self.collection.insert_one(db_template(**data))


    def dbGetAll(self) -> pd.DataFrame:
        return pd.DataFrame(list(self.collection.find({})))
    

    def dbGetOne(self, dbFilter: dict) -> dict:
        return self.collection.find_one(dbFilter)
    

    def dbUpdateOne(self, dbFilter: dict, newContent: dict):
        return self.collection.update_one(dbFilter, {"$set": newContent})


    def dbDeleteOne(self, dbFilter: dict):
        return self.collection.delete_one(dbFilter)


if __name__ == '__main__':
    dbm = DbMethods()