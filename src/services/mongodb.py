from pymongo import MongoClient
from typing import List

class MongoDBConnector:
    def __init__(self, uri: str) -> None:
        self.client = MongoClient(uri)
        
    def list_collections(self, database: str) -> List[str]:
        """Retorna uma lista com os nomes das collections do banco de dados"""
        db = self.client[database]
        return db.list_collection_names
    
    def fetch_documents(self, database: str, collection: str, batch_size: int = 1000) -> List[dict]:
        """Extrai os documentos (registros) de uma collection em lotes"""
        db = self.client[database]
        coll = db[collection]
        cursor = coll.find().batch_size(batch_size)
        
        for document in cursor:
            yield document