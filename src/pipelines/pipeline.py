import logging
import json
from bson import ObjectId
from typing import NoReturn, List
from services.mongodb import MongoDBConnector
from services.metadata import OpenMetadataAPI
from services.utils import flatten_document, serialize_document
from pipelines.configurator import MetadataConfigurator

logging.basicConfig(level=logging.INFO)

class DataPipeline:
    def __init__(
            self, 
            db_username: str,
            db_password: str,
            service_name: str,
            service_type: str,
            mongo_uri: str, 
            om_api_url: str, 
            om_api_token: str, 
            batch_size: int = 1000
        ) -> None:
        self.db_username = db_username
        self.db_password = db_password
        self.service_name = service_name
        self.service_type = service_type
        self.host_port = mongo_uri.split("//")[1].split("/")[0].split("@")[1]
        self.database = mongo_uri.split("//")[1].split("/")[1]
        
        self.mongo: MongoDBConnector = MongoDBConnector(mongo_uri)
        self.om: OpenMetadataAPI = OpenMetadataAPI(om_api_url, om_api_token)
        self.batch_size: int = batch_size
        
        self.configurator: MetadataConfigurator = MetadataConfigurator(self.om)
        
        
    def run(self, database: str, collection: str) -> NoReturn:
        """
        Executa o pipeline parza extrair documentos do MongoDB,
        processá-los e enviá-los ao OpenMetadata.
        """
        
        # Garantir configurações no Open Metadata
        service = self.configurator.ensure_service(self.service_name, self.service_type, self.host_port, self.db_username, self.db_password)
        database_obj = self.configurator.ensure_database(service["name"], database)
        self.configurator.ensure_table(database_obj["name"], collection)
        
        # Processar documentos e enviar metadados
        try:
            for document in self.mongo.fetch_documents(database, collection, self.batch_size):
                logging.info(f"Processando documento com _id: {document.get('_id')}")
                
                if "_id" in document and isinstance(document["_id"], ObjectId):
                    document["_id"] = str(document["_id"])
                
                flat_doc = flatten_document(document)
                status, response = self.om.send_metadata(json.loads(serialize_document(flat_doc)))
                if status != 200:
                    logging.error(f"Erro ao enviar metadados: {response}")
                else:
                    logging.info(f"Metadados enviados com sucesso para o documento {document["_id"]}.")
        except Exception as e:
            logging.error(f"Erro durante a execução do pipeline: {e}", exc_info=True)