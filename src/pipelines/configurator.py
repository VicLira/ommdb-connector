import logging
from services.metadata import OpenMetadataAPI

class MetadataConfigurator:
    def __init__(self, om: OpenMetadataAPI) -> None:
        self.om: OpenMetadataAPI = om
        
    def ensure_service(self, service_name: str, service_type: str, host_port: str, db_username: str, db_password: str) -> dict:
        service = self.om.get_service(service_name)
        if not service:
            logging.info(f"Serviço {service_name} não encontrado. Criando...")
            service = self.om.create_service(service_name, service_type, host_port, db_username, db_password)
        return service
    
    def ensure_database(self, service_name: str, database_name: str) -> dict:
        database = self.om.get_database(service_name, database_name)
        if not database:
            logging.info(f"Banco de dados {database_name} não encontrado. Criando...")
            database = self.om.create_database(service_name, database_name)
        return database
    
    def ensure_table(self, database_name: str, table_name: str) -> dict:
        table = self.om.get_table(database_name, table_name)
        if not table:
            logging.info(f"Tabela {table_name} não encontrada. Criando...")
            table = self.om.create_table(database_name, table_name)
        return table
    