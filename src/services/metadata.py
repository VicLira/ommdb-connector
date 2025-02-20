import requests
from typing import Any, Dict, Tuple, Optional

class OpenMetadataAPI:
    def __init__(self, api_url: str, api_token: str) -> None:
        self.api_url: str = api_url
        self.headers: Dict[str, str] = {"Authorization": f"Bearer {api_token}"}
        
    def get_service(self, service_name: str) -> Optional[dict]:
        """Verifica se o serviço existe no OpenMetadata."""
        response = requests.get(
            f"{self.api_url}/services/databaseServices", 
            headers=self.headers
        )
        if response.status_code == 200:
            services = response.json().get("data", [])
            for service in services:
                if service.get("name") == service_name:
                    return service
        return None
    
    def create_service(self, service_name: str, service_type: str, host_port: str, db_username: str, db_password: str) -> dict:
        """Criar um serviço no Open Metadata"""
        payload = {
            "name": service_name,
            "serviceType": service_type,
            "connection": {
                "config": {
                        "username": db_username,
                        "password": db_password,
                        "hostPort": host_port
                    }
            },
            "description": f"Serviço de {service_name} {service_type} criado via API."
            }
        response = requests.post(
            f"{self.api_url}/services/databaseServices",
            headers=self.headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    def get_database(self, service_name: str, database_name: str) -> Optional[dict]:
        """Verifica se o banco de dados existe no Open Metadata"""
        response = requests.get(
            f"{self.api_url}/databases",
            headers=self.headers
        )
        if response.status_code == 200:
            databases = response.json().get("data", [])
            for database in databases:
                if database.get("name") == database_name and database["service"]["name"] == service_name:
                    return database
        return None
    
    def create_database(self, service_name: str, database_name: str) -> dict:
        """Cria um banco de dados no Open Metadata"""   
        payload = {
            "name": database_name,
            "service": {
                "id": service_name,
                "type": "databaseService"
            },
            "description": f"Banco de dados {database_name} configurado automaticamente."
        }
        response = requests.post(
            f"{self.api_url}/databases",
            headers=self.headers,
            json=payload
        ) 
        response.raise_for_status()
        return response.json()
    
    def get_table(self, database_name: str, table_name: str) -> Optional[dict]:
        """Verifica se a tabela existe no Open Metadata"""
        response = requests.get(
            f"{self.api_url}/tables",
            headers=self.headers
        )
        if response.status_code == 200:
            tables = response.json().get("data", [])
            for table in tables:
                if table.get("name") == table_name and table["database"]["name"] == database_name:
                    return table
        return None
    
    def create_table(self, database_name: str, table_name: str, schema: dict) -> dict:
        """Cria uma tabela no OpenMetadata"""
        payload = {
            "name": table_name,
            "database": {"id": database_name, "type": "database"},
            "columns": schema,
            "description": f"Tabela {table_name} configurada automaticamente."
        }
        response = requests.post(
            f"{self.api_url}/tables",
            headers=self.headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()
        
                        
        
    def send_metadata(self, data: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Envia os metadados para a API do OpenMetadata"""
        response = requests.post(
            f"{self.api_url}/data",
            json=data,
            headers=self.headers
        )
        return response.status_code, response.json()