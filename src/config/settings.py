import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_USERNAME=os.getenv("DB_USERNAME")
    DB_PASSWORD=os.getenv("DB_PASSWORD")
    SERVICE_NAME = os.getenv("SERVICE_NAME")
    SERVICE_TYPE = os.getenv("SERVICE_TYPE")
    MONGO_URI = os.getenv("MONGO_URI")
    OM_API_URL = os.getenv("OM_API_URL")
    OM_API_TOKEN = os.getenv("OM_API_TOKEN")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE"))

settings = Settings()