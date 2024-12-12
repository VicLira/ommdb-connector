from pipelines.pipeline import DataPipeline
from config.settings import settings
import logging

logging.basicConfig(level=logging.INFO)

def main() -> None:
    logging.info("Iniciando o pipeline...")
    pipeline = DataPipeline(
        db_username=settings.DB_USERNAME,
        db_password=settings.DB_PASSWORD,
        service_name=settings.SERVICE_NAME,
        service_type=settings.SERVICE_TYPE,
        mongo_uri=settings.MONGO_URI,
        om_api_url=settings.OM_API_URL,
        om_api_token=settings.OM_API_TOKEN,
        batch_size=settings.BATCH_SIZE
    )
    try:
        pipeline.run("testeAPI", "produtos")
        logging.info("Pipeline concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro no pipeline: {str(e)}")
        
if __name__ == "__main__":
    logging.info("Chamando a função principal...")
    main()