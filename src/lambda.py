import json
from trigger import main
from logs.logger import logger
from src.s3 import S3Manager
from config.config_loader import Config

def lambda_handler(event, context) -> dict:
    try:
        # Ejecutar scraping
        df = main()
        logger.debug("DataFrame generado, columnas: %s", df.columns.tolist())
        
        # Subir archivos a S3
        s3_manager = S3Manager()
        uploaded_files = s3_manager.upload_raw()
        logger.info(f"Archivos subidos a S3: {uploaded_files}")
        
        # Preparar payload de respuesta
        payload = {
            "scraped_records": len(df),
            "uploaded_files": uploaded_files,
            "sample_data": df.head(8).astype(str).to_dict(orient="records")
        }

        logger.info("Payload listo: %s", payload)

        response = {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload)
        }
        logger.debug("Response completa: %s", response)

        return response


    except Exception as e:
        logger.exception("Error en lambda_handler")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    test_event = {}
    test_context = None
    lambda_handler(test_event,test_context)

