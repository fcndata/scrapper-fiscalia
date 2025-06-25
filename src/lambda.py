import json
from trigger import main
from logs.logger import logger
from src.s3 import S3Manager

def lambda_handler(event, context) -> dict:
    try:
        # Ejecutar scraping
        main()
        
        # Subir archivos a S3
        s3_manager = S3Manager()
        uploaded_files = s3_manager.upload_raw()
        logger.info(f"Archivos subidos a S3: {uploaded_files}")

        response = {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"uploaded_files": uploaded_files})
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

