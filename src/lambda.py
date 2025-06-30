import json
from typing import Dict, Any, Optional

from logs.logger import logger
from src.s3 import S3Manager
from trigger import main


def lambda_handler(event: Dict[str, Any], context: Optional[Any] = None) -> Dict[str, Any]:
    """
    Función principal para AWS Lambda.
    
    Ejecuta el proceso de scraping y sube los archivos resultantes a S3.
    
    Args:
        event: Evento de AWS Lambda que activa la función.
        context: Objeto de contexto de AWS Lambda.
        
    Returns:
        Diccionario con la respuesta HTTP para API Gateway.
    """
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
    test_event: Dict[str, Any] = {}
    test_context = None
    lambda_handler(test_event, test_context)

