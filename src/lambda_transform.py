import json
from typing import Dict, Any, Optional, List

from logs.logger import logger
from src.s3 import S3Manager
from src.athena import AthenaManager
from src.utils import merge_data


def lambda_handler(event: Dict[str, Any], context: Optional[Any] = None) -> Dict[str, Any]:
    """
    Función principal para AWS Lambda de transformación.
    
    Args:
        event: Evento de AWS Lambda con uploaded_files de la lambda anterior.
        context: Objeto de contexto de AWS Lambda.
        
    Returns:
        Diccionario con la respuesta y archivos transformados.
    """
    try:
        # Obtener archivos subidos por la Lambda de extracción
        if 'body' in event:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            uploaded_files = body.get('uploaded_files', [])
        else:
            uploaded_files = event.get('uploaded_files', [])
        
        if not uploaded_files:
            logger.warning("No se encontraron archivos para transformar")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No se encontraron archivos para transformar"})
            }
        
        # Inicializar gestores
        s3_manager = S3Manager()
        athena_manager = AthenaManager()
        transformed_files = []
        
        list_objects = s3_manager.download_raw()
        empresas = athena_manager.get_empresas_data(list_objects)
        funcionarios = athena_manager.get_funcionarios_data(empresas)

        final_df = merge_data(list_objects, empresas, funcionarios)
        
        if len(final_df) == len(list_objects):

            s3_url = s3_manager.upload_processed(final_df, "processed")
            logger.info(f"DataFrame transformado y subido a S3: {s3_url}")
            if s3_url:
                transformed_files.append(s3_url)

        response = {
            "statusCode": 200,
            "len_validation": f'extracted:{len(list_objects)} transformed:{len(final_df)}',
            "counts_match": len(final_df) == len(list_objects),
            "final_df": final_df.head(100).to_dict(orient='records'),
            "body": json.dumps({
                "transformed_files": transformed_files,
                "message": f"Se transformaron {len(transformed_files)} de {len(uploaded_files)} archivos"
            })
        }
        
        return response
        
    except Exception as e:
        logger.exception("Error en lambda_handler de transformación")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }