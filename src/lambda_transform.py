import json
from typing import Dict, Any, Optional, List

from logs.logger import logger
from src.s3 import S3Manager
from src.athena import AthenaManager
from src.utils import merge_data,reglas_de_negocio
from src.simple_email_service import SESManager


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
        
        list_objects = s3_manager.download_raw()
        
        empresas = athena_manager.get_empresas_data(list_objects)
        funcionarios = athena_manager.get_funcionarios_data(empresas)

        enriched_data = merge_data(list_objects, empresas, funcionarios)
        
        if len(enriched_data) == len(list_objects):

            upload_df = reglas_de_negocio(enriched_data, state='processed')

            s3_url = s3_manager.upload_processed(df = upload_df, state = 'processed')
            
            ses_manager = SESManager()

            delivery_df = reglas_de_negocio(enriched_data, state='delivery')

            s3_url = s3_manager.upload_processed(df = delivery_df, state = 'delivery')

            logger.info(f"DataFrame transformado y subido a S3: {s3_url}")

            email_sent = ses_manager.send_report(
                file=delivery_df)
            
            logger.info(f"Email sent: {email_sent}")


        response = {
            "statusCode": 200,
            "len_validation": f'extracted:{len(list_objects)} transformed:{len(enriched_data)}',
            "counts_match": len(enriched_data) == len(list_objects),
            "enriched_data": enriched_data.head(100).to_dict(orient='records'),
            "email_sent":str(email_sent),
            "body": json.dumps({
                "transformed_files": s3_url,
                "message": f"Se transformaron {len(s3_url)} de {len(uploaded_files)} archivos"
            })
        }
        
        return response
        
    except Exception as e:
        logger.exception("Error en lambda_handler de transformación")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }