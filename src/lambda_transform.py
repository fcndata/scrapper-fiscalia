import json
from typing import Dict, Any, Optional, List

from logs.logger import logger
from src.s3 import S3Manager
from src.athena import AthenaManager
from src.utils import enrich_company_data, filter_enriched_by_sufs, reglas_de_negocio
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
        
        companies = s3_manager.download_raw()
        
        # Extraer RUTs para consultas
        ruts = [comp.rut for comp in companies if comp.rut]
        
        # Obtener datos de enriquecimiento
        empresas = athena_manager.get_empresas_data(ruts)
        funcionarios = athena_manager.get_funcionarios_data(empresas)
        sufs = athena_manager.get_sufs_data(ruts)
        
        # Enriquecer datos
        enriched_df = enrich_company_data(companies, empresas, funcionarios)
        
        # Filtrar por SUFs
        filter_by_suf = filter_enriched_by_sufs(enriched_df, sufs)
        
        # Aplicar reglas de negocio

        delivery_df = reglas_de_negocio(filter_by_suf, state='processed')
        
        # Subir archivos procesados
        processed_url = s3_manager.upload_processed(df=delivery_df, state='processed')
        delivery_url = s3_manager.upload_processed(df=delivery_df, state='delivery')
        
        # Enviar reporte
        ses_manager = SESManager()
        email_sent = ses_manager.send_report(file=delivery_df)

        logger.info(f"Procesados: {processed_url}, Delivery: {delivery_url}")
        logger.info(f"Email enviado: {email_sent}")


        response = {
            "statusCode": 200,
            "len_validation": f'extracted:{len(companies)} enriched:{len(enriched_df)} filtered:{len(final_df)}',
            "sufs_filter_applied": len(final_df) < len(enriched_df),
            "sample_data": final_df.head(5).to_dict(orient='records'),
            "email_sent":str(email_sent),
            "body": json.dumps({
                "processed_file": processed_url,
                "delivery_file": delivery_url,
                "message": f"Procesadas {len(final_df)} empresas con filtro SUFs"
            })
        }
        
        return response
        
    except Exception as e:
        logger.exception("Error en lambda_handler de transformación")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }