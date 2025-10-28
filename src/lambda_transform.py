import json
from typing import Dict, Any, Optional, List

from logs.logger import logger
from src.s3 import S3Manager
from src.athena import AthenaManager
from src.utils import enrich_company_data, filter_enriched_by_sufs, reglas_de_negocio
from src.simple_notification_service import SNSManager


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
        enriched_objects = enrich_company_data(companies, empresas, funcionarios)
        
        # Filtrar por SUFs
        df_filtered_by_suf = filter_enriched_by_sufs(enriched_objects, sufs)
        
        # Aplicar reglas de negocio
        processed_df = reglas_de_negocio(df_filtered_by_suf, state='processed')

        # Subir archivos procesados
        processed_url = s3_manager.upload_processed(df=processed_df, state='processed')

        gobierno_df = reglas_de_negocio(df_filtered_by_suf, state='gobierno')

        gobierno_url = s3_manager.upload_gobierno(df=gobierno_df)

        logger.info(f"Procesados: {processed_url}")
        logger.info(f"Gobierno: {gobierno_url}")
        
        sns_manager = SNSManager()
        business_email_sent = sns_manager.send_business_report()

        logger.info(f"Email enviado: {business_email_sent}")

        log_data = {'data_extracted': len(companies), 
            'data_enriched': len(enriched_objects), 
            'data_filtered': len(df_filtered_by_suf),
            "message": f"Procesadas {len(processed_df)} empresas con filtro SUFs",
            "message_business": str(business_email_sent)
            }
        
        sns_manager.send_logs_report(log_data)
        response = {
            "statusCode": 200,
            "len_validation": f'extracted:{len(companies)} enriched:{len(enriched_objects)} filtered:{len(df_filtered_by_suf)}',
            "sufs_filter_applied": len(df_filtered_by_suf) < len(enriched_objects),
            "sample_data": processed_df.head(3).to_dict(orient='records'),
            "gobierno_url": str(business_email_sent),
            "body": json.dumps({
                "processed_file": processed_url,
                "gobierno_file": gobierno_url,
                "message": f"Procesadas {len(processed_df)} empresas con filtro SUFs"
            })
        }
        
        return response
        
    except Exception as e:
        logger.exception("Error en lambda_handler de transformación")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }