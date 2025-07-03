import json
import pandas as pd
from typing import Dict, Any, Optional, List

from logs.logger import logger
from src.s3 import S3Manager
from src.athena import AthenaManager


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
        
        # Procesar cada archivo
        for file_path in uploaded_files:
            try:
                logger.info(f"Procesando archivo: {file_path}")
                
                # 1. Descargar archivo raw usando S3Manager
                s3_key = file_path.replace(f"s3://{s3_manager.bucket_name}/", "")
                local_raw_path = f"/tmp/raw_data.parquet"
                
                if not s3_manager.download_raw(s3_key, local_raw_path):
                    logger.error(f"Error al descargar archivo: {file_path}")
                    continue
                
                # Cargar datos raw
                raw_df = pd.read_parquet(local_raw_path)
                logger.info(f"Datos raw cargados: {len(raw_df)} registros")
                
                # Obtener lista de RUTs únicos
                ruts = raw_df['rut'].unique().tolist()
                
                # 2. Consultar tablas en Athena con filtros
                empresas_df = athena_manager.get_empresas_data(ruts)
                funcionarios_df = athena_manager.get_funcionarios_data(ruts)
                
                logger.info(f"Empresas encontradas: {len(empresas_df)}")
                logger.info(f"Funcionarios encontrados: {len(funcionarios_df)}")
                
                # 3. Lógica de transformación - Crear nuevo DataFrame
                transformed_df = raw_df.copy()
                
                # Unir con datos de empresas
                if not empresas_df.empty and 'rut_cliente' in empresas_df.columns:
                    empresas_df_renamed = empresas_df.rename(columns={'rut_cliente': 'rut'})
                    transformed_df = pd.merge(
                        transformed_df,
                        empresas_df_renamed,
                        on='rut',
                        how='left',
                        suffixes=('', '_empresa')
                    )
                
                # Unir con datos de funcionarios
                if not funcionarios_df.empty and 'rut_funcionario' in funcionarios_df.columns:
                    funcionarios_df_renamed = funcionarios_df.rename(columns={'rut_funcionario': 'rut'})
                    transformed_df = pd.merge(
                        transformed_df,
                        funcionarios_df_renamed,
                        on='rut',
                        how='left',
                        suffixes=('', '_funcionario')
                    )
                
                logger.info(f"Datos transformados: {len(transformed_df)} registros con {len(transformed_df.columns)} columnas")
                
                # 4. Guardar archivo transformado usando S3Manager
                filename = file_path.split('/')[-1].split('.')[0]
                local_processed_path = f"/tmp/{filename}_transformed.parquet"
                
                transformed_df.to_parquet(local_processed_path, index=False)
                
                # Subir usando upload_processed
                s3_url = s3_manager.upload_processed(local_processed_path, f"{filename}_transformed")
                
                if s3_url:
                    transformed_files.append(s3_url)
                    logger.info(f"Archivo transformado subido: {s3_url}")
                
            except Exception as e:
                logger.error(f"Error al procesar archivo {file_path}: {e}")
        
        # Preparar respuesta
        response = {
            "statusCode": 200,
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