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
        
        list_objects = s3_manager.download_raw()
        empresas = athena_manager.get_empresas_data(list_objects)
        funcionarios = athena_manager.get_funcionarios_data(empresas)

        def merge_data(list_objects, empresas, funcionarios):
            # Convertir list_objects a DataFrame
            raw_df = pd.DataFrame(list_objects)
            
            # Añadir un identificador único para rastrear filas originales
            raw_df['original_index'] = range(len(raw_df))
            
            # Asegurar que los tipos de datos sean compatibles para los joins
            if 'rut' in raw_df.columns:
                # Convertir rut a string para hacer el join
                raw_df['rut'] = raw_df['rut'].astype(str)
            
            # Convertir rut_cliente a string en empresas
            if 'rut_cliente' in empresas.columns:
                empresas['rut_cliente'] = empresas['rut_cliente'].astype(str)
            
            # Asegurar que los códigos de ejecutivo son del mismo tipo
            if 'ejec_cod' in empresas.columns:
                empresas['ejec_cod'] = empresas['ejec_cod'].astype(str)
            
            if 'ejc_cod' in funcionarios.columns:
                funcionarios['ejc_cod'] = funcionarios['ejc_cod'].astype(str)
            
            # Unir empresas con funcionarios
            aggregated_df = pd.merge(
                empresas,
                funcionarios,
                left_on='ejec_cod',
                right_on='ejc_cod',
                how='left')
            
            # Unir el resultado con raw_df
            final_df = pd.merge(
                aggregated_df,
                raw_df,
                left_on='rut_cliente',
                right_on='rut',
                how='right')
            
            # Verificar si hay duplicados en original_index
            duplicated_indices = final_df['original_index'].duplicated()
            if duplicated_indices.any():
                logger.warning(f"Se encontraron {duplicated_indices.sum()} filas duplicadas")
                # Eliminar duplicados manteniendo la primera ocurrencia
                final_df = final_df.drop_duplicates(subset=['original_index'])
            
            # Seleccionar columnas relevantes
            columns_to_keep = ['rut', 'rut_df', 'razon_social', 'url', 'actuacion', 'nro_atencion', 'cve',
                              'segmento', 'plataforma', 'ejec_cod', 'rut_funcionario', 'rut_funcionario_dv',
                              'nombre_funcionario', 'nombre_puesto', 'correo', 'dependencia',
                              'fecha', 'fecha_actuacion']
            
            # Filtrar solo las columnas que existen en el DataFrame
            final_columns = [col for col in columns_to_keep if col in final_df.columns]
            final_df = final_df[final_columns]

            return final_df
        final_df = merge_data(list_objects, empresas, funcionarios)
        len_final_df = len(final_df)
        len_objects = len(list_objects)
        
        # Verificar si los conteos coinciden
        if len_final_df != len_objects:
            logger.warning(f"Discrepancia en conteos: {len_objects} registros originales vs {len_final_df} en el resultado final")
        
        response = {
            "statusCode": 200,
            "len_validation": f'extracted:{len_objects} transformed:{len_final_df}',
            "counts_match": len_final_df == len_objects,
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