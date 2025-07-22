import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional, Union

import pandas as pd
from bs4 import BeautifulSoup, Tag

from config import config
from logs.logger import logger


def get_url_scrape(url_key: str) -> str:
    """
    Construye la URL dinámica en función de la clave de URL y la fecha de ayer.
    
    Args:
        url_key: Clave de la URL en el archivo de configuración.
        
    Returns:
        URL completa con la fecha de ayer formateada.
        
    Raises:
        ValueError: Si la clave de URL no existe en la configuración.
    """
    base_url = config.get(f"urls.{url_key}")
    if not base_url:
        raise ValueError(f"La clave de URL '{url_key}' no existe en la configuración")

    yesterday = datetime.now() - timedelta(days=1)
    dd = yesterday.strftime('%d')
    mm = yesterday.strftime('%m')
    yyyy = yesterday.strftime('%Y')

    return f"{base_url}{dd}-{mm}-{yyyy}"


def get_date_update() -> datetime:
    """
    Obtiene la fecha de ayer para usar en los registros.
    
    Returns:
        Objeto datetime con la fecha de ayer.
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday
    

def parse_total_expected(text: str) -> int:
    """
    Parsea el número total de registros desde el texto de la tabla.
    
    Args:
        text: Texto que contiene la información sobre el número de registros.
        
    Returns:
        Número total de registros esperados.
        
    Raises:
        ValueError: Si no se puede extraer el número de registros del texto.
    """
    match = re.search(r"en\s+([\d,.]+)\s+registros", text)
    if match:
        total_expected = int(match.group(1).replace(",", "").replace(".", ""))
        return total_expected
    else:
        raise ValueError(f"No se pudo parsear el número de registros desde el texto: '{text}'")


def extract_metadata(row: Tag) -> Tuple[str, str, str, str, str]:
    """
    Extrae los metadatos de una fila de la tabla del Diario Oficial.
    
    Args:
        row: Elemento BeautifulSoup que representa una fila de la tabla.
        
    Returns:
        Tupla con (number_part, dv_part, razon_social, url_pdf, cve).
        
    Raises:
        ValueError: Si no se puede extraer alguno de los campos requeridos.
    """
    cols = row.find_all('td')
    if len(cols) < 2:
        raise ValueError(f"La fila no tiene suficientes columnas: {len(cols)}")

    # Extraer razón social
    razon_social_div = cols[0].find('div', style=lambda x: x and 'float:left' in x)
    razon_social = razon_social_div.text.strip() if razon_social_div else None
    if not razon_social:
        raise ValueError("No se pudo extraer la razón social")

    # Extraer RUT
    rut_div = cols[0].find('div', style=lambda x: x and 'float:right' in x)
    
    raw_rut = rut_div.text.strip().replace('*','') if rut_div else None
    
    if raw_rut:
        number_part, dv_part = raw_rut.split('-')
        number_part = number_part.replace('.', '')
    else:
        number_part, dv_part = None, None
    
    # Extraer URL y CVE
    link = cols[1].find('a')
    if not link:
        raise ValueError("No se encontró el enlace al documento")

    url_pdf = link.get('href')
    if not url_pdf:
        raise ValueError("No se pudo extraer la URL del PDF")
        
    text_cve = link.text.strip() if link else None
    match = re.search(r'CVE-(\d+)', text_cve) if text_cve else None
    cve = match.group(1) if match else None
    if not cve:
        raise ValueError("No se pudo extraer el CVE")

    return number_part,dv_part, razon_social, url_pdf, cve
    

def jsonl_to_parquet(jsonl_path: str, parquet_path: str) -> bool:
    """
    Convierte un archivo JSONL a formato Parquet.
    
    Args:
        jsonl_path: Ruta al archivo JSONL de origen.
        parquet_path: Ruta donde se guardará el archivo Parquet.
        
    Returns:
        True si la conversión fue exitosa, False en caso contrario.
        
    Raises:
        No lanza excepciones directamente, captura y registra errores internamente.
    """
    try:
        records: List[Dict[str, Any]] = []
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line.strip()))
        
        if not records:
            logger.warning(f"No se encontraron registros en {jsonl_path}")
            return False
        
        df = pd.DataFrame(records)
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Archivo {jsonl_path} convertido a {parquet_path} con {len(records)} registros")
        return True
        
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Error al leer o decodificar {jsonl_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error al convertir {jsonl_path} a parquet: {e}")
        return False

def query_empresas(list_objects: List[Dict[str, Any]]) -> str:
    """
    Genera una consulta SQL para obtener datos de empresas basado en una lista de objetos.
    
    Args:
        list_objects: Lista de diccionarios con datos de empresas, cada uno debe tener una clave 'rut'.
        
    Returns:
        Consulta SQL para obtener datos de empresas. Si no hay RUTs válidos, devuelve una consulta
        que no retornará resultados.
    """
    rut_empresas = []
    for item in list_objects:
        rut = item.get('rut')
        if rut:
            try:
                rut_empresas.append(int(rut))
            except (ValueError, TypeError):
                logger.warning(f"RUT no válido para conversión a entero: {rut}")
    
    # Si no hay RUTs válidos, devolver una consulta que no retornará resultados
    if not rut_empresas:
        logger.warning("No se encontraron RUTs válidos para consultar empresas")
        return "SELECT rut_cliente, rut_cliente_dv, segmento, plataforma, ejec_cod, fecha_proceso FROM \"bd_in_tablas_generales\".\"tbl_maestro_empresas\" WHERE 1=0"

    # Formatear la lista de RUTs para la consulta SQL
    rut_list = f"({', '.join(map(str, rut_empresas))})"
    
    logger.info(f"Consultando datos para {len(rut_empresas)} RUTs válidos")
    
    custom_query = f'''
            WITH RankedEmpresas AS (
                SELECT 
                    rut_cliente,
                    rut_cliente_dv,
                    segmento,
                    plataforma,
                    ejec_cod,
                    fecha_proceso,
                    ROW_NUMBER() OVER (PARTITION BY rut_cliente ORDER BY fecha_proceso DESC) as rn
                FROM "bd_in_tablas_generales"."tbl_maestro_empresas"
                WHERE rut_cliente IN {rut_list}
                )
                SELECT 
                rut_cliente,
                rut_cliente_dv,
                segmento,
                plataforma,
                ejec_cod,
                fecha_proceso
                FROM RankedEmpresas
                WHERE rn = 1
            '''
    return custom_query

def query_funcionarios(ejec_code: List) -> str:
    """
    Genera una consulta SQL para obtener datos de funcionarios basado en los códigos de ejecutivo
    de las empresas.
    
    Args:
        ejec_code: Lista de códigos de ejecutivo para filtrar los funcionarios.
        
    Returns:
        Consulta SQL para obtener datos de funcionarios. Si no hay códigos de ejecutivo válidos,
        devuelve una consulta que no retornará resultados.
    """
    # Asegurar que todos los códigos son strings
    ejec_code_str = [str(code) for code in ejec_code if code is not None]
    
    # Si todos los códigos eran None, devolver una consulta vacía
    if not ejec_code_str:
        logger.warning("Todos los códigos de ejecutivo eran nulos")
        return "SELECT rut_funcionario, rut_funcionario_dv, nombre_funcionario, nombre_puesto, correo, dependencia, fecha_carga_dl, ejc_cod FROM \"bd_dlk_bcc_tablas_generales\".\"tbl_base_funcionarios\" WHERE 1=0"
    
    # Formatear la lista de códigos de ejecutivo para la consulta SQL - con comillas para tipo varchar
    ejec_list = f"({', '.join([f"'{code}'" for code in ejec_code_str])})"
    
    # Añadir logging detallado para depuración
    logger.info(f"Buscando funcionarios para {len(ejec_code_str)} códigos de ejecutivo válidos")
    
    custom_query = f'''       
            WITH EjecutivosRUT AS (
                SELECT 
                    ejc_cod,
                    -- Convertir ejc_rut a string y eliminar ceros iniciales
                    TRIM(LEADING '0' FROM CAST(ejc_rut AS VARCHAR)) AS ejc_rut_trim
                FROM "bd_dlk_bcc_tablas_generales"."tbl_codigo_ejecutivo"
                WHERE ejc_cod IN {ejec_list}
            ),
            RankedFuncionarios AS (
                SELECT 
                    f.rut_funcionario,
                    f.rut_funcionario_dv,
                    f.nombre_funcionario,
                    f.nombre_puesto,
                    f.correo,
                    f.dependencia,
                    f.fecha_carga_dl,
                    e.ejc_cod,
                    ROW_NUMBER() OVER (PARTITION BY f.rut_funcionario ORDER BY f.fecha_carga_dl DESC) as rn
                FROM "bd_dlk_bcc_tablas_generales"."tbl_base_funcionarios" f
                JOIN EjecutivosRUT e ON CAST(f.rut_funcionario AS VARCHAR) = e.ejc_rut_trim
            )
            SELECT 
                rut_funcionario,
                rut_funcionario_dv,
                nombre_funcionario,
                nombre_puesto,
                correo,
                dependencia,
                fecha_carga_dl,
                ejc_cod
            FROM RankedFuncionarios
            WHERE rn = 1
            '''
    return custom_query


def merge_data(list_objects, empresas, funcionarios):

        raw_df = pd.DataFrame(list_objects)
        raw_df['original_index'] = range(len(raw_df))

        if 'rut' in raw_df.columns:
            raw_df['rut'] = raw_df['rut'].astype(str)
        
        if 'rut_cliente' in empresas.columns:
            empresas['rut_cliente'] = empresas['rut_cliente'].astype(str)
        
        if 'ejec_cod' in empresas.columns:
            empresas['ejec_cod'] = empresas['ejec_cod'].astype(str)
        
        if 'ejc_cod' in funcionarios.columns:
            funcionarios['ejc_cod'] = funcionarios['ejc_cod'].astype(str)

        metadata = pd.merge(
            empresas,
            funcionarios,
            left_on='ejec_cod',
            right_on='ejc_cod',
            how='left')

        final_df = pd.merge(
            metadata,
            raw_df,
            left_on='rut_cliente',
            right_on='rut',
            how='right')
        
        duplicated_indices = final_df['original_index'].duplicated()
        if duplicated_indices.any():
            logger.warning(f"Se encontraron {duplicated_indices.sum()} filas duplicadas")
            final_df = final_df.drop_duplicates(subset=['original_index'])

        columns_to_keep = ['fuente','rut', 'rut_df', 'razon_social', 'url', 'actuacion', 'nro_atencion', 'cve',
                            'segmento', 'plataforma', 'ejec_cod', 'rut_funcionario', 'rut_funcionario_dv',
                            'nombre_funcionario', 'nombre_puesto', 'correo', 'dependencia',
                            'fecha', 'fecha_actuacion']

        final_columns = [col for col in columns_to_keep if col in final_df.columns]
        final_df = final_df[final_columns]

        return final_df