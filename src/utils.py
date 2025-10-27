import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from src.models import CompanyMetadata, EmpresaData, FuncionarioData, EnrichedCompanyData, SufData
else:
    from src.models import EnrichedCompanyData

import pandas as pd
from bs4 import BeautifulSoup, Tag

from config import config
from logs.logger import logger
from src.business_rules import (BusinessRuleEngine, DateFormatRule, CleanNumberRule, 
                                ExcludeValueRule, NotNullRule, ColumnOrderRule)


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

def get_date_update() -> str:
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return str(yesterday)

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
    numeros = re.findall(r"\d+(?:[.,]\d+)*", text)
    
    if not numeros:
        raise ValueError(f"No se pudo parsear el número de registros desde el texto: '{text}'")
    
    valores = [int(n.replace('.', '').replace(',', '')) for n in numeros]
    return max(valores)

def extract_metadata(row: Tag) -> Tuple[int, str, str, str, str]:
    """
    Extrae los metadatos de una fila de la tabla del Diario Oficial.
    
    Args:
        row: Elemento BeautifulSoup que representa una fila de la tabla.
        
    Returns:
        Tupla con (rut_int, dv_str, razon_social, url_pdf, cve).
        
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
        number_part = int(number_part.replace('.', ''))
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
    
def query_empresas(rut_list: List[int]) -> str:
    """
    Genera una consulta SQL para obtener datos de empresas basado en una lista de RUTs.
    
    Args:
        rut_list: Lista de RUTs para filtrar empresas.
        
    Returns:
        Consulta SQL para obtener datos de empresas.
    """   
    if not rut_list:
        logger.warning("No se encontraron RUTs válidos para consultar empresas")
        return "SELECT rut_cliente, rut_cliente_dv, segmento, plataforma, ejec_cod, fecha_proceso FROM \"bd_in_tablas_generales\".\"tbl_maestro_empresas\" WHERE 1=0"
        
    logger.info(f"Consultando datos para {len(rut_list)} RUTs válidos")
    
    # Formatear la lista de RUTs para la consulta SQL
    rut_str = f"({', '.join(map(str, rut_list))})"
        
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
                WHERE rut_cliente IN {rut_str}
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

def query_funcionarios(ejec_codes: List[int]) -> str:
    """
    Genera una consulta SQL para obtener datos de funcionarios basado en códigos de ejecutivo.
    
    Args:
        ejec_codes: Lista de códigos de ejecutivo para filtrar funcionarios.
        
    Returns:
        Consulta SQL para obtener datos de funcionarios.
    """
    if not ejec_codes:
        logger.warning("No se proporcionaron códigos de ejecutivo")
        return "SELECT rut_funcionario, rut_funcionario_dv, nombre_funcionario, nombre_puesto, correo, dependencia, fecha_carga_dl, ejc_cod FROM \"bd_dlk_bcc_tablas_generales\".\"tbl_base_funcionarios\" WHERE 1=0"
    
    
    formatted_codes = [f"'{str(code)}'" for code in ejec_codes if code is not None]

    ejec_list = "(" + ", ".join(formatted_codes) + ")"
    
    custom_query = f'''       
            WITH EjecutivosRUT AS (
                SELECT 
                    ejc_cod,
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

def query_sufs(rut_list: List[int]) -> str:
    """
    Genera consulta SQL para obtener RUTs únicos de la tabla SUFs.
    
    Returns:
        Consulta SQL para obtener RUTs de SUFs.
    """
    if not rut_list:
        logger.warning("No se proporcionaron RUTs")
        return "SELECT DISTINCT cli_rut FROM \"bd_in_gesdatos\".\"tbl_tsuf_pcp\" WHERE 1=0"
    
    rut_str = f"({', '.join(map(str, rut_list))})"

    return f'SELECT DISTINCT cli_rut, cli_rut_dv, fecha_proceso FROM "bd_in_gesdatos"."tbl_tsuf_pcp" WHERE cli_rut IN {rut_str}'

def filter_enriched_by_sufs(enriched_objects: List['EnrichedCompanyData'], sufs_data: List['SufData']) -> pd.DataFrame:
    """
    Filtra DataFrame enriquecido manteniendo solo RUTs que están en SUFs.
    
    Args:
        enriched_df: DataFrame con datos enriquecidos.
        sufs_data: Lista de objetos SufData desde Athena.
        
    Returns:
        DataFrame filtrado que solo contiene RUTs presentes en SUFs.
    """
    # Crear set de RUTs válidos desde SUFs para lookup O(1)
    valid_ruts = {suf.cli_rut for suf in sufs_data}
    
    # Filtrar objetos por RUTs válidos
    filtered_objects = [obj for obj in enriched_objects if obj.rut in valid_ruts]
    
    # Convertir a DataFrame
    filtered_data = [obj.model_dump() for obj in filtered_objects]
    filtered_df = pd.DataFrame(filtered_data)
    
    logger.info(f"Filtradas {len(filtered_df)} filas de {len(enriched_objects)} usando SUFs")
    return filtered_df


def enrich_company_data(companies: List['CompanyMetadata'], empresas: List['EmpresaData'], funcionarios: List['FuncionarioData']) -> List['EnrichedCompanyData']:
    """
    Combina datos de empresas con información corporativa y de funcionarios.
    
    Args:
        companies: Lista de objetos CompanyMetadata extraídos.
        empresas: Lista de objetos EmpresaData desde Athena.
        funcionarios: Lista de objetos FuncionarioData desde Athena.
        
    Returns:
        DataFrame con datos combinados.
    """
    # Crear diccionarios de lookup con serialización
    data_empresas = {}
    for empresa in empresas:
        try:
            serialized = empresa.model_dump(mode='json')
            data_empresas[serialized['rut_cliente']] = serialized
        except AttributeError as e:
            logger.error(f"Error serializando empresa {empresa.rut_cliente}: {e} el type es {type(empresa)} y el contenido es {empresa.__dict__}")

    data_funcionarios = {}
    for funcionario in funcionarios:
        try:    
            serialized = funcionario.model_dump(mode='json')
            data_funcionarios[serialized['ejc_cod']] = serialized
        except AttributeError as e:
            logger.error(f"Error serializando funcionario {funcionario.rut_funcionario}: {e} el type es {type(funcionario)} y el contenido es {funcionario.__dict__}")

    enriched_data = []
    
    for company in companies:
        # Serializar datos originales
        try:
            company_data = company.model_dump(mode='json')
        except AttributeError as e:
            logger.error(f"Error serializando empresa {company.rut}: {e} el type es {type(company)} y el contenido es {company.__dict__}")
            continue

        # Buscar datos de empresa
        empresa_data = data_empresas.get(company_data['rut']) if company_data['rut'] else None
        
        # Buscar datos de funcionario usando ejec_cod de empresa
        funcionario_data = None
        if empresa_data and empresa_data['ejec_cod']:
            funcionario_data = data_funcionarios.get(empresa_data['ejec_cod'])
        
        # Combinar todos los datos
        enriched = {**company_data}
        if empresa_data:
            enriched.update({k: v for k, v in empresa_data.items() if k not in enriched})
        if funcionario_data:
            enriched.update({k: v for k, v in funcionario_data.items() if k not in enriched})
        
        enriched_data.append(enriched)
    
    # Crear objetos EnrichedCompanyData
    enriched_objects = []
    for data in enriched_data:
        try:
            enriched_obj = EnrichedCompanyData(**data)
            enriched_objects.append(enriched_obj)
        except Exception as e:
            logger.warning(f"Error creando EnrichedCompanyData: {e}")
            continue
    
    logger.info(f"Enriquecidos {len(enriched_objects)} registros de {len(companies)} originales")
    return enriched_objects
    

def reglas_de_negocio(data: pd.DataFrame, state: str = 'processed') -> pd.DataFrame:
    """
    Aplica reglas de negocio estandarizadas a un DataFrame.

    Args:
        data: DataFrame de pandas.
        state: Estado del procesamiento ('processed' o 'delivery').

    Returns:
        DataFrame con las reglas de negocio aplicadas.
    """
    df = data.copy()
    
    # Crear motor de reglas
    engine = BusinessRuleEngine()
    
    # Reglas de formato
    engine.add_rule(DateFormatRule(['fecha_actuacion', 'pa_date']))
    engine.add_rule(CleanNumberRule(['nro_atencion']))
    engine.add_rule(ExcludeValueRule('actuacion', ['CONSTITUCIÓN']))
    engine.add_rule(NotNullRule(['segmento', 'rut']))

    if state == 'processed':
        # Reglas de filtrado
        columns_to_keep = config.get("columns.all")
        engine.add_rule(ColumnOrderRule(columns_to_keep))

    if state == 'gobierno':
        # Reglas específicas para gobierno
        columns_to_keep = config.get("columns.gobierno")
        engine.add_rule(ColumnOrderRule(columns_to_keep))
        

    return engine.apply_all(df)