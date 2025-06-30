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


def extract_metadata(row: Tag) -> Tuple[str, str, str, str]:
    """
    Extrae los metadatos de una fila de la tabla del Diario Oficial.
    
    Args:
        row: Elemento BeautifulSoup que representa una fila de la tabla.
        
    Returns:
        Tupla con (rut, razon_social, url_pdf, cve).
        
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
    rut = rut_div.text.strip() if rut_div else None
    if not rut:
        raise ValueError("No se pudo extraer el RUT")
    
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

    return rut.replace('*','').replace('.',''), razon_social, url_pdf, cve
    

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
