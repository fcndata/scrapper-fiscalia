from datetime import datetime,timedelta
import re
from config.config_loader import Config
from pathlib import Path
import pandas as pd
import json
from src.models import CompanyMetadata

def get_url_scrape(config: Config, url_key: str) -> str:
    """
    Construye la URL dinámica en función de la clave de URL y la fecha de ayer.
    """
     
    base_url = config.get(f"urls.{url_key}")

    yesterday = datetime.now() - timedelta(days=1)
    dd = yesterday.strftime('%d')
    mm = yesterday.strftime('%m')
    yyyy = yesterday.strftime('%Y')

    return f"{base_url}{dd}-{mm}-{yyyy}"

def get_date_update() -> datetime:
    """
    Obtiene la fecha actual.
    """
    yesterday = datetime.now() - timedelta(days=1)

    return yesterday
    
def parse_total_expected(text: str) -> int:
    """
    Parsea el número total de registros desde el texto de la tabla.
    """
    match = re.search(r"en\s+([\d,.]+)\s+registros", text)
    if match:
        total_expected = int(match.group(1).replace(",", "").replace(".", ""))
        return total_expected
    else:
        raise ValueError(f"No se pudo parsear el número de registros desde el texto: '{total_expected}'")

def extract_metadata(row):
    """
    Extrae los metadatos de una fila de la tabla.
    Esta función debe ser implementada según la estructura de la fila.
    """
    cols = row.find_all('td')

    razon_social_div = cols[0].find('div', style=lambda x: x and 'float:left' in x)
    razon_social = razon_social_div.text.strip() if razon_social_div else None

    rut_div = cols[0].find('div', style=lambda x: x and 'float:right' in x)
    rut = rut_div.text.strip() if rut_div else None
    
    link = cols[1].find('a')

    url_pdf = link['href']
    text_cve = link.text.strip() if link else None

    match = re.search(r'CVE-(\d+)', text_cve)
    
    cve = match.group(1) if match else None

    return rut.replace('*','').replace('.',''), razon_social, url_pdf, cve
    
def return_metadata() -> pd.DataFrame:
    """
    Lee los archivos JSONL de 'diario_scraper' y 'empresa_scraper',
    parsea cada línea a CompanyMetadata y devuelve un DataFrame.
    """
    paths = [
        Path("data/diario_scraper.jsonl"),
        Path("data/empresa_scraper.jsonl"),
    ]

    registros = []
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(f"No se encontró el archivo {p}")
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:

                    modelo = CompanyMetadata.parse_obj(json.loads(line))
                    registros.append(modelo.dict())
                except Exception as e:
                    # Aquí podrías loguear o recolectar errores de parsing
                    print(f"[Warning] Falló parseo en {p}: {e}")

    # Convertimos la lista de dicts a DataFrame
    df = pd.DataFrame.from_records(registros)
    return df