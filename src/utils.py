from datetime import datetime,timedelta
import re
from config.config_loader import Config

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
    

