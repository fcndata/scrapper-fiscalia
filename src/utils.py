from datetime import datetime,timedelta
import re
from config.config_loader import Config

def get_url_scrape(config: Config, url_key: str) -> str:
    """
    Construye la URL dinámica en función de la clave de URL y la fecha de ayer.
    """
    base_url = config.get(f"urls.{url_key}")  # <-- nota: en tu config es "urls", no "url"

    yesterday = datetime.now() - timedelta(days=1)
    dd = yesterday.strftime('%d')
    mm = yesterday.strftime('%m')
    yyyy = yesterday.strftime('%Y')

    if url_key == "url_1":
        print (f"{base_url}{dd}-{mm}-{yyyy}")
        return f"{base_url}{dd}-{mm}-{yyyy}"

    elif url_key == "url_2":
        return f"{base_url}{dd}/{mm}/{yyyy}"
    else:
        raise ValueError(f"URL key '{url_key}' no reconocida.")

def parse_total_expected(text: str) -> int:
    """
    Parsea el número total de registros desde el texto de la tabla.
    """
    match = re.search(r"en\s+(\d+)\s+registros", text)
    if match:
        total_expected = int(match.group(1))
        return total_expected
    else:
        raise ValueError(f"No se pudo parsear el número de registros desde el texto: '{text}'")

