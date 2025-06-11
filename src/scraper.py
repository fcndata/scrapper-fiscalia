from config.config_loader import Config
from pydantic import Field
from abc import abstractmethod
from pathlib import Path
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import os
import json
import logging
from logs.logger import logger
from src.models import CompanyMetadata
from src.utils import get_url_scrape
from src.utils import parse_total_expected
from src.utils import get_date_update, extract_metadata

class BrowserSession:
    def __init__(self, config: Config ):  

        self.config = config

        # Levantar parametros de config
        self.headless = self.config.get("scraper.headless", True)
        self.user_agent = self.config.get("scraper.user_agent", "Mozilla/5.0 ...")
        self.verbose = self.config.get("scraper.verbose", False)

        # Paths de binarios desde ENV
        self.chromedriver_path = Path(os.getenv("CHROMEDRIVER_BIN"))
        self.chrome_bin = Path(os.getenv("CHROME_BIN"))
        
        if not self.chromedriver_path.exists():
            raise FileNotFoundError(f"El archivo chromedriver no se encuentra en la ruta: {self.chromedriver_path}")

        self.driver = None

    def init_driver(self) -> webdriver.Chrome:
        """
        Inicializa el navegador con las opciones configuradas.
        """
        logger.info("Iniciando Webdriver con configs.")

        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        options.add_argument(f"user-agent={self.user_agent}")
        options.binary_location = str(self.chrome_bin)

        service = ChromeService(executable_path=str(self.chromedriver_path))
        
        self.driver = webdriver.Chrome(service=service, options=options)
        
        return self.driver

    def __enter__(self):
        """
        Context manager entry point to initialize the driver.
        """
        self.driver = self.init_driver()
        return self.driver
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit point to close the driver.
        """
        if self.driver:
            self.driver.quit()
            logger.info("Webdriver cerrado correctamente.")
        else:
            logger.warning("No se pudo cerrar el Webdriver porque no estaba inicializado.")
    
class BaseScraper:
    def __init__(self, driver: webdriver.Chrome, config: Config, logger: logging.Logger = logger):
        self.driver = driver
        self.config = config
        self.logger = logger
    
    def load_page(self, url: str):
        """
        Carga la página web en el navegador.
        """
        self.logger.info(f"Cargando la página de {url}")
        dynamic_url = get_url_scrape(self.config, url)
        self.logger.info(f"URL dinámica construida: {dynamic_url}")
               
        self.driver.get(dynamic_url)
        self.driver.implicitly_wait(6)

        return self.driver

    @abstractmethod
    def extract_data(self) -> list:
        """    Extrae las filas de la tabla (TRs).    """
        pass

    @abstractmethod
    def extract_serialization(self, rows) -> list[CompanyMetadata]:
        """
        Parsea las filas (TRs) y construye la lista de CompanyMetadata.
        """
        pass
    
    @abstractmethod
    def validation(self,url_key ,data_objects) -> bool:
        """
        Realiza validaciones previas al scraping.
        """
        pass

    def save_data(self, data_objects: list[CompanyMetadata], output_path: str) -> None:
        """
        Guarda los datos serializados en formato JSONL.
        Cada línea del archivo es un JSON individual.
        """
        self.logger.info(f"Guardando {len(data_objects)} registros en '{output_path}'...")

        with open(output_path, "a", encoding="utf-8") as f:
            for obj in data_objects:
                json_line = json.dumps(obj.serialize(), ensure_ascii=False)
                f.write(json_line + "\n")

        self.logger.info(f"Se guardaron {len(data_objects)} registros en '{output_path}'.")
    
    def run(self, url_key: str) -> list[CompanyMetadata]:
        """
        Ejecuta el flujo completo de scraping y serialización.
        """
        self.logger.info("Iniciando proceso de scraping.")

        url_base = self.config.get(f"urls.{url_key}")

        if not url_base:
            self.logger.error(f"No se encontró URL para la key: {url_key}")
            raise ValueError(f"URL no encontrada en config para key: {url_key}")

        # 2️⃣ Cargar la página
        self.load_page(url_key)

        # 3️⃣ Extraer las filas
        rows = self.extract_data()

        # 4️⃣ Serializa las filas a objetos CompanyMetadata
        data_objects = self.extract_serialization(rows)

        self.logger.info(f"Proceso completo. Total registros: {len(data_objects)}.")

        return data_objects
        
    def trigger(self, url_key: str, output_path: str) -> None:
        """
        Ejecuta el flujo completo: extracción, validación y guardado.
        """
        self.logger.info(f"===> Iniciando trigger de scraping para URL key: {url_key}")

        url_base = self.config.get(f"urls.{url_key}")

        if not url_base:
            self.logger.error(f"No se encontró URL para la key: {url_key}")
            raise ValueError(f"URL no encontrada en config para key: {url_key}")

        # Carga los datos serializados
        data_objects = self.run(url_key)

        self.logger.info(f"Total registros extraídos: {len(data_objects)}.")

        # Valida
        is_valid = self.validation(url_key, data_objects)

        if is_valid:
            # Guardar en formato JSONL
            self.save_data(data_objects, output_path)
            self.logger.info(f" Trigger finalizado con éxito. Archivo guardado: {output_path}")
        else:
            self.logger.error(f" Validación fallida. No se guardará el archivo '{output_path}'.")

    def close(self):
        """
        Cierra el driver del navegador y libera recursos.
        """
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Webdriver cerrado correctamente desde close().")
            except Exception as e:
                self.logger.error(f"Error al cerrar el Webdriver: {e}")
        else:
            self.logger.warning("Webdriver no estaba inicializado. Nada que cerrar.")

class SociedadScraper(BaseScraper):
    def extract_data(self) -> list:
        """    Extrae las filas de la tabla (TRs).    """
        self.logger.info(f"Extrayendo filas de la tabla... {self.driver.current_url}")

        # Expandir tabla
        select = Select(self.driver.find_element(By.XPATH, '//select[@name="tblSociedades_length"]'))
        select.select_by_value("-1")

        # Obtener HTML de la tabla
        html = self.driver.find_element(By.XPATH, '//*[@id="tblSociedades"]').get_attribute('outerHTML')

        if not html:
            self.logger.error("No se pudo cargar la tabla de sociedades.")
            raise ValueError("Tabla de sociedades no encontrada en la página.")

        # Parsear HTML con BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('tbody')
        rows = table.find_all('tr', attrs={'role': 'row'})

        self.logger.info(f"Se encontraron {len(rows)} filas en la tabla.")
        return rows

    def extract_serialization(self, rows) -> list[CompanyMetadata]:
        """
        Parsea las filas (TRs) y construye la lista de CompanyMetadata.
        """
        self.logger.info("Serializando las filas...")

        data_objects = []
        for row in rows:
            cols = [td.text.strip() for td in row.find_all('td')]

            # Asegúrate de que el número de columnas corresponde al modelo
            if not cols or len(cols) != 6:
                self.logger.error(f"Estructura inesperada de la tabla: {cols}")
                raise ValueError("Cambio en la estructura de la tabla, debe revisarse!")

            # Construcción del objeto
            obj = CompanyMetadata(
                rut=cols[2],
                razon_social=cols[4],
                url=self.driver.current_url,
                actuacion=cols[1],
                nro_atencion=cols[3],
                cve=cols[5],
                fecha_actuacion = datetime.datetime.strptime(cols[0].strip(), "%d-%m-%Y")
            )

            data_objects.append(obj)

        self.logger.info(f"Se serializaron {len(data_objects)} registros.")

        return data_objects

    def validation(self,url_key ,data_objects) -> bool:
        """
        Realiza validaciones previas al scraping.
        """
        self.logger.info("Realizando validaciones previas al scraping...")

        html = self.driver.find_element(By.XPATH, '//*[@id="tblSociedades_info"]').text

        total_expected = parse_total_expected(html)
        total_extracted = len(data_objects) 

        if total_expected == total_extracted:
            return True
        else:
            self.logger.info(f"Validando URL key: {url_key} — Esperados: {total_expected}, Extraídos: {total_extracted}")

            return False

class DiarioScraper(BaseScraper):
    
    def extract_data(self) -> list:
        """
        Extrae todas las secciones <section class="norma_general"> directamente de la página principal.
        """
        self.logger.info(f"Extrayendo filas de la tabla... {self.driver.current_url}")
        
        if "select_edition" in self.driver.current_url:
                WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, "index.php?date=")]'))).click()

        try:
            # Expandir tabla
            WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, "empresas_cooperativas.php")]'))).click()

            # Verificar si existe <p class="nofound">
            soup_page = BeautifulSoup(self.driver.page_source, 'html.parser')
            nofound = soup_page.find('p', class_='nofound')
            if nofound:
                self.logger.info(f" No hay publicaciones para esta edición ({self.driver.current_url}): '{nofound.text.strip()}'")
                return []   

            # 2️⃣ Si hay publicaciones → obtener tbody
            html = self.driver.find_element(By.XPATH, "//tbody").get_attribute('outerHTML')

            # Parsear con BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            rows = soup.find_all("tr")

            self.logger.info(f"Se encontraron {len(rows)} filas en la tabla.")
            return rows

        except Exception as e:
            self.logger.error(f" Error inesperado en {self.driver.current_url}: {e}")
            raise ValueError(f"No se pudo extraer la tabla de sociedades cooperativas en {self.driver.current_url}.")

    def extract_serialization(self, rows) -> list[CompanyMetadata]:
        data_objects = []
        current_actuacion = None  # Contexto que vamos actualizando
        actuacion_counter = {}

        for row in rows:
            # Caso 1️⃣ → Es fila "contextual" → revisar si hay title3
            if not row.has_attr('class'):
                td_title3 = row.find('td', class_='title3')
                if td_title3:
                    current_actuacion = td_title3.text.strip()
                    if current_actuacion not in actuacion_counter.keys():
                        actuacion_counter[current_actuacion] = 0

            # Caso 2️⃣ → Es fila de contenido
            elif 'content' in row.get('class', []):

                rut_number,razon,url_pdf,cve_number = extract_metadata(row)
                
                obj = CompanyMetadata(
                    rut=rut_number, 
                    razon_social=razon,
                    url=url_pdf,
                    actuacion=current_actuacion,
                    nro_atencion=None,  
                    cve=cve_number,
                    fecha_actuacion=get_date_update()
                )

                data_objects.append(obj)

                if current_actuacion:
                    actuacion_counter[current_actuacion] += 1

        for act, count in actuacion_counter.items():
            self.logger.info(f"Actualizando actuación: '{act} - {count}'")

        self.logger.info(f"Se serializaron {len(data_objects)} registros.")
        return data_objects
   
    def validation(self, url_key, data_objects) -> bool:
        return True