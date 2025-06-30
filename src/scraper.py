import json
import os
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Any, Dict, Type, Union

from bs4 import BeautifulSoup, Tag
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait

from config import config
from logs.logger import logger
from src.models import CompanyMetadata
from src.utils import get_url_scrape, parse_total_expected, get_date_update, extract_metadata


class BrowserSession:
    """
    Gestiona la sesión del navegador Chrome para el scraping.
    
    Esta clase se encarga de inicializar y configurar el navegador Chrome
    con las opciones adecuadas para el scraping, y proporciona una interfaz
    de context manager para su uso seguro.
    """
    
    def __init__(self) -> None:  
        """
        Inicializa la sesión del navegador con la configuración del proyecto.
        
        Carga la configuración del scraper y verifica la existencia de los
        binarios necesarios.
        
        Raises:
            FileNotFoundError: Si no se encuentra el archivo chromedriver.
        """
        # Levantar parametros de config
        self.headless = config.get("scraper.headless", True)
        self.user_agent = config.get("scraper.user_agent", "Mozilla/5.0 ...")
        self.verbose = config.get("scraper.verbose", False)

        # Paths de binarios desde ENV
        self.chromedriver_path = Path(os.getenv("CHROMEDRIVER_BIN", "/usr/bin/chromedriver"))
        self.chrome_bin = Path(os.getenv("CHROME_BIN", "/usr/bin/google-chrome"))
        
        if not self.chromedriver_path.exists():
            raise FileNotFoundError(f"El archivo chromedriver no se encuentra en la ruta: {self.chromedriver_path}")

        self.driver: Optional[webdriver.Chrome] = None

    def init_driver(self) -> webdriver.Chrome:
        """
        Inicializa el navegador con las opciones configuradas.
        
        Configura las opciones del navegador Chrome según los parámetros
        establecidos y crea una nueva instancia del driver.
        
        Returns:
            Instancia de webdriver.Chrome configurada.
            
        Raises:
            WebDriverException: Si hay problemas al inicializar el driver.
        """
        logger.info("Iniciando Webdriver con configs.")

        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--single-process")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"user-agent={self.user_agent}")
        options.binary_location = str(self.chrome_bin)

        service = ChromeService(executable_path=str(self.chromedriver_path))
        
        try:
            self.driver = webdriver.Chrome(service=service, options=options)
            return self.driver
        except WebDriverException as e:
            logger.error(f"Error al inicializar el WebDriver: {e}")
            raise

    def __enter__(self) -> webdriver.Chrome:
        """
        Context manager entry point to initialize the driver.
        
        Returns:
            Instancia de webdriver.Chrome inicializada.
        """
        self.driver = self.init_driver()
        return self.driver
    
    def __exit__(self, exc_type: Optional[Type[BaseException]], 
                 exc_value: Optional[BaseException], 
                 traceback: Optional[Any]) -> None:
        """
        Context manager exit point to close the driver.
        
        Args:
            exc_type: Tipo de excepción si ocurrió alguna.
            exc_value: Valor de la excepción si ocurrió alguna.
            traceback: Traceback si ocurrió alguna excepción.
        """
        if self.driver:
            self.driver.quit()
            logger.info("Webdriver cerrado correctamente.")
        else:
            logger.warning("No se pudo cerrar el Webdriver porque no estaba inicializado.")
    
class BaseScraper:
    """
    Clase base abstracta para implementar scrapers.
    
    Define la estructura y flujo común para todos los scrapers,
    incluyendo la carga de páginas, extracción de datos,
    validación y guardado de resultados.
    """
    
    def __init__(self, driver: webdriver.Chrome) -> None:
        """
        Inicializa el scraper con un driver de navegador.
        
        Args:
            driver: Instancia de webdriver.Chrome para navegar por las páginas.
        """
        self.driver = driver
        self.logger = logger
    
    def load_page(self, url_key: str) -> webdriver.Chrome:
        """
        Carga la página web en el navegador.
        
        Construye la URL dinámica a partir de la clave de configuración
        y navega a esa dirección.
        
        Args:
            url_key: Clave de la URL en el archivo de configuración.
            
        Returns:
            Instancia del driver con la página cargada.
            
        Raises:
            ValueError: Si la URL no puede ser construida o cargada.
        """
        self.logger.info(f"Cargando la página de {url_key}")
        try:
            dynamic_url = get_url_scrape(url_key)
            self.logger.info(f"URL dinámica construida: {dynamic_url}")
                   
            self.driver.get(dynamic_url)
            self.driver.implicitly_wait(6)

            return self.driver
        except Exception as e:
            self.logger.error(f"Error al cargar la página {url_key}: {e}")
            raise ValueError(f"No se pudo cargar la página para {url_key}: {e}") from e

    @abstractmethod
    def extract_data(self) -> List[Tag]:
        """
        Extrae las filas de la tabla (TRs) de la página cargada.
        
        Returns:
            Lista de elementos BeautifulSoup que representan las filas de datos.
            
        Raises:
            ValueError: Si no se pueden extraer los datos.
        """
        pass

    @abstractmethod
    def extract_serialization(self, rows: List[Tag]) -> List[CompanyMetadata]:
        """
        Parsea las filas (TRs) y construye la lista de CompanyMetadata.
        
        Args:
            rows: Lista de elementos BeautifulSoup que representan las filas de datos.
            
        Returns:
            Lista de objetos CompanyMetadata con los datos extraídos.
            
        Raises:
            ValueError: Si no se pueden serializar los datos.
        """
        pass
    
    @abstractmethod
    def validation(self, url_key: str, data_objects: List[CompanyMetadata]) -> bool:
        """
        Realiza validaciones sobre los datos extraídos.
        
        Args:
            url_key: Clave de la URL de donde se extrajeron los datos.
            data_objects: Lista de objetos CompanyMetadata a validar.
            
        Returns:
            True si los datos son válidos, False en caso contrario.
        """
        pass

    def save_data(self, data_objects: List[CompanyMetadata], output_path: str) -> None:
        """
        Guarda los datos serializados en formato JSONL.
        
        Cada línea del archivo es un JSON individual correspondiente
        a un objeto CompanyMetadata.
        
        Args:
            data_objects: Lista de objetos CompanyMetadata a guardar.
            output_path: Ruta donde se guardará el archivo JSONL.
            
        Raises:
            IOError: Si hay problemas al escribir el archivo.
        """
        self.logger.info(f"Guardando {len(data_objects)} registros en '{output_path}'...")
        
        # Asegurar que el directorio existe (útil para desarrollo local y otras rutas)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                for obj in data_objects:
                    json_line = json.dumps(obj.serialize(), ensure_ascii=False)
                    f.write(json_line + "\n")

            self.logger.info(f"Se guardaron {len(data_objects)} registros en '{output_path}'.")
            self.logger.info(f"Archivo creado: {Path(output_path).exists()}")
        except IOError as e:
            self.logger.error(f"Error al guardar datos en {output_path}: {e}")
            raise
    
    def run(self, url_key: str) -> List[CompanyMetadata]:
        """
        Ejecuta el flujo completo de scraping y serialización.
        
        Args:
            url_key: Clave de la URL en el archivo de configuración.
            
        Returns:
            Lista de objetos CompanyMetadata con los datos extraídos.
            
        Raises:
            ValueError: Si hay problemas durante el proceso de scraping.
        """
        self.logger.info("Iniciando proceso de scraping.")

        url_base = config.get(f"urls.{url_key}")

        if not url_base:
            self.logger.error(f"No se encontró URL para la key: {url_key}")
            raise ValueError(f"URL no encontrada en config para key: {url_key}")

        # Cargar la página
        self.load_page(url_key)

        # Extraer las filas
        rows = self.extract_data()

        # Serializa las filas a objetos CompanyMetadata
        data_objects = self.extract_serialization(rows)

        self.logger.info(f"Proceso completo. Total registros: {len(data_objects)}.")

        return data_objects
        
    def trigger(self, url_key: str, output_path: str) -> None:
        """
        Ejecuta el flujo completo: extracción, validación y guardado.
        
        Args:
            url_key: Clave de la URL en el archivo de configuración.
            output_path: Ruta donde se guardará el archivo JSONL.
            
        Raises:
            ValueError: Si hay problemas durante el proceso.
        """
        self.logger.info(f"===> Iniciando trigger de scraping para URL key: {url_key}")

        url_base = config.get(f"urls.{url_key}")

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
            self.logger.info(f"Trigger finalizado con éxito. Archivo guardado: {output_path}")
        else:
            self.logger.error(f"Validación fallida. No se guardará el archivo '{output_path}'.")

    def close(self) -> None:
        """
        Cierra el driver del navegador y libera recursos.
        
        Esta función debe llamarse cuando ya no se necesita el scraper
        para liberar recursos del sistema.
        """
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Webdriver cerrado correctamente desde close().")
            except WebDriverException as e:
                self.logger.error(f"Error al cerrar el Webdriver: {e}")
            except Exception as e:
                self.logger.error(f"Error inesperado al cerrar el Webdriver: {e}")
        else:
            self.logger.warning("Webdriver no estaba inicializado. Nada que cerrar.")

class SociedadScraper(BaseScraper):
    """
    Implementación de scraper para el Registro de Empresas y Sociedades.
    
    Extrae datos de sociedades desde la página del Registro de Empresas
    y Sociedades, incluyendo información como RUT, razón social, etc.
    """
    
    def extract_data(self) -> List[Tag]:
        """
        Extrae las filas de la tabla de sociedades.
        
        Expande la tabla para mostrar todos los registros y extrae
        las filas de datos.
        
        Returns:
            Lista de elementos BeautifulSoup que representan las filas de datos.
            
        Raises:
            ValueError: Si no se puede cargar o encontrar la tabla de sociedades.
        """
        self.logger.info(f"Extrayendo filas de la tabla... {self.driver.current_url}")

        try:
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
            if not table:
                raise ValueError("No se encontró el cuerpo de la tabla en la página.")
                
            rows = table.find_all('tr', attrs={'role': 'row'})

            self.logger.info(f"Se encontraron {len(rows)} filas en la tabla.")
            return rows
            
        except TimeoutException:
            self.logger.error("Tiempo de espera agotado al cargar la tabla de sociedades.")
            raise ValueError("Tiempo de espera agotado al cargar la tabla de sociedades.")
        except Exception as e:
            self.logger.error(f"Error al extraer datos de la tabla: {e}")
            raise ValueError(f"Error al extraer datos de la tabla: {e}") from e

    def extract_serialization(self, rows: List[Tag]) -> List[CompanyMetadata]:
        """
        Parsea las filas de la tabla y construye la lista de CompanyMetadata.
        
        Args:
            rows: Lista de elementos BeautifulSoup que representan las filas de datos.
            
        Returns:
            Lista de objetos CompanyMetadata con los datos extraídos.
            
        Raises:
            ValueError: Si hay problemas al parsear los datos o la estructura
                       de la tabla no es la esperada.
        """
        self.logger.info("Serializando las filas...")

        data_objects: List[CompanyMetadata] = []
        for row in rows:
            cols = [td.text.strip() for td in row.find_all('td')]

            # Asegúrate de que el número de columnas corresponde al modelo
            if not cols or len(cols) != 6:
                self.logger.error(f"Estructura inesperada de la tabla: {cols}")
                raise ValueError("Cambio en la estructura de la tabla, debe revisarse!")

            try:
                # Construcción del objeto
                obj = CompanyMetadata(
                    rut=cols[2].replace('.',''),
                    razon_social=cols[4],
                    url=self.driver.current_url,
                    actuacion=cols[1],
                    nro_atencion=cols[3],
                    cve=cols[5],
                    fecha_actuacion=datetime.strptime(cols[0].strip(), "%d-%m-%Y")
                )

                data_objects.append(obj)
            except ValueError as e:
                self.logger.error(f"Error al parsear fecha u otro campo: {e}")
                raise ValueError(f"Error al parsear datos de la fila: {e}") from e

        self.logger.info(f"Se serializaron {len(data_objects)} registros.")

        return data_objects

    def validation(self, url_key: str, data_objects: List[CompanyMetadata]) -> bool:
        """
        Valida que se hayan extraído todos los registros esperados.
        
        Compara el número de registros extraídos con el número total
        indicado en la página.
        
        Args:
            url_key: Clave de la URL de donde se extrajeron los datos.
            data_objects: Lista de objetos CompanyMetadata a validar.
            
        Returns:
            True si el número de registros coincide, False en caso contrario.
        """
        self.logger.info("Realizando validaciones de los datos extraídos...")

        try:
            html = self.driver.find_element(By.XPATH, '//*[@id="tblSociedades_info"]').text

            total_expected = parse_total_expected(html)
            total_extracted = len(data_objects) 

            if total_expected == total_extracted:
                self.logger.info(f"Validación exitosa: {total_extracted} registros extraídos.")
                return True
            else:
                self.logger.warning(f"Validando URL key: {url_key} — Esperados: {total_expected}, Extraídos: {total_extracted}")
                return False
        except Exception as e:
            self.logger.error(f"Error durante la validación: {e}")
            return False

class DiarioScraper(BaseScraper):
    """
    Implementación de scraper para el Diario Oficial.
    
    Extrae datos de empresas y sociedades publicados en el Diario Oficial,
    incluyendo diferentes tipos de actuaciones como constituciones,
    modificaciones, etc.
    """
    
    def extract_data(self) -> List[Tag]:
        """
        Extrae las filas de datos del Diario Oficial.
        
        Navega por las diferentes secciones del Diario Oficial para
        encontrar las publicaciones relacionadas con empresas y sociedades.
        
        Returns:
            Lista de elementos BeautifulSoup que representan las filas de datos.
            Lista vacía si no hay publicaciones para la fecha.
            
        Raises:
            ValueError: Si hay problemas al extraer los datos.
        """
        self.logger.info(f"Extrayendo filas de la tabla... {self.driver.current_url}")
        
        # Verificar si hay publicaciones
        soup_page = BeautifulSoup(self.driver.page_source, 'html.parser')
        nofound = soup_page.find('p', class_='nofound')
        
        if nofound:
            self.logger.info(f"No hay publicaciones para esta edición ({self.driver.current_url}): '{nofound.text.strip()}'")
            return [] 

        try:
            # Navegar a la edición correcta si estamos en la página de selección
            if "select_edition" in self.driver.current_url:
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, "index.php?date=")]'))
                ).click()
                self.logger.info('Ingreso a select_edition')

            # Navegar a la sección de empresas cooperativas
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, "empresas_cooperativas.php")]'))
            ).click()
            self.logger.info('Navegando a la sección de empresas cooperativas')

            # Obtener la tabla de datos
            html = self.driver.find_element(By.XPATH, "//tbody").get_attribute('outerHTML')

            # Parsear con BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            rows = soup.find_all("tr")

            self.logger.info(f"Se encontraron {len(rows)} filas en la tabla.")
            return rows

        except TimeoutException as e:
            self.logger.error(f"Tiempo de espera agotado en {self.driver.current_url}: {e}")
            raise ValueError(f"Tiempo de espera agotado al cargar la página: {e}") from e
        except Exception as e:
            self.logger.error(f"Error inesperado en {self.driver.current_url}: {e}")
            raise ValueError(f"No se pudo extraer la tabla de sociedades cooperativas en {self.driver.current_url}: {e}") from e

    def extract_serialization(self, rows: List[Tag]) -> List[CompanyMetadata]:
        """
        Parsea las filas de la tabla y construye la lista de CompanyMetadata.
        
        Procesa las filas de la tabla del Diario Oficial, identificando
        tanto filas de contexto (que indican el tipo de actuación) como
        filas de contenido (con los datos de las empresas).
        
        Args:
            rows: Lista de elementos BeautifulSoup que representan las filas de datos.
            
        Returns:
            Lista de objetos CompanyMetadata con los datos extraídos.
        """
        data_objects: List[CompanyMetadata] = []
        current_actuacion: Optional[str] = None  # Contexto que vamos actualizando
        actuacion_counter: Dict[str, int] = {}

        for row in rows:
            try:
                # Caso 1 → Es fila "contextual" → revisar si hay title3
                if not row.has_attr('class'):
                    td_title3 = row.find('td', class_='title3')
                    if td_title3:
                        current_actuacion = td_title3.text.strip()
                        if current_actuacion not in actuacion_counter:
                            actuacion_counter[current_actuacion] = 0

                # Caso 2 → Es fila de contenido
                elif 'content' in row.get('class', []):
                    if not current_actuacion:
                        self.logger.warning("Fila de contenido sin actuación definida")
                        continue

                    try:
                        rut_number, razon, url_pdf, cve_number = extract_metadata(row)
                        
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

                        actuacion_counter[current_actuacion] += 1
                    except ValueError as e:
                        self.logger.error(f"Error al extraer metadatos de la fila: {e}")
                        continue
            except Exception as e:
                self.logger.error(f"Error al procesar fila: {e}")
                continue

        # Registrar estadísticas de actuaciones
        for act, count in actuacion_counter.items():
            self.logger.info(f"Actualizando actuación: '{act} - {count}'")

        self.logger.info(f"Se serializaron {len(data_objects)} registros.")
        return data_objects
   
    def validation(self, url_key: str, data_objects: List[CompanyMetadata]) -> bool:
        """
        Valida los datos extraídos del Diario Oficial.
        
        Para el Diario Oficial, no hay una validación específica
        ya que no hay un contador de registros esperados en la página.
        
        Args:
            url_key: Clave de la URL de donde se extrajeron los datos.
            data_objects: Lista de objetos CompanyMetadata a validar.
            
        Returns:
            True siempre, ya que no hay validación específica.
        """
        self.logger.info(f"Validación para {url_key}: {len(data_objects)} registros extraídos")
        return True