from typing import Optional

from config import config
from logs.logger import logger
from src.scraper import DiarioScraper, SociedadScraper, BrowserSession


def main() -> None:
    """
    Función principal que ejecuta el proceso de scraping.
    
    Inicializa una sesión de navegador y ejecuta los scrapers
    para extraer datos de las diferentes fuentes configuradas.
    
    Raises:
        Exception: Captura y registra cualquier excepción durante el proceso.
    """
    #  Crear instancia de BrowserSession.
    browser_session = BrowserSession()
    driver = browser_session.init_driver()

    try:
        #  FiscaliaScraper
        fiscalia_scraper = SociedadScraper(driver)
        fiscalia_scraper.trigger("sociedades", output_path=config.get("output.sociedades"))
        logger.info("Extracción de modificaciones de sociedades se ejecutó correctamente.")

        #  DiarioScraper
        diario_scraper = DiarioScraper(driver)
        diario_scraper.trigger("diario_oficial", output_path=config.get("output.diario_oficial"))
        logger.info("Extracción de modificaciones de diario_oficial se ejecutó correctamente.")

    except Exception as e:
        logger.exception(f"Error general: {e}")

    finally:
        # ⚠️ Cerramos el driver SOLO AL FINAL
        driver.quit()
        logger.info("Driver cerrado correctamente.")
    
    return


if __name__ == "__main__":
    main()
