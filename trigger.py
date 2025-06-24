from src.scraper import DiarioScraper, SociedadScraper, BrowserSession
from logs.logger import logger
from config.config_loader import Config
from src.utils import return_metadata
import json

def main():
    config = Config()

    #  Crear instancia de BrowserSession.
    browser_session = BrowserSession(config)
    driver = browser_session.init_driver()

    try:
        #  FiscaliaScraper
        fiscalia_scraper = SociedadScraper(driver, config)
        fiscalia_scraper.trigger("sociedades", output_path=config.get("output.sociedades"))
        logger.exception("Extracción de modificaciones de sociedades se ejecutó correctamente.")

        #  PimeScraper
        diario_scraper = DiarioScraper(driver, config)
        diario_scraper.trigger("diario_oficial", output_path=config.get("output.diario_oficial"))
        logger.exception("Extracción de modificaciones de diario_oficial se ejecutó correctamente.")

    except Exception as e:
        logger.exception(f" Error general: {e}")

    finally:
        # ⚠️ Cerramos el driver SOLO AL FINAL
        driver.quit()
        logger.exception(" Driver cerrado correctamente.")
    
    
    df = return_metadata()
    return df


if __name__ == "__main__":
    main()
