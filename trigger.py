from src.scraper import DiarioScraper, SociedadScraper, BrowserSession
from logs.logger import logger
from config import config

def main():
    #  Crear instancia de BrowserSession.
    browser_session = BrowserSession()
    driver = browser_session.init_driver()

    try:
        #  FiscaliaScraper
        fiscalia_scraper = SociedadScraper(driver)
        fiscalia_scraper.trigger("sociedades", output_path=config.get("output.sociedades"))
        logger.info("Extracción de modificaciones de sociedades se ejecutó correctamente.")

        #  PimeScraper
        diario_scraper = DiarioScraper(driver)
        diario_scraper.trigger("diario_oficial", output_path=config.get("output.diario_oficial"))
        logger.info("Extracción de modificaciones de diario_oficial se ejecutó correctamente.")

    except Exception as e:
        logger.exception(f" Error general: {e}")

    finally:
        # ⚠️ Cerramos el driver SOLO AL FINAL
        driver.quit()
        logger.info(" Driver cerrado correctamente.")
    
    return


if __name__ == "__main__":
    main()
