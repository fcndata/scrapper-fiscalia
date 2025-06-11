from src.scraper import DiarioScraper, SociedadScraper, BrowserSession
from config.config_loader import Config

def main():
    config = Config()

    #  Crear instancia de BrowserSession.
    browser_session = BrowserSession(config)
    driver = browser_session.init_driver()

    try:
        #  FiscaliaScraper
        fiscalia_scraper = SociedadScraper(driver, config)
        fiscalia_scraper.trigger("sociedades", output_path="data/fiscalia_output.jsonl")
        print("Extracción de modificaciones de sociedades se ejecutó correctamente.")

        #  PimeScraper
        pime_scraper = DiarioScraper(driver, config)
        pime_scraper.trigger("diario_oficial", output_path="data/pime_output.jsonl")
        print("Extracción de modificaciones de diario_oficial se ejecutó correctamente.")

    except Exception as e:
        print(f" Error general: {e}")

    finally:
        # ⚠️ Cerramos el driver SOLO AL FINAL
        driver.quit()
        print(" Driver cerrado correctamente.")

if __name__ == "__main__":
    main()
