from src.scraper import DiarioScraper, SociedadScraper, BrowserSession
from config.config_loader import Config
from src.utils import return_metadata

def main():
    config = Config()

    #  Crear instancia de BrowserSession.
    browser_session = BrowserSession(config)
    driver = browser_session.init_driver()

    try:
        #  FiscaliaScraper
        fiscalia_scraper = SociedadScraper(driver, config)
        fiscalia_scraper.trigger("sociedades", output_path=config.get("output.sociedades"))
        print("Extracción de modificaciones de sociedades se ejecutó correctamente.")

        #  PimeScraper
        diario_scraper = DiarioScraper(driver, config)
        diario_scraper.trigger("diario_oficial", output_path=config.get("output.diario_oficial"))
        print("Extracción de modificaciones de diario_oficial se ejecutó correctamente.")

    except Exception as e:
        print(f" Error general: {e}")

    finally:
        # ⚠️ Cerramos el driver SOLO AL FINAL
        driver.quit()
        print(" Driver cerrado correctamente.")
    
    
    df = return_metadata()
    return df


def lambda_handler(event, context) -> dict:
    try:
        df = main()
        # Tomamos la primera fila como lista de dicts
        payload = (
                    df.head(1)
                    .astype(str)  # convierte todos los valores a su representación de texto
                    .to_dict(orient="records"))
        
        print ({
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload)
        })

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload)
        }


    except Exception as e:
        logger.exception("Error en lambda_handler")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    test_event = {}
    test_context = None
    lambda_handler(test_event,test_context)

