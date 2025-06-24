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


def lambda_handler(event, context) -> dict:
    try:
        df = main()
        logger.debug("DataFrame generado, columnas: %s", df.columns.tolist())

        payload = (
                    df.head(8)
                    .astype(str)  # convierte todos los valores a su representación de texto
                    .to_dict(orient="records"))

        logger.info("Payload listo: %s", payload)

        response = {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload)
        }
        logger.debug("Response completa: %s", response)

        return response


    except Exception as e:
        logger.exception("Error en lambda_handler")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    resp = lambda_handler({}, None)
    print(json.dumps(resp, indent=2))
