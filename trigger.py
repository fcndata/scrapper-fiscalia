from src.scraper import FiscaliaScraper
from src.scraper import BrowserSession
from config.config_loader import Config

def main():
    config = Config()

    with BrowserSession(config) as driver:
        scraper = FiscaliaScraper(driver, config)

        # 🚀 Llamada trigger
        scraper.trigger("url_1", output_path="data/output.jsonl")

if __name__ == "__main__":
    main()
