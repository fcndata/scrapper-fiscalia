from pathlib import Path
from config.config_loader import Config
from src.scrapper.sociedades import SociedadesScraper

def main():
    # Cargar configuración desde config.yaml
    config = Config(Path("config/config.yaml"))

    # Ejecutar scraper
    scraper = SociedadesScraper(config=config)
    data = scraper.run()

    # Mostrar cantidad de filas y primeras 3 para validar
    print(f"✅ Se extrajeron {len(data)} filas.")
    for i, row in enumerate(data[:3], 1):
        print(f"Fila {i}: {row}")

if __name__ == "__main__":
    main()
