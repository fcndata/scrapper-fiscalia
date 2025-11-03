import logging
from datetime import datetime
from pathlib import Path


# Log directory
LOG_DIR = Path("/tmp/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Log file name
today = datetime.now().strftime('%Y%m%d')
log_filename = LOG_DIR / f'upload_log_{today}.txt'

# Basic logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def notification_structure(title: str, body_blocks: list[dict] = []) -> dict:
    message = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": title,
                            "weight": "Bolder",
                            "size": "Medium",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": "",
                            "spacing": "Medium",
                            "separator": True
                        }
                    ] + body_blocks
                }
            }
        ]
    }
    return message

def logs_message(log_data: dict) -> list[dict]:
    text_lines = []
    text_lines.append(f"El total de modificaciones rescatadas fue de {log_data.get('data_extracted', 0)}")
    text_lines.append(f"Se le agregó metadata a {log_data.get('data_enriched', 0)} empresas")
    text_lines.append(f"Del total de las modificaciones, únicamente {log_data.get('data_filtered_by_suf', 0)} son clientes del banco")
    text_lines.append(f"Finalmente, por reglas de negocio, se enviaron {log_data.get('data_filtered_by_business_rules', 0)}")
    text_lines.append(f"¿Se envió el correo al Teams del negocio?: {log_data.get('message_business', '')}")
    
    line_blocks = []
    for line in text_lines:
      line_blocks.append({
        "type": "TextBlock",
        "text": line,
        "wrap": True,
        "spacing": "None"
      })
    return line_blocks

# Logger instance
logger = logging.getLogger(__name__)

