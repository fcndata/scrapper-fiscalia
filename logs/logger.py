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

# Logger instance
logger = logging.getLogger(__name__)

