import boto3
from datetime import datetime
from pathlib import Path
from config.config_loader import Config
from logs.logger import logger
from typing import Optional
from src.utils import jsonl_to_parquet


class S3Manager:
    def __init__(self):
        self.config = Config()
        self.bucket_name = self.config.get("aws.s3_bucket")
        self.s3_base_path = self.config.get("aws.s3_name", "/scraper/fiscalia/")
        self.region = self.config.get("aws.region", "us-east-1")
        
        self.s3_client = boto3.client('s3', region_name=self.region)
    
    def _get_daily_path(self) -> str:
        """
        Genera el path con la fecha del día: /scraper/fiscalia/20250125/
        """
        today = datetime.now()
        return f"{self.s3_base_path.rstrip('/')}/{today.year}{today.month:02d}{today.day:02d}/"
    
    def upload_raw(self) -> list[str]:
        """
        Procesa todos los JSONL en /tmp, los convierte a Parquet y los sube
        """        
        uploaded_files = []
        jsonl_files = list(Path("/tmp").glob("*.jsonl"))
        
        for jsonl_file in jsonl_files:
            try:
                # Convertir JSONL a Parquet usando utils
                parquet_file = f"/tmp/{jsonl_file.stem}.parquet"
                
                if not jsonl_to_parquet(str(jsonl_file), parquet_file):
                    logger.warning(f"No records found in {jsonl_file}")
                    continue
                
                # Subir a S3
                daily_path = self._get_daily_path()
                s3_key = f"{daily_path}raw/{jsonl_file.stem}.parquet"
                
                self.s3_client.upload_file(parquet_file, self.bucket_name, s3_key)
                s3_url = f"s3://{self.bucket_name}/{s3_key}"
                uploaded_files.append(s3_url)
                logger.info(f"Raw parquet uploaded: {s3_url}")
                
                # Limpiar archivo temporal
                Path(parquet_file).unlink()
                
            except Exception as e:
                logger.error(f"Error processing {jsonl_file}: {e}")
        
        return uploaded_files
    
    def download_raw(self, s3_key: str, local_path: str) -> bool:
        """
        Descarga archivo raw
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Raw file downloaded: {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading raw file {s3_key}: {e}")
            return False
    
    def upload_processed(self, local_path: str, filename: str) -> Optional[str]:
        """
        Sube archivo procesado con timestamp y path diario
        """
        timestamp = datetime.now().strftime("%H%M%S")
        daily_path = self._get_daily_path()
        s3_key = f"{daily_path}processed/{filename}_{timestamp}.jsonl"
        
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"Processed file uploaded: s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
        except Exception as e:
            logger.error(f"Error uploading processed file {local_path}: {e}")
            return None
    
    def download_processed(self, s3_key: str, local_path: str) -> bool:
        """
        Descarga archivo procesado
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Processed file downloaded: {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading processed file {s3_key}: {e}")
            return False