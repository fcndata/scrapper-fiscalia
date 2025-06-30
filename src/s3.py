from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import boto3
from botocore.exceptions import ClientError

from config import config
from logs.logger import logger
from src.utils import jsonl_to_parquet


class S3Manager:
    """
    Gestiona las operaciones de almacenamiento en AWS S3.
    
    Esta clase proporciona métodos para subir y descargar archivos
    desde/hacia AWS S3, organizando los datos en una estructura
    jerárquica por fecha.
    """
    
    def __init__(self) -> None:
        """
        Inicializa el gestor de S3 con la configuración del proyecto.
        
        Carga la configuración de AWS desde el archivo de configuración
        e inicializa el cliente de S3.
        """
        self.bucket_name = config.get("aws.s3_bucket")
        self.s3_base_path = config.get("aws.s3_name", "scraper/fiscalia")
        self.region = config.get("aws.region", "us-east-1")
        
        self.s3_client = boto3.client('s3', region_name=self.region)
    
    def _get_daily_path(self) -> str:
        """
        Genera el path con la fecha del día: scraper/fiscalia/20250125/
        
        Returns:
            String con la ruta base incluyendo la fecha actual en formato YYYYMMDD.
        """
        today = datetime.now()
        # Eliminar slash inicial y final para evitar doble slash
        clean_path = self.s3_base_path.strip('/')
        return f"{clean_path}/{today.year}{today.month:02d}{today.day:02d}/"
    
    def upload_raw(self) -> List[str]:
        """
        Procesa todos los archivos JSONL en /tmp, los convierte a Parquet y los sube a S3.
        
        Busca todos los archivos .jsonl en el directorio /tmp, los convierte
        a formato Parquet y los sube al bucket de S3 en la carpeta 'raw'.
        
        Returns:
            Lista de URLs de S3 de los archivos subidos.
            
        Raises:
            No lanza excepciones directamente, captura y registra errores internamente.
        """
        uploaded_files: List[str] = []
        jsonl_files = list(Path("/tmp").glob("*.jsonl"))
        
        logger.info(f"Bucket: {self.bucket_name}, Base path: {self.s3_base_path}")
        logger.info(f"Found {len(jsonl_files)} JSONL files: {[f.name for f in jsonl_files]}")
        
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
                
            except ClientError as e:
                logger.error(f"AWS error processing {jsonl_file}: {e}")
            except IOError as e:
                logger.error(f"I/O error processing {jsonl_file}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing {jsonl_file}: {e}")
        
        return uploaded_files
    
    def download_raw(self, s3_key: str, local_path: str) -> bool:
        """
        Descarga un archivo raw desde S3.
        
        Args:
            s3_key: Clave del objeto en S3 (ruta relativa dentro del bucket).
            local_path: Ruta local donde se guardará el archivo.
            
        Returns:
            True si la descarga fue exitosa, False en caso contrario.
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Raw file downloaded: {local_path}")
            return True
        except ClientError as e:
            logger.error(f"AWS error downloading raw file {s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error downloading raw file {s3_key}: {e}")
            return False
    
    def upload_processed(self, local_path: str, filename: str) -> Optional[str]:
        """
        Sube un archivo procesado a S3 con timestamp y path diario.
        
        Args:
            local_path: Ruta local del archivo a subir.
            filename: Nombre base del archivo (sin extensión).
            
        Returns:
            URL de S3 del archivo subido o None si hubo un error.
        """
        timestamp = datetime.now().strftime("%H%M%S")
        daily_path = self._get_daily_path()
        s3_key = f"{daily_path}processed/{filename}_{timestamp}.jsonl"
        
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            s3_url = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Processed file uploaded: {s3_url}")
            return s3_url
        except ClientError as e:
            logger.error(f"AWS error uploading processed file {local_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error uploading processed file {local_path}: {e}")
            return None
    
    def download_processed(self, s3_key: str, local_path: str) -> bool:
        """
        Descarga un archivo procesado desde S3.
        
        Args:
            s3_key: Clave del objeto en S3 (ruta relativa dentro del bucket).
            local_path: Ruta local donde se guardará el archivo.
            
        Returns:
            True si la descarga fue exitosa, False en caso contrario.
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Processed file downloaded: {local_path}")
            return True
        except ClientError as e:
            logger.error(f"AWS error downloading processed file {s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error downloading processed file {s3_key}: {e}")
            return False