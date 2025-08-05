import json
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import List, Optional, Union, Dict, Any

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from config import config
from logs.logger import logger




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
        self.raw_path = None
        self.processed_path = None
        # Inicializar los paths
        self._get_daily_path()
    
    def _get_daily_path(self) -> None:
        """
        Genera los paths con la fecha del día para raw y processed.
        
        Establece self.raw_path y self.processed_path con la estructura:
        """
        today = datetime.now()
        clean_path = self.s3_base_path.strip('/')
        self.raw_path = f"{clean_path}/raw/pa_date={today.strftime('%Y-%m-%d')}/"
        self.processed_path = f"{clean_path}/processed/pa_date={today.strftime('%Y-%m-%d')}/"

    def upload_raw(self) -> List[str]:
        """
        Sube todos los archivos JSONL en /tmp directamente a S3.
        
        Busca todos los archivos .jsonl en el directorio /tmp y los sube
        al bucket de S3 en la carpeta 'raw' manteniendo el formato JSONL.
        
        Returns:
            Lista con los nombres de los archivos JSONL procesados.
            
        Raises:
            No lanza excepciones directamente, captura y registra errores internamente.
        """
        uploaded_files: List[str] = []
        jsonl_files = list(Path("/tmp").glob("*.jsonl"))
        
        logger.info(f"Bucket: {self.bucket_name}, Base path: {self.s3_base_path}")
        logger.info(f"Found {len(jsonl_files)} JSONL files: {[f.name for f in jsonl_files]}")
        
        for jsonl_file in jsonl_files:
            try:
                # Verificar que el archivo no esté vacío
                if jsonl_file.stat().st_size == 0:
                    logger.warning(f"Empty file: {jsonl_file}")
                    continue   
                # Subir a S3 directamente en formato JSONL
                s3_key = f"{self.raw_path}{jsonl_file.name}"
                
                self.s3_client.upload_file(str(jsonl_file), self.bucket_name, s3_key)
                s3_url = f"s3://{self.bucket_name}/{s3_key}"
                uploaded_files.append(s3_url)
                logger.info(f"Raw JSONL uploaded: {s3_url}")
                
            except ClientError as e:
                logger.error(f"AWS error processing {jsonl_file}: {e}")
            except IOError as e:
                logger.error(f"I/O error processing {jsonl_file}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing {jsonl_file}: {e}")
        
        return uploaded_files
    
    def download_raw(self) -> List[Dict[str, Any]]:
        """
        Descarga y consolida todos los archivos JSONL de la ruta raw en S3 en una lista de diccionarios.
        
        Returns:
            Lista de diccionarios con los datos consolidados de todos los archivos JSONL.
            Lista vacía si no se encontraron archivos o hubo errores.
        """
        try:
            # Listar directamente archivos JSONL en la ruta raw
            jsonl_files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=self.raw_path
            )
            
            # Extraer solo las claves de archivos JSONL
            for page in pages:
                if 'Contents' in page:
                    jsonl_files.extend([obj['Key'] for obj in page['Contents'] 
                                      if obj['Key'].endswith('.jsonl')])
            
            if not jsonl_files:
                logger.warning(f"No JSONL files found in raw path: {self.raw_path}")
                return []
                
            logger.info(f"Found {len(jsonl_files)} JSONL files to consolidate")
            
            # Consolidar registros directamente en memoria como diccionarios
            all_records = []
            
            for s3_key in jsonl_files:
                try:
                    # Obtener el contenido del archivo directamente en memoria
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                    content = response['Body'].read().decode('utf-8')
                    
                    # Procesar cada línea JSON directamente como diccionario
                    for line in content.splitlines():
                        if line.strip():
                            try:
                                record = json.loads(line)
                                all_records.append(record)
                            except json.JSONDecodeError as e:
                                logger.error(f"Error decoding JSON in {s3_key}: {e}")
                                continue
                    
                    logger.info(f"Processed {s3_key}, records so far: {len(all_records)}")
                    
                except ClientError as e:
                    logger.error(f"AWS error downloading {s3_key}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing {s3_key}: {e}")
                    continue
            
            logger.info(f"Consolidated {len(all_records)} records from {len(jsonl_files)} files")
            return all_records

        except Exception as e:
            logger.error(f"Error during consolidation from raw path: {e}")
            return []
    
    def upload_processed(self, df: pd.DataFrame, state: str = 'processed') -> Optional[str]:
        """
        Sube un DataFrame particionado por pa_date a S3 en formato Parquet.
        
        Args:
            df: DataFrame con columna pa_date.
            
        Returns:
            URL base de S3 o None si error.
        """
        if df.empty :
            logger.warning("DataFrame vacío")
            return None
        elif 'pa_date' not in df.columns:
            logger.warning("Sin pa_date")
            return None
            
        try:            
            # Agrupar por pa_date y escribir cada partición
            for pa_date, group_df in df.groupby('pa_date'):
                file_key = f"{self.s3_base_path.strip('/')}/{state}/pa_date={pa_date}/processed_data.parquet"
                
                # Convertir a Parquet en memoria
                buffer = BytesIO()
                group_df.to_parquet(buffer, compression='snappy', index=False)
                buffer.seek(0)
                
                # Subir a S3
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=file_key,
                    Body=buffer.getvalue(),
                    ContentType='application/octet-stream'
                )
                
                logger.info(f"Partición pa_date={pa_date} escrita: {len(group_df)} registros")
            
            base_s3_path = f"s3://{self.bucket_name}/{self.s3_base_path.strip('/')}/{state}"
            logger.info(f"DataFrame particionado escrito en {base_s3_path}")
            return base_s3_path
            
        except Exception as e:
            logger.error(f"Error escribiendo datos particionados: {e}")
            return None

    def download_processed(self, local_path: str) -> bool:
        """
        Descarga y consolida todos los archivos JSONL de la ruta processed en S3 en un único archivo local.
        
        Args:
            local_path: Ruta local donde se guardará el archivo JSONL consolidado.
            
        Returns:
            True si la descarga y consolidación fue exitosa, False en caso contrario.
        """
        try:
            # Listar todos los objetos en la ruta processed
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=self.processed_path
            )
            
            # Filtrar solo archivos JSONL
            jsonl_files = []
            for page in pages:
                if 'Contents' in page:
                    jsonl_files.extend([obj['Key'] for obj in page['Contents'] 
                                      if obj['Key'].endswith('.jsonl')])
            
            if not jsonl_files:
                logger.warning(f"No JSONL files found in processed path: {self.processed_path}")
                return False
                
            logger.info(f"Found {len(jsonl_files)} JSONL files to consolidate")
            
            # Crear directorio para archivos temporales si no existe
            temp_dir = Path("/tmp/s3_downloads")
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            # Descargar cada archivo a una ubicación temporal
            all_records = []
            for i, s3_key in enumerate(jsonl_files):
                temp_file = temp_dir / f"temp_{i}.jsonl"
                
                try:
                    self.s3_client.download_file(self.bucket_name, s3_key, str(temp_file))
                    logger.info(f"Downloaded: {s3_key} to {temp_file}")
                    
                    # Leer registros del archivo
                    with open(temp_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                all_records.append(line.strip())
                                
                    # Eliminar archivo temporal
                    temp_file.unlink()
                    
                except Exception as e:
                    logger.error(f"Error processing file {s3_key}: {e}")
                    continue
            
            # Escribir todos los registros al archivo consolidado
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, 'w', encoding='utf-8') as f:
                for record in all_records:
                    f.write(f"{record}\n")
                    
            logger.info(f"Consolidated {len(all_records)} records into {local_path}")
            return True
            
        except ClientError as e:
            logger.error(f"AWS error during consolidation from processed path: {e}")
            return False
        except Exception as e:
            logger.error(f"Error during consolidation from processed path: {e}")
            return False