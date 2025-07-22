from typing import Optional, List
import pandas as pd
import boto3

from config import config
from logs.logger import logger

class GlueManager:
    """
    Gestiona operaciones de AWS Glue para datos particionados.
    
    Esta clase proporciona métodos para escribir datos particionados
    a S3 utilizando AWS Glue, optimizando el almacenamiento y las
    consultas posteriores con Athena.
    """
    
    def __init__(self) -> None:
        """
        Inicializa el gestor de Glue con la configuración del proyecto.
        """
        self.bucket_name = config.get("aws.s3_bucket")
        self.s3_base_path = config.get("aws.s3_name", "scraper/fiscalia")
        self.region = config.get("aws.region", "us-east-1")
        self.glue_client = boto3.client('glue', region_name=self.region)
        
    def write_partitioned_data(self, df: pd.DataFrame, path: str = "processed") -> Optional[str]:
        """
        Escribe un DataFrame a S3 con particionamiento por pa_date.
        
        Args:
            df: DataFrame de pandas a escribir.
            path: Ruta base en S3 donde escribir los datos.
            
        Returns:
            URL de S3 donde se escribieron los datos o None si hubo un error.
        """
        if df.empty:
            logger.warning("DataFrame vacío, no se escribirá a S3")
            return None
            
        try:
            # Importar las dependencias de Glue aquí para evitar problemas si no están instaladas
            from awsglue.context import GlueContext
            from awsglue.dynamicframe import DynamicFrame
            from pyspark.context import SparkContext
            
            spark_context = SparkContext.getOrCreate()
            glue_context = GlueContext(spark_context)
            
            # Convertir DataFrame de pandas a DynamicFrame de Glue
            spark_df = glue_context.spark_session.createDataFrame(df)
            dynamic_frame = DynamicFrame.fromDF(spark_df, glue_context, "dynamic_frame")
            
            # Construir la ruta completa usando el path proporcionado
            s3_path = f"s3://{self.bucket_name}/{path}"
            
            # Escribir con particionamiento
            glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": s3_path,
                    "partitionKeys": ["pa_date"]
                },
                format="parquet",
                format_options={"compression": "snappy"}
            )
            
            logger.info(f"DataFrame escrito con particionamiento por pa_date en {s3_path}")
            return s3_path
            
        except ImportError as e:
            logger.error(f"Error al importar dependencias de Glue: {e}. Usando método alternativo.")
            return None
        except Exception as e:
            logger.error(f"Error al escribir DataFrame particionado a S3: {e}")
            return None