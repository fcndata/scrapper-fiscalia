import time
from typing import Optional, List, Dict, Any

import boto3
import pandas as pd

from config import config
from logs.logger import logger
from src.utils import query_empresas, query_funcionarios, query_sufs
from src.models import CompanyMetadata, EmpresaData, FuncionarioData, SufData

class AthenaManager:
    """
    Gestiona las consultas a Amazon Athena.
    """
    
    def __init__(self) -> None:
        """
        Inicializa el gestor de Athena con la configuración del proyecto.
        """
        self.region = config.get("aws.region", "us-east-1")
        self.bucket_name = config.get("aws.s3_bucket")
        self.output_location = f"s3://{self.bucket_name}/athena-results/"
        
        self.athena_client = boto3.client('athena', region_name=self.region)
        self.s3_client = boto3.client('s3', region_name=self.region)
    
    def execute_query(self, query: str, database: str) -> pd.DataFrame:
        """
        Ejecuta una consulta en Athena y devuelve los resultados como DataFrame.

        Args:
            query: Consulta SQL a ejecutar.
            database: Base de datos de Athena donde ejecutar la consulta.
            
        Returns:
            DataFrame con los resultados de la consulta.
        """
        logger.info(f"Ejecutando consulta en {database}: {query}")
        
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': self.output_location}
            )
            
            query_id = response['QueryExecutionId']
            status = self._wait_for_completion(query_id)
            
            if status != 'SUCCEEDED':
                # Obtener detalles del error
                error_details = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                error_message = error_details.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason', 'No details available')
                logger.error(f"Error en consulta Athena: {error_message}")
                raise Exception(f"La consulta falló con estado: {status}. Detalles: {error_message}")
            
            return self._get_results(query_id)
        except Exception as e:
            logger.error(f"Error al ejecutar consulta en Athena: {str(e)}")
            # Crear un DataFrame vacío para evitar errores en el flujo
            return pd.DataFrame()
    
    def _wait_for_completion(self, query_id: str) -> str:
        """
        Espera a que una consulta termine y devuelve su estado.
        
        Args:
            query_id: ID de ejecución de la consulta.
            
        Returns:
            Estado final de la consulta.
        """
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status
            
            time.sleep(2)
    
    def _get_results(self, query_id: str) -> pd.DataFrame:
        """
        Obtiene los resultados de una consulta y los convierte a DataFrame.
        
        Args:
            query_id: ID de ejecución de la consulta.
            
        Returns:
            DataFrame con los resultados.
        """
        response = self.athena_client.get_query_execution(QueryExecutionId=query_id)
        s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        
        # Extraer bucket y key
        s3_path = s3_path.replace("s3://", "")
        bucket, key = s3_path.split("/", 1)
        
        # Descargar y leer archivo
        local_path = f"/tmp/{query_id}.csv"
        self.s3_client.download_file(bucket, key, local_path)
        
        df = pd.read_csv(local_path)
        logger.info(f"Consulta completada: {len(df)} filas obtenidas")
        
        return df
    
    def get_empresas_data(self, ruts: List[int]) -> List[EmpresaData]:
        """
        Obtiene los datos de la tabla maestro de empresas.
        
        Args:
            ruts: Lista de RUTs para filtrar empresas.
        
        Returns:
            Lista de objetos EmpresaData.
        """
        if not ruts:
            logger.warning("No se proporcionaron RUTs para consultar empresas")
            return []
        
        # Convertir RUTs a formato dict para query_empresas
        list_ruts = [rut for rut in ruts]
        
        df = self.execute_query(query_empresas(list_ruts), "bd_in_tablas_generales")
        
        # Convertir DataFrame a lista de EmpresaData
        empresas_list = []
        for _, row in df.iterrows():
            try:
                empresa = EmpresaData(**row.to_dict())
                empresas_list.append(empresa)
            except Exception as e:
                logger.error(f"Error creando EmpresaData: {e}")
                continue
        
        logger.info(f"Se obtuvieron {len(empresas_list)} registros de empresas")
        return empresas_list
    
    def get_funcionarios_data(self, empresas: List[EmpresaData]) -> List[FuncionarioData]:
        """
        Obtiene los datos de la tabla base de funcionarios basado en códigos de ejecutivo.
        
        Args:
            empresas: Lista de objetos EmpresaData para extraer códigos de ejecutivo.
        
        Returns:
            Lista de objetos FuncionarioData.
        """
        ejec_codes = [emp.ejec_cod for emp in empresas if emp.ejec_cod]
        
        if not ejec_codes:
            logger.warning("No se encontraron códigos de ejecutivo")
            return []
            
        logger.info(f"Códigos de ejecutivo únicos: {len(set(ejec_codes))}")
        
        df = self.execute_query(query_funcionarios(ejec_codes), "bd_dlk_bcc_tablas_generales")
        
        # Convertir DataFrame a lista de FuncionarioData
        funcionarios_list = []
        for _, row in df.iterrows():
            try:
                funcionario = FuncionarioData(**row.to_dict())
                funcionarios_list.append(funcionario)
            except Exception as e:
                logger.error(f"Error creando FuncionarioData: {e}")
                continue
        
        logger.info(f"Se obtuvieron {len(funcionarios_list)} registros de funcionarios")
        return funcionarios_list
    
    def get_sufs_data(self, ruts: List[int]) -> List[SufData]:
        """
        Obtiene los RUTs de la tabla SUFs para filtrado.
        
        Returns:
            Lista de RUTs que están en SUFs.
        """
        list_ruts = [rut for rut in ruts]
        
        df = self.execute_query(query_sufs(list_ruts), "bd_in_gesdatos")
        
        # Convertir DataFrame a lista de EmpresaData
        empresas_sufs = []
        for _, row in df.iterrows():
            try:
                empresa = SufData(**row.to_dict())
                empresas_sufs.append(empresa)
            except Exception as e:
                logger.error(f"Error creando SufData: {e}")
                continue
        
        logger.info(f"Se obtuvieron {len(empresas_sufs)} RUTs únicos de SUFs")
        
        return empresas_sufs