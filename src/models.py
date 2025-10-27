from datetime import datetime, date
from typing import Dict, Optional, Any, Union
from pydantic import BaseModel, field_validator

class CompanyMetadata(BaseModel):
    """
    Representa los metadatos de una empresa o sociedad.
    
    Esta clase modela la información extraída de las fuentes oficiales
    sobre empresas y sociedades, incluyendo su identificación, datos
    de registro y fechas relevantes.
    """
    fuente: str
    rut: Optional[int]
    rut_df: Optional[str]
    razon_social: str
    url: Optional[str]
    actuacion: str
    nro_atencion: Optional[str]
    cve: str
    pa_date: str = datetime.now().strftime("%Y-%m-%d")
    fecha_actuacion: datetime

    class Config:
        arbitrary_types_allowed = True
    
    def serialize(self) -> Dict[str, Any]:
        """
        Serializa el objeto a diccionario para exportación.
        
        Convierte las fechas a formato string para facilitar
        la serialización a JSON.
        
        Returns:
            Dict[str, Any]: Diccionario con los datos serializados.
        """
        return {
            "fuente": self.fuente,
            "rut": self.rut,
            "rut_df": self.rut_df,
            "razon_social": self.razon_social,
            "url": self.url,
            "actuacion": self.actuacion,
            "nro_atencion": self.nro_atencion,
            "cve": self.cve,
            "pa_date": self.pa_date,
            "fecha_actuacion": self.fecha_actuacion
        }

class EmpresaData(BaseModel):
    """
    Modelo para datos de la tabla tbl_maestro_empresas.
    
    Convierte tipos de string (Athena) a tipos correctos de Python.
    """
    rut_cliente: int
    rut_cliente_dv: str
    plataforma: Optional[str]
    segmento: Optional[str]
    ejec_cod: Optional[int]
    fecha_proceso: datetime
    
    @field_validator('rut_cliente_dv', mode='before')
    @classmethod
    def convert_rut_dv_to_str(cls, v):
        return str(v) if v is not None else None
    
class SufData(BaseModel):
    """
    Modelo para datos de la tabla tbl_tsuf_pcp.
    
    Convierte tipos de string (Athena) a tipos correctos de Python.
    """
    cli_rut: int
    cli_rut_dv: str
    fecha_proceso: datetime
    
    @field_validator('cli_rut_dv', mode='before')
    @classmethod
    def convert_rut_dv_to_str(cls, v):
        return str(v) if v is not None else None
  
class FuncionarioData(BaseModel):
    """
    Modelo para datos de la tabla tbl_base_funcionarios.
    
    Convierte tipos de string (Athena) a tipos correctos de Python.
    """
    rut_funcionario: int
    rut_funcionario_dv: str
    nombre_funcionario: Optional[str]
    nombre_puesto: Optional[str]
    dependencia: Optional[str]
    correo: Optional[str]
    fecha_carga_dl: date
    ejc_cod: Optional[int]
    
    @field_validator('rut_funcionario_dv', mode='before')
    @classmethod
    def convert_rut_dv_to_str(cls, v):
        return str(v) if v is not None else None
    
class EnrichedCompanyData(BaseModel):
    """
    Modelo para el dataset final enriquecido.
    
    Combina datos de CompanyMetadata, EmpresaData y FuncionarioData.
    """
    # Campos de CompanyMetadata
    fuente: str
    rut: Optional[int]
    rut_df: Optional[str]
    razon_social: str
    url: Optional[str]
    actuacion: str
    nro_atencion: Optional[str]
    cve: str
    pa_date: str
    fecha_actuacion: datetime
    
    # Campos de EmpresaData
    segmento: Optional[str] = None
    plataforma: Optional[str] = None
    ejec_cod: Optional[int] = None
    
    # Campos de FuncionarioData
    rut_funcionario: Optional[int] = None
    rut_funcionario_dv: Optional[str] = None
    nombre_funcionario: Optional[str] = None
    nombre_puesto: Optional[str] = None
    correo: Optional[str] = None
    dependencia: Optional[str] = None
    
    class Config:
        arbitrary_types_allowed = True