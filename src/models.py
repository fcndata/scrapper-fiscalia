from datetime import datetime, date
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field, field_validator


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
    pa_date: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    fecha_actuacion: datetime

    class Config:
        arbitrary_types_allowed = True

    @field_validator('rut', pre=True)
    def convert_rut(cls, v):
        """Convierte rut de string a int si es necesario."""
        if v is None:
            return None
        return int(v) if str(v).strip() else None
    
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
            "fecha_actuacion": self.fecha_actuacion.strftime("%Y-%m-%d %H:%M:%S")
        }


class EmpresaData(BaseModel):
    """
    Modelo para datos de la tabla tbl_maestro_empresas.
    
    Convierte tipos de string (Athena) a tipos correctos de Python.
    """
    rut_cliente: int
    rut_cliente_dv: str
    plataforma: str
    segmento: str
    ejec_cod: int
    fecha_proceso: datetime
    
    @field_validator('rut_cliente', pre=True)
    def convert_rut_cliente(cls, v):
        """Convierte rut_cliente de string a int."""
        return int(v) if v and str(v).strip() else None
    
    @field_validator('ejec_cod', pre=True)
    def convert_ejec_cod(cls, v):
        """Convierte ejec_cod de string a int."""
        return int(v) if v and str(v).strip() else None
    
    @field_validator('fecha_proceso', pre=True)
    def convert_fecha_proceso(cls, v):
        """Convierte fecha_proceso de string a datetime."""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class SufData(BaseModel):
    """
    Modelo para datos de la tabla tbl_tsuf_pcp.
    
    Convierte tipos de string (Athena) a tipos correctos de Python.
    """
    cli_rut: int
    cli_rut_dv: str
    fecha_proceso: datetime
    
    @field_validator('cli_rut', pre=True)
    def convert_cli_rut(cls, v):
        """Convierte cli_rut de string a int."""
        return int(v) if v and str(v).strip() else None

    @field_validator('fecha_proceso', pre=True)
    def convert_fecha_proceso(cls, v):
        """Convierte fecha_proceso de string a datetime."""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class FuncionarioData(BaseModel):
    """
    Modelo para datos de la tabla tbl_base_funcionarios.
    
    Convierte tipos de string (Athena) a tipos correctos de Python.
    """
    rut_funcionario: int
    rut_funcionario_dv: str
    nombre_funcionario: str
    nombre_puesto: str
    dependencia: str
    correo: str
    fecha_carga_dl: date
    ejc_cod: int
    
    @field_validator('rut_funcionario', pre=True)
    def convert_rut_funcionario(cls, v):
        """Convierte rut_funcionario de string a int."""
        return int(v) if v and str(v).strip() else None
    
    @field_validator('ejc_cod', pre=True)
    def convert_ejc_cod(cls, v):
        """Convierte ejc_cod de string a int."""
        return int(v) if v and str(v).strip() else None
    
    @field_validator('fecha_carga_dl', pre=True)
    def convert_fecha_carga_dl(cls, v):
        """Convierte fecha_carga_dl de string a date."""
        if isinstance(v, str):
            return datetime.fromisoformat(v).date()
        return v


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

