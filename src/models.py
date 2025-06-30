from datetime import datetime
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field


class CompanyMetadata(BaseModel):
    """
    Representa los metadatos de una empresa o sociedad.
    
    Esta clase modela la información extraída de las fuentes oficiales
    sobre empresas y sociedades, incluyendo su identificación, datos
    de registro y fechas relevantes.
    """
    rut: Optional[str]
    razon_social: str
    url: Optional[str]
    actuacion: str
    nro_atencion: Optional[str]
    cve: str
    fecha: datetime = Field(default_factory=datetime.now)
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
            "rut": self.rut,
            "razon_social": self.razon_social,
            "url": self.url,
            "actuacion": self.actuacion,
            "nro_atencion": self.nro_atencion,
            "cve": self.cve,
            "fecha": self.fecha.strftime("%Y-%m-%d %H:%M:%S"),
            "fecha_actuacion": self.fecha_actuacion.strftime("%Y-%m-%d %H:%M:%S")
        }
