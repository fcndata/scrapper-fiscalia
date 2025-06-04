from pydantic import BaseModel, Field
from pathlib import Path
import datetime
from typing import Optional
import time

class CompanyMetadata(BaseModel):
    ''' Represents metadata for a company. '''
    rut: Optional[str]
    razon_social: str
    url: Optional[str]
    actuacion: str
    nro_atencion: Optional[str]
    cve: str
    fecha: datetime = Field(default_factory=datetime.datetime.now)
    fecha_actuacion: datetime.datetime

    class Config:
        arbitrary_types_allowed = True

    def serialize(self) -> dict:
        """
        Serializa el objeto a dict para exportación.
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
