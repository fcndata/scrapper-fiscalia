from pathlib import Path
from typing import Any, Dict, Optional, Union, TypeVar, Type

import yaml


T = TypeVar('T')


class Config:
    """
    Singleton para gestionar la configuración del sistema.
    
    Esta clase implementa el patrón Singleton para proporcionar
    acceso centralizado a la configuración del sistema cargada
    desde un archivo YAML.
    """
    _instance = None
    _config = None
    
    def __new__(cls: Type[T], config_path: Path = Path("config/config.yaml")) -> T:
        """
        Implementa el patrón Singleton.
        
        Args:
            config_path: Ruta al archivo de configuración YAML.
            
        Returns:
            Instancia única de Config.
        """
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance
    
    def _load_config(self, config_path: Path) -> None:
        """
        Carga la configuración desde el archivo YAML.
        
        Args:
            config_path: Ruta al archivo de configuración.
            
        Raises:
            FileNotFoundError: Si el archivo de configuración no existe.
        """
        self.path = Path(config_path)
        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")
        with open(self.path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f) or {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtiene un valor de configuración usando notación de punto.
        
        Permite acceder a configuraciones anidadas usando notación de punto,
        por ejemplo: config.get("urls.sociedades")
        
        Args:
            key: Clave de configuración con notación de punto.
            default: Valor por defecto si la clave no existe.
            
        Returns:
            Valor de configuración o el valor por defecto.
        """
        keys = key.split(".")
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value

    def __getitem__(self, key: str) -> Any:
        """
        Permite acceso a la configuración usando la sintaxis de corchetes.
        
        Args:
            key: Clave de configuración.
            
        Returns:
            Valor de configuración.
        """
        return self.get(key)

    def __contains__(self, key: str) -> bool:
        """
        Comprueba si una clave existe en la configuración.
        
        Args:
            key: Clave de configuración a comprobar.
            
        Returns:
            True si la clave existe y no es None, False en caso contrario.
        """
        return self.get(key) is not None

    def __repr__(self) -> str:
        """
        Representación en string de la instancia Config.
        
        Returns:
            String con la representación del objeto.
        """
        return f"<Config path={self.path}>"
