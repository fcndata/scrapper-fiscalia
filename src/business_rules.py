from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd
from logs.logger import logger


class BusinessRule(ABC):
    """Clase base para reglas de negocio."""
    
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica la regla al DataFrame."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Nombre de la regla."""
        pass


class DateFormatRule(BusinessRule):
    """Formatea columnas de fecha a YYYY-MM-DD."""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d')
                logger.info(f"Formateada columna {col} a YYYY-MM-DD")
        return df
    
    @property
    def name(self) -> str:
        return f"DateFormat({', '.join(self.columns)})"


class CleanNumberRule(BusinessRule):
    """Limpia números eliminando decimales .0."""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.replace(r'\.0$', '', regex=True)
                logger.info(f"Limpiada columna {col} eliminando .0")
        return df
    
    @property
    def name(self) -> str:
        return f"CleanNumber({', '.join(self.columns)})"


class FilterRule(BusinessRule):
    """Filtra filas basado en condiciones."""
    
    def __init__(self, condition: str, description: str):
        self.condition = condition
        self.description = description
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        initial_count = len(df)
        df = df.query(self.condition)
        final_count = len(df)
        logger.info(f"Filtro '{self.description}': {initial_count} → {final_count} registros")
        return df
    
    @property
    def name(self) -> str:
        return f"Filter({self.description})"


class ExcludeValueRule(BusinessRule):
    """Excluye filas que contienen valores específicos."""
    
    def __init__(self, column: str, values: List[str]):
        self.column = column
        self.values = values
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.column not in df.columns:
            logger.warning(f"Columna {self.column} no existe")
            return df
            
        initial_count = len(df)
        df = df[~df[self.column].isin(self.values)]
        final_count = len(df)
        logger.info(f"Excluidos valores {self.values} de {self.column}: {initial_count} → {final_count} registros")
        return df
    
    @property
    def name(self) -> str:
        return f"ExcludeValue({self.column}: {self.values})"


class NotNullRule(BusinessRule):
    """Filtra filas donde las columnas no sean nulas o vacías."""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        initial_count = len(df)
        
        for col in self.columns:
            if col in df.columns:
                # Filtrar nulos, vacíos y strings vacíos
                df = df[df[col].notna() & (df[col] != '') & (df[col] != ' ')]
                
        final_count = len(df)
        logger.info(f"Filtro NotNull {self.columns}: {initial_count} → {final_count} registros")
        return df
    
    @property
    def name(self) -> str:
        return f"NotNull({', '.join(self.columns)})"


class ColumnOrderRule(BusinessRule):
    """Define el orden y selección de columnas del DataFrame."""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        # Seleccionar solo columnas que existen
        existing_columns = [col for col in self.columns if col in df.columns]
        missing_columns = [col for col in self.columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Columnas faltantes: {missing_columns}")
        
        df = df[existing_columns]
        logger.info(f"Columnas ordenadas: {len(existing_columns)} de {len(self.columns)} especificadas")
        return df
    
    @property
    def name(self) -> str:
        return f"ColumnOrder({len(self.columns)} columnas)"


class BusinessRuleEngine:
    """Motor que ejecuta reglas de negocio."""
    
    def __init__(self):
        self.rules: List[BusinessRule] = []
    
    def add_rule(self, rule: BusinessRule) -> None:
        """Agrega una regla al motor."""
        self.rules.append(rule)
    
    def apply_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica todas las reglas al DataFrame."""
        logger.info(f"Aplicando {len(self.rules)} reglas de negocio")
        
        for rule in self.rules:
            try:
                df = rule.apply(df)
                logger.info(f"✓ Regla aplicada: {rule.name}")
            except Exception as e:
                logger.error(f"✗ Error en regla {rule.name}: {e}")
        
        return df