from datetime import datetime, timedelta
from typing import Dict, List
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import BytesIO
from logs.logger import logger


class WeeklyStatsManager:
    """Gestiona estadísticas semanales desde S3."""
    
    def __init__(self, bucket_name: str, s3_base_path: str):
        self.bucket_name = bucket_name
        self.s3_base_path = s3_base_path
        self.s3_client = boto3.client('s3')
    
    def get_weekly_stats(self) -> Dict[str, Dict[str, int]]:
        """
        Obtiene estadísticas diarias de la semana por fuente.
        
        Returns:
            Diccionario con estadísticas diarias por fuente.
        """
        try:
            def week():
                today = datetime.now()
                # Si es lunes, mostrar semana anterior completa
                if today.weekday() == 0:  # 0 = lunes
                    start_date = today 
                else:
                    # Calcular lunes de la semana actual
                    days_since_monday = today.weekday()
                    start_date = today - timedelta(days=days_since_monday)
                
                day_name = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                
                week_dict = {}
                for idx ,day in enumerate(day_name):
                    week_dict[day] = {
                        'date': start_date + timedelta(days=idx),
                        'sociedad': 0,
                        'diario': 0
                    }

                return week_dict         
            
            def retrive_stats(date):
                date_str = date.strftime('%Y-%m-%d')
                s3_key = f"{self.s3_base_path.strip('/')}/processed/pa_date={date_str}/processed_data.parquet"
                
                try:
                    df = self._read_parquet_from_s3(s3_key)
                    if df is not None:
                        return df['fuente'].value_counts().to_dict()
                    return {}
                except Exception as e:
                    logger.warning(f"Error procesando fecha {date_str}: {e}")
                    return {}
            
            stats_of_the_week = week()
            today = datetime.now()

            for day in stats_of_the_week.keys():
                date = stats_of_the_week[day].get('date')
                if date >= today:
                    stats_of_the_week[day]['sociedad'] = '-'
                    stats_of_the_week[day]['diario'] = '-'
                else:
                    dict_stats = retrive_stats(date)
                    stats_of_the_week[day]['sociedad'] = dict_stats.get('empresa', 0)
                    stats_of_the_week[day]['diario'] = dict_stats.get('diario_oficial', 0)

            logger.info(f"Estadísticas semanales generadas")
            return stats_of_the_week
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas semanales: {e}")
            return {}
    
    def _read_parquet_from_s3(self, s3_key: str) -> pd.DataFrame:
        """Lee archivo parquet desde S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            parquet_buffer = BytesIO(response['Body'].read())
            return pd.read_parquet(parquet_buffer)
        except Exception as e:
            logger.error(f"Error leyendo {s3_key}: {e}")
            return None

        
    def format_weekly_summary(self, stats: Dict[str, Dict[str, int]], notification_service: str = 'sns') -> str:
        """
        Formatea resumen semanal optimizado para Teams (45 caracteres max).
        
        Args:
            stats: Diccionario con estadísticas diarias por fuente.
            
        Returns:
            String formateado para Teams.
        """
        if not stats:
            return "No hay datos disponibles."
                # Definir anchos fijos para todas las columnas
        
        space_string = '\u00A0'
        space_column = 9
        total_diario = 0
        total_sociedad = 0
        body_lines = []
        rows = []
        
        header = f"{'Dia'.ljust(space_column,space_string)}|{'Diario'.center(space_column,space_string)}|{'Empresa'.center(space_column,space_string)}|{'Total'.center(space_column,space_string)} |"
        
        body_lines.append(header)

        for day in stats.keys():
            sociedad = stats[day].get('sociedad')
            diario = stats[day].get('diario')
            date = stats[day].get('date').strftime('%d')

            # Formatear valores
            diario_str = f"{diario}" if isinstance(diario, int) else str(diario)
            sociedad_str = f"{sociedad}" if isinstance(sociedad, int) else str(sociedad)
            
            # Calcular total
            if isinstance(diario, int) and isinstance(sociedad, int):
                total_day = diario + sociedad
                total_str = f"{total_day}"
                total_diario += diario
                total_sociedad += sociedad
            else:
                total_str = "-"
            
            # Día abreviado
            day_short = day[:3]  # Lun, Mar, Mié, etc.
            day_date = f"{day_short} {date}"
            
            row = f"{day_date.ljust(space_column,space_string)}|{diario_str.center(space_column,space_string)}|{sociedad_str.center(space_column,space_string)}|{total_str.center(space_column,space_string)} |"
            rows.append(row)

        total_general = total_diario + total_sociedad
        total_row = f"{'TOTAL'.ljust(space_column,space_string)}|{str(total_diario).center(space_column,space_string)}|{str(total_sociedad).center(space_column,space_string)}|{str(total_general).center(space_column,space_string)} |"
        
        body_lines.extend(rows)
        body_lines.append(total_row)

        line_blocks = []
        for line in body_lines:
            line_blocks.append({
                "type": "TextBlock",
                "text": line,
                "wrap": True,
                "spacing": "None",
                "fontType": "Monospace"
            })
        
        return line_blocks