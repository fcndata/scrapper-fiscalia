from datetime import datetime, timedelta
from typing import Dict, List
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
            #%%
            from datetime import datetime, timedelta
            def week():
                today = datetime.now()
                # Si es lunes, mostrar semana anterior completa
                if today.weekday() == 0:  # 0 = lunes
                    start_date = today - timedelta(days=7)
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
                s3_key = f"{self.s3_base_path.strip('/')}/delivery/pa_date={date_str}/processed_data.parquet"
                
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
                    stats_of_the_week[day]['diario'] = dict_stats.get('diario', 0)

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
    
    def format_weekly_summary(self, stats: Dict[str, Dict[str, int]]) -> str:
        """
        Formatea resumen semanal en tabla para email.
        
        Args:
            stats: Diccionario con estadísticas diarias por fuente.
            
        Returns:
            String formateado con tabla semanal.
        """
        if not stats:
            return "No hay datos disponibles para el reporte semanal."
        
        
        # Encabezado de la tabla con anchos fijos
        header = f"{'Día':<30}{'Diario Oficial':<50}{'Registro de Empresa':<75}{'Total':<85}"
        separator = "═" * 90
        
        rows = []
        total_diario = 0
        total_sociedad = 0

        for day in stats.keys():
            sociedad = stats[day].get('sociedad')
            diario = stats[day].get('diario')

            # Formatear valores (números o guiones)
            diario_str = f"{diario:,}" if isinstance(diario, int) and diario > 0 else str(diario)
            sociedad_str = f"{sociedad:,}" if isinstance(sociedad, int) and sociedad > 0 else str(sociedad)
            
            # Calcular total solo si ambos son números
            if isinstance(diario, int) and isinstance(sociedad, int):
                total_day = diario + sociedad
                total_str = f"{total_day:,}" if total_day > 0 else "0"
                total_diario += diario
                total_sociedad += sociedad
            else:
                total_str = "-"
            
            row = f"{day:<40}{diario_str:<16}{sociedad_str:<16}{total_str:<10}"
            rows.append(row)
        
        # Fila de totales
        total_general = total_diario + total_sociedad
        total_row = f"{'TOTAL SEMANAL':<40}{total_diario:<16,}{total_sociedad:<16,}{total_general:<10,}"
        
        summary = f"""

{separator}
{header}
{separator}
{"".join(f"{row}\n" for row in rows)}{separator}
{total_row}
        """
        
        return summary.strip()