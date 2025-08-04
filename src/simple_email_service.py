import boto3
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from typing import List, Union, Tuple, Any
from config import config
import io
from datetime import datetime


class SESManager:
    """Service for sending emails via Amazon SES."""
    
    def __init__(self, region: str = 'us-east-1'):
        """Initialize SES client."""
        self.ses_client = boto3.client('ses', region_name=region)
    
    def _create_file_buffer(self, file: Any) -> Tuple[io.BytesIO, str, str]:
        """
        Create buffer from different file types.
        
        Args:
            file: DataFrame, dict, list, or file path
            
        Returns:
            Tuple of (buffer, filename, mime_type)
        """
        buffer = io.BytesIO()
        timestamp = datetime.now().strftime("%Y-%m-%d")
        
        if isinstance(file, pd.DataFrame):
            file.to_excel(buffer, index=False, engine='openpyxl')
            filename = f"{config.get('email.file_name')}_{timestamp}.xlsx"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        elif isinstance(file, (dict, list)):
            df = pd.DataFrame(file)
            df.to_excel(buffer, index=False, engine='openpyxl')
            filename = f"reporte_fiscalia_{timestamp}.xlsx"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        elif hasattr(file, 'read'):  # File-like object
            try:
                df = pd.read_parquet(file)
            except:
                df = pd.read_csv(file)
            df.to_excel(buffer, index=False, engine='openpyxl')
            filename = f"reporte_fiscalia_{timestamp}.xlsx"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        else:
            raise ValueError(f"Unsupported file type: {type(file)}")
        
        buffer.seek(0)
        return buffer, filename, mime_type
    
    def send_report(
        self, 
        file
    ) -> str:
        """
        Send emails with a file attached via SES.
        
        Args:
            file: Any kind of file object 
                        
        Returns:
            True if email sent successfully
        """
        try:
            # Create file buffer based on input type
            file_buffer, filename, mime_type = self._create_file_buffer(file)
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = config.get("email.from", )
            msg['To'] = ', '.join(config.get("email.to", ))
            msg['Subject'] = config.get("email.subject", 'Report:')
            
            # Email body
            record_count = len(file) if hasattr(file, '__len__') else "N/A"
            body = f"""
            {config.get('email.body','')}
            
            Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            Registros procesados: {record_count}
            
            Adjunto encontrarás el archivo con los datos extraídos.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach file
            attachment = MIMEApplication(file_buffer.getvalue())
            attachment.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(attachment)
            
            # Send email
            response = self.ses_client.send_raw_email(
                Source=config.get("email.from", ),
                Destinations= config.get("email.to", ),
                RawMessage={'Data': msg.as_string()}
            )
            
            return f"Full traceback: Email sent {response}"
            
        except Exception as e:
            print(f"Error sending email: {e}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
            return f"Full traceback: {traceback.format_exc()}"