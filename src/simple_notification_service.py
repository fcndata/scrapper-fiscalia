import boto3
import json
import urllib3
from datetime import datetime
from src.weekly_stats import WeeklyStatsManager
from logs.logger import logger
from config import config
from logs.logger import logs_message, notification_structure


class SNSManager:
    """Service for retrieving lambda logs and sending process notifications."""
    
    def __init__(self, region: str = 'us-east-1'):
        """Initialize AWS clients."""
        self.sns_client = boto3.client('sns', region_name=region)
        self.date = str(datetime.now().strftime("%d-%m-%Y"))
    
    def send_business_report(self) -> str:
        """
        Send business report to Teams using Adaptive Cards format.
        
        Returns:
            Status message
        """
        logger.info("Starting send_business_report")
        http = urllib3.PoolManager()
        try:
            logger.info("Initializing WeeklyStatsManager")
            # Get weekly statistics
            stats_manager = WeeklyStatsManager(
                bucket_name=config.get('aws.s3_bucket'),
                s3_base_path=config.get('aws.s3_name')
            )
            weekly_stats = stats_manager.get_weekly_stats()
            weekly_summary = stats_manager.format_weekly_summary(weekly_stats, notification_service='sns')
            
            logger.info("Creating Adaptive Cards payload")
            # Create Adaptive Cards payload for Teams

            message = notification_structure("Consolidado Modificación de Sociedades",
                                   weekly_summary)
            
            # Send using urllib3 like the working code
            webhook_url = config.get('teams.webhook_url_negocio')
            logger.info(f"Teams webhook URL: {webhook_url}")
            
            logger.info("Encoding message to JSON")
            encoded_msg = json.dumps(message).encode('utf-8')
            logger.info(f"Message encoded, size: {len(encoded_msg)} bytes")
            
            logger.info("Sending HTTP request to Teams")
            resp = http.request(
                'POST', 
                webhook_url,
                body=encoded_msg, 
                headers={'Content-Type': 'application/json'})
            
            logger.info(f"Teams response status: {resp.status}")
            
            if resp.status in [200, 202]:
                logger.info(f"Business report sent to Teams successfully (status: {resp.status})")
                
            else:
                logger.error(f"Failed to send business report: {resp.status}")
                logger.error(f"Response data: {resp.data}")
                return f"Error: {resp.status} - {resp.data}"
                
        except Exception as e:
            logger.error(f"Exception in send_business_report: {e}")
            return f"Error: {str(e)}"
        
        logger.info("send_business_report completed successfully")
        return "Business report sent successfully"
    
    def send_logs_report(self, log_data: dict) -> str:
        """
        Send lambda logs notification to SNS topic.
        
        Args:
            log_data (dict): Log data to send.
        
        Returns:
            Status message
        """
        try:
            logger.info("Starting send_logs_report")
            http = urllib3.PoolManager()

            message = notification_structure(f"Logs de ejecución: {self.date}", logs_message(log_data))
            logger.info("Payload created successfully")
            
            # Send using urllib3 like the working code
            webhook_url = config.get('teams.webhook_url_operacion')
            logger.info(f"Teams webhook URL: {webhook_url}")
            
            logger.info("Encoding message to JSON")
            encoded_msg = json.dumps(message).encode('utf-8')
            logger.info(f"Message encoded, size: {len(encoded_msg)} bytes")
            
            logger.info("Sending HTTP request to Teams")
            resp = http.request(
                'POST', 
                webhook_url,
                body=encoded_msg, 
                headers={'Content-Type': 'application/json'})
            
            logger.info(f"Teams response status: {resp.status}")
            
            if resp.status in [200, 202]:
                logger.info(f"Logs report sent to Teams successfully (status: {resp.status})")
                
            else:
                logger.error(f"Failed to send logs report: {resp.status}")
                logger.error(f"Response data: {resp.data}")
                return f"Error: {resp.status} - {resp.data}"
        except Exception as e:
            logger.error(f"Error sending logs notification: {e}")
            return f"Error: {str(e)}"