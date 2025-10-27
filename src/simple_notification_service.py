import boto3
import json
import urllib3
from src.weekly_stats import WeeklyStatsManager
from logs.logger import logger
from config import config


class SNSManager:
    """Service for retrieving lambda logs and sending process notifications."""
    
    def __init__(self, region: str = 'us-east-1'):
        """Initialize AWS clients."""
        self.sns_client = boto3.client('sns', region_name=region)
    
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
            datos = {
                "type": "message",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.2",
                            "body": [
                                {
                                    "type": "TextBlock",
                                    "text": "Consolidado Modificación de Sociedades con información del Diario Oficial y Registro de Empresas y Sociedades",
                                    "weight": "Bolder",
                                    "size": "Medium",
                                    "wrap": True
                                },
                                {
                                    "type": "TextBlock",
                                    "text": weekly_summary,
                                    "wrap": True,
                                    "fontType": "Monospace"
                                }
                            ]
                        }
                    }
                ]
            }
            logger.info("Payload created successfully")
            
            # Send using urllib3 like the working code
            webhook_url = config.get('teams.webhook_url')
            logger.info(f"Teams webhook URL: {webhook_url}")
            
            logger.info("Encoding message to JSON")
            encoded_msg = json.dumps(datos).encode('utf-8')
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
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return f"Error: {str(e)}"
        
        logger.info("send_business_report completed successfully")
        return "Business report sent successfully"