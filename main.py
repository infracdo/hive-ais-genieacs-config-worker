"""
GenieACS Configuration Worker

Listens to genieacs_config Kafka topic and applies configurations
to devices via GenieACS TR-069. Supports multiple event types with
dynamic data handling.
"""
import os
import json
import logging
import requests
from typing import Dict, Any, Optional
from kafka import KafkaConsumer, KafkaProducer
import genieacs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GenieACSConfigWorker:
    """Worker to apply configurations to devices via GenieACS"""
    
    def __init__(self):
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '10.42.4.19:9092')
        self.kafka_input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'genieacs_config')
        self.kafka_success_topic = os.getenv('KAFKA_SUCCESS_TOPIC', 'genieacs_configured')
        self.kafka_error_topic = os.getenv('KAFKA_ERROR_TOPIC', 'genieacs_config_error')
        self.kafka_consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'genieacs-config-worker')
        
        # GenieACS Configuration
        self.genieacs_host = os.getenv('GENIEACS_HOST', '10.42.4.3')
        self.genieacs_user = os.getenv('GENIEACS_USER', 'onu')
        self.genieacs_password = os.getenv('GENIEACS_PASSWORD', 'onu')
        self.genieacs_ssl = os.getenv('GENIEACS_SSL', 'false').lower() == 'true'
        
        # Gotify Configuration
        self.gotify_url = os.getenv('GOTIFY_URL', 'https://gotify.mcandres.com')
        self.gotify_token = os.getenv('GOTIFY_TOKEN', 'AfvvshNxqj6wk59')
        
        # WiFi Configuration Paths
        self.wifi_2g_ssid_path = os.getenv('WIFI_2G_SSID_PATH',
            'InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.SSID')
        self.wifi_2g_password_path = os.getenv('WIFI_2G_PASSWORD_PATH',
            'InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.KeyPassphrase')
        self.wifi_5g_ssid_path = os.getenv('WIFI_5G_SSID_PATH',
            'InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.SSID')
        self.wifi_5g_password_path = os.getenv('WIFI_5G_PASSWORD_PATH',
            'InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.KeyPassphrase')
        
        # Admin Password Configuration Path
        self.admin_password_path = os.getenv('ADMIN_PASSWORD_PATH',
            'InternetGatewayDevice.DeviceInfo.X_CT-COM_TeleComAccount.Password')
        
        self.consumer = None
        self.producer = None
        self.genieacs_connection = None
        
        logger.info("GenieACS Configuration Worker initialized")
        logger.info(f"Kafka Bootstrap: {self.kafka_bootstrap_servers}")
        logger.info(f"Input Topic: {self.kafka_input_topic}")
        logger.info(f"GenieACS Host: {self.genieacs_host}")
    
    def connect_kafka(self):
        """Initialize Kafka consumer and producer"""
        logger.info("Connecting to Kafka...")
        
        self.consumer = KafkaConsumer(
            self.kafka_input_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info("✅ Kafka connected successfully")
    
    def connect_genieacs(self):
        """Initialize GenieACS connection"""
        logger.info("Connecting to GenieACS...")
        
        self.genieacs_connection = genieacs.Connection(
            self.genieacs_host,
            ssl=self.genieacs_ssl,
            auth=True,
            user=self.genieacs_user,
            passwd=self.genieacs_password
        )
        
        logger.info("✅ GenieACS connected successfully")
    
    def send_gotify_notification(self, title: str, message: str, priority: int = 5, extras: Optional[Dict] = None):
        """Send notification to Gotify"""
        try:
            url = f"{self.gotify_url}/message?token={self.gotify_token}"
            payload = {
                "title": title,
                "message": message,
                "priority": priority
            }
            if extras:
                payload["extras"] = extras
            
            response = requests.post(url, json=payload, timeout=5)
            response.raise_for_status()
            logger.info(f"✅ Gotify notification sent: {title}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to send Gotify notification: {e}")
    
    def publish_to_kafka(self, topic: str, message: Dict[str, Any]):
        """Publish message to Kafka topic"""
        try:
            future = self.producer.send(topic, message)
            future.get(timeout=10)
            logger.info(f"✅ Published to topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to publish to Kafka topic '{topic}': {e}")
    
    def configure_wifi(self, device_id: str, wifi_ssid: str, wifi_password: str) -> bool:
        """Configure WiFi settings (both 2.4GHz and 5GHz) on device via GenieACS"""
        try:
            logger.info(f"Configuring WiFi on device: {device_id}")
            logger.info(f"2.4GHz SSID: {wifi_ssid}")
            logger.info(f"5GHz SSID: {wifi_ssid}-5G")
            
            # Configure 2.4GHz network
            logger.info("Setting up 2.4GHz network...")

            wifi_2g_password_path = self.wifi_2g_password_path
            if "HKZ28B" in device_id.upper():
                wifi_2g_password_path = "InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.PreSharedKey.1.KeyPassphrase"
            self.genieacs_connection.task_set_parameter_values(device_id, [
                [self.wifi_2g_ssid_path, wifi_ssid],
                [wifi_2g_password_path, wifi_password]
            ])
            logger.info("✅ 2.4GHz configuration task sent")
            
            # Configure 5GHz network
            logger.info("Setting up 5GHz network...")
            ssid_5g = f"{wifi_ssid}-5G"
            wifi_5g_password_path = self.wifi_5g_password_path
            if "HKZ28B" in device_id.upper():
                wifi_5g_password_path = "InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.PreSharedKey.1.KeyPassphrase"
                ssid_5g = f"{wifi_ssid} 5G"
            self.genieacs_connection.task_set_parameter_values(device_id, [
                [self.wifi_5g_ssid_path, ssid_5g],
                [wifi_5g_password_path, wifi_password]
            ])
            logger.info("✅ 5GHz configuration task sent")
            
            logger.info(f"✅ WiFi configuration tasks sent to GenieACS for {device_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to configure WiFi on {device_id}: {e}")
            return False
    
    def configure_admin_password(self, device_id: str, admin_password: str) -> bool:
        """Configure admin password on device via GenieACS"""
        try:
            logger.info(f"Configuring admin password on device: {device_id}")
            
            # Configure admin password
            logger.info("Setting admin password...")
            admin_password_path = self.admin_password_path
            if "HKZ28B" in device_id.upper():
                admin_password_path = "InternetGatewayDevice.ManagementServer.ConnectionRequestPassword"
            self.genieacs_connection.task_set_parameter_values(device_id, [
                [admin_password_path, admin_password]
            ])
            logger.info("✅ Admin password configuration task sent")
            
            logger.info(f"✅ Admin password configuration task sent to GenieACS for {device_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to configure admin password on {device_id}: {e}")
            return False
    
    def handle_wifi_config_event(self, message: Dict[str, Any]) -> bool:
        """Handle wifi_config event"""
        try:
            # Extract required data
            device_id = message.get('acs_device_id')
            wifi_ssid = message.get('wifi_ssid')
            wifi_password = message.get('wifi_password')
            
            if not device_id or not wifi_ssid or not wifi_password:
                logger.error("Missing required fields for wifi_config event")
                return False
            
            # Configure WiFi
            return self.configure_wifi(device_id, wifi_ssid, wifi_password)
            
        except Exception as e:
            logger.error(f"Error handling wifi_config event: {e}")
            return False
    
    def handle_change_admin_password_event(self, message: Dict[str, Any]) -> bool:
        """Handle change_admin_password event"""
        try:
            # Extract required data
            device_id = message.get('acs_device_id')
            admin_password = message.get('admin_password')
            
            if not device_id or not admin_password:
                logger.error("Missing required fields for change_admin_password event")
                return False
            
            # Configure admin password
            return self.configure_admin_password(device_id, admin_password)
            
        except Exception as e:
            logger.error(f"Error handling change_admin_password event: {e}")
            return False
    
    def process_message(self, message: Dict[str, Any]):
        """Process a single configuration message"""
        try:
            logger.info("=" * 80)
            logger.info("📡 New GenieACS Configuration Event")
            logger.info("=" * 80)
            
            # Extract common fields
            event = message.get('event')
            timestamp = message.get('timestamp')
            device_id = message.get('acs_device_id')
            username = message.get('username')
            serial_number = message.get('serial_number')
            
            if not event:
                error_msg = "Missing 'event' field in message"
                logger.error(f"❌ {error_msg}")
                return
            
            logger.info(f"📋 Event Type: {event}")
            logger.info(f"📱 Device ID: {device_id}")
            logger.info(f"👤 Username: {username}")
            logger.info(f"🔢 Serial Number: {serial_number}")
            logger.info(f"⏰ Timestamp: {timestamp}")
            
            # Route to appropriate handler based on event type
            success = False
            
            if event == "wifi_config":
                logger.info("Processing WiFi configuration...")
                success = self.handle_wifi_config_event(message)
            elif event == "change_admin_password":
                logger.info("Processing admin password change...")
                success = self.handle_change_admin_password_event(message)
            else:
                logger.warning(f"⚠️ Unknown event type: {event}")
                error_msg = f"Unknown event type: {event}"
                
                # Send error notification
                self.send_gotify_notification(
                    title="❌ Unknown Configuration Event",
                    message=f"Event: {event}\n"
                            f"Device: {device_id}\n"
                            f"Error: Event type not supported",
                    priority=8,
                    extras={
                        "event": event,
                        "deviceId": device_id,
                        "status": "unknown_event"
                    }
                )
                
                # Publish to error topic
                error_payload = {
                    **message,
                    "error": error_msg,
                    "error_type": "unknown_event"
                }
                self.publish_to_kafka(self.kafka_error_topic, error_payload)
                return
            
            if not success:
                error_msg = f"Failed to apply {event} configuration on device: {device_id}"
                logger.error(f"❌ {error_msg}")
                
                # Send error notification
                self.send_gotify_notification(
                    title=f"❌ Configuration Failed ({event})",
                    message=f"Event: {event}\n"
                            f"Device: {device_id}\n"
                            f"Username: {username}\n"
                            f"Error: Failed to apply configuration via GenieACS",
                    priority=8,
                    extras={
                        "event": event,
                        "deviceId": device_id,
                        "username": username,
                        "status": "configuration_failed"
                    }
                )
                
                # Publish to error topic
                error_payload = {
                    **message,
                    "error": error_msg,
                    "error_type": "genieacs_configuration_failed"
                }
                self.publish_to_kafka(self.kafka_error_topic, error_payload)
                return
            
            # Success - publish to success topic
            success_payload = {
                **message,
                "configuration_status": "success",
                "configured_at": timestamp
            }
            self.publish_to_kafka(self.kafka_success_topic, success_payload)
            
            # Send success notification
            event_details = ""
            if event == "wifi_config":
                wifi_ssid = message.get('wifi_ssid')
                event_details = f"SSID 2.4GHz: {wifi_ssid}\nSSID 5GHz: {wifi_ssid}-5G"
            elif event == "change_admin_password":
                event_details = "Admin password updated successfully"
            
            self.send_gotify_notification(
                title=f"✅ Configuration Applied ({event})",
                message=f"Event: {event}\n"
                        f"Device: {device_id}\n"
                        f"Username: {username}\n"
                        f"{event_details}\n"
                        f"Status: Configuration applied via GenieACS",
                priority=5,
                extras={
                    "event": event,
                    "deviceId": device_id,
                    "username": username,
                    "status": "success"
                }
            )
            
            logger.info("=" * 80)
            logger.info(f"✅ {event} Configuration Process Completed Successfully")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.exception(e)
    
    def run(self):
        """Main worker loop"""
        logger.info("🚀 Starting GenieACS Configuration Worker...")
        
        try:
            # Initialize connections
            self.connect_kafka()
            self.connect_genieacs()
            
            logger.info(f"👂 Listening to topic: {self.kafka_input_topic}")
            logger.info("=" * 80)
            
            # Process messages
            for kafka_message in self.consumer:
                try:
                    message = kafka_message.value
                    self.process_message(message)
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    logger.exception(e)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down worker...")
        except Exception as e:
            logger.error(f"Fatal error in worker: {e}")
            logger.exception(e)
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            logger.info("👋 Worker stopped")


if __name__ == "__main__":
    worker = GenieACSConfigWorker()
    worker.run()
