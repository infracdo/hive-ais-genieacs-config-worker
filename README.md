# GenieACS Configuration Worker

Kafka consumer that automatically applies configurations to devices via GenieACS TR-069. Supports multiple event types with dynamic data handling.

## Overview

This service listens to configuration events from Kafka, processes them based on event type, and applies the configuration to devices via GenieACS.

## Workflow

1. **Listen**: Consume messages from `genieacs_config` Kafka topic
2. **Route**: Determine configuration type based on event field
3. **Configure**: Apply configuration via GenieACS TR-069
4. **Notify**: Send Gotify notifications and publish results to Kafka

## Architecture

```
API Request → Kafka (genieacs_config)
                    ↓
          GenieACS Config Worker
                    ↓
        ┌───────────┴───────────┐
        ↓                       ↓
   Event Router          GenieACS Apply Config
        ↓                       ↓
   wifi_config          Set TR-069 Parameters
   (more events...)            ↓
        └───────────┬───────────┘
                    ↓
           Gotify Notifications
                    ↓
   Kafka (genieacs_configured or genieacs_config_error)
```

## Supported Events

### 1. wifi_config

Configure WiFi settings (both 2.4GHz and 5GHz networks).

**Input:**
```json
{
  "event": "wifi_config",
  "timestamp": "2025-11-04T10:30:00.000000Z",
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9",
  "username": "MYB-779",
  "serial_number": "MHAR08F6C2D9",
  "wifi_ssid": "MyHomeWiFi",
  "wifi_password": "SecurePass123",
  "nas_ip_address": "10.42.3.28",
  "mikrotik_group": "premium"
}
```

**Configuration Applied:**
- 2.4GHz Network:
  - SSID: `MyHomeWiFi`
  - Password: `SecurePass123`
- 5GHz Network:
  - SSID: `MyHomeWiFi-5G`
  - Password: `SecurePass123`

### 2. change_admin_password

Change the admin password on the device.

**Input:**
```json
{
  "event": "change_admin_password",
  "timestamp": "2025-11-04T10:30:00.000000Z",
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9",
  "username": "MYB-779",
  "serial_number": "MHAR08F6C2D9",
  "admin_password": "NewAdminPass123",
  "nas_ip_address": "10.42.3.28",
  "mikrotik_group": "premium"
}
```

**Configuration Applied:**
- Admin Password: `NewAdminPass123`

## Kafka Topics

### Input Topic: `genieacs_config`
Configuration requests with event type and data:
```json
{
  "event": "wifi_config",
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9",
  "wifi_ssid": "MyHomeWiFi",
  "wifi_password": "SecurePass123"
}
```

### Success Topic: `genieacs_configured`
Published when configuration succeeds:
```json
{
  "event": "wifi_config",
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9",
  "configuration_status": "success",
  "configured_at": "2025-11-04T10:30:00.000000Z"
}
```

### Error Topic: `genieacs_config_error`
Published when configuration fails:
```json
{
  "event": "wifi_config",
  "acs_device_id": "E007C2-MH80-MHAR08F6C2D9",
  "error": "Failed to apply wifi_config configuration",
  "error_type": "genieacs_configuration_failed"
}
```

## GenieACS Integration

### WiFi Configuration (2.4GHz)
```python
acs.task_set_parameter_values(device_id, [
    ["InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.SSID", "MyHomeWiFi"],
    ["InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.KeyPassphrase", "SecurePass123"]
])
```

### WiFi Configuration (5GHz)
```python
acs.task_set_parameter_values(device_id, [
    ["InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.SSID", "MyHomeWiFi-5G"],
    ["InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.KeyPassphrase", "SecurePass123"]
])
```

### Admin Password Configuration
```python
acs.task_set_parameter_values(device_id, [
    ["InternetGatewayDevice.DeviceInfo.X_CUSTOM.AdminPassword", "NewAdminPass123"]
])
```

## Environment Variables

See `.env.example` for all configuration options:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `10.42.4.19:9092` | Kafka broker addresses |
| `KAFKA_INPUT_TOPIC` | `genieacs_config` | Input topic for config events |
| `KAFKA_SUCCESS_TOPIC` | `genieacs_configured` | Success output topic |
| `KAFKA_ERROR_TOPIC` | `genieacs_config_error` | Error output topic |
| `GENIEACS_HOST` | `10.42.4.3` | GenieACS server address |
| `GENIEACS_USER` | `onu` | GenieACS username |
| `GENIEACS_PASSWORD` | `onu` | GenieACS password |
| `GOTIFY_URL` | `https://gotify.mcandres.com` | Gotify server URL |
| `GOTIFY_TOKEN` | `AfvvshNxqj6wk59` | Gotify app token |
| `WIFI_2G_SSID_PATH` | `InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.SSID` | TR-069 path for 2.4GHz SSID |
| `WIFI_2G_PASSWORD_PATH` | `InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.KeyPassphrase` | TR-069 path for 2.4GHz password |
| `WIFI_5G_SSID_PATH` | `InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.SSID` | TR-069 path for 5GHz SSID |
| `WIFI_5G_PASSWORD_PATH` | `InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.KeyPassphrase` | TR-069 path for 5GHz password |
| `ADMIN_PASSWORD_PATH` | `InternetGatewayDevice.DeviceInfo.X_CUSTOM.AdminPassword` | TR-069 path for admin password |

## Setup & Run

### Local Development

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your settings
```

3. **Load environment and run**:
```bash
source load_env.sh
python main.py
```

### Docker Deployment

1. **Build and push to Docker Hub**:
```bash
./docker-build-push.sh
```

2. **Run with Docker Compose**:
```bash
docker-compose up -d
```

3. **View logs**:
```bash
docker-compose logs -f genieacs-config-worker
```

4. **Stop service**:
```bash
docker-compose down
```

## Notifications

Gotify notifications are sent for:

### Success (Priority 5)
```
✅ Configuration Applied (wifi_config)
Event: wifi_config
Device: E007C2-MH80-MHAR08F6C2D9
Username: MYB-779
SSID 2.4GHz: MyHomeWiFi
SSID 5GHz: MyHomeWiFi-5G
Status: Configuration applied via GenieACS
```

### Configuration Failed (Priority 8)
```
❌ Configuration Failed (wifi_config)
Event: wifi_config
Device: E007C2-MH80-MHAR08F6C2D9
Username: MYB-779
Error: Failed to apply configuration via GenieACS
```

### Unknown Event (Priority 8)
```
❌ Unknown Configuration Event
Event: unknown_event
Device: E007C2-MH80-MHAR08F6C2D9
Error: Event type not supported
```

## TR-069 Parameter Paths

Default paths (configurable via environment variables):

**2.4GHz WiFi:**
- SSID: `InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.SSID`
- Password: `InternetGatewayDevice.LANDevice.1.WLANConfiguration.1.KeyPassphrase`

**5GHz WiFi:**
- SSID: `InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.SSID`
- Password: `InternetGatewayDevice.LANDevice.1.WLANConfiguration.5.KeyPassphrase`

For different ONU models, adjust these paths in `.env`.

## Adding New Event Types

The worker is designed to handle multiple event types. To add a new event:

1. **Create handler method**:
```python
def handle_new_event(self, message: Dict[str, Any]) -> bool:
    """Handle new_event type"""
    # Extract required data
    device_id = message.get('acs_device_id')
    # ... extract other fields
    
    # Apply configuration via GenieACS
    # ... configuration logic
    
    return True
```

2. **Add to event router** in `process_message()`:
```python
if event == "wifi_config":
    success = self.handle_wifi_config_event(message)
elif event == "new_event":
    success = self.handle_new_event(message)
```

3. **Update environment variables** if needed for new TR-069 paths

## Monitoring

Check worker status:
```bash
docker-compose ps
docker-compose logs genieacs-config-worker
```

Monitor Kafka topics:
```bash
# Check input topic
kafka-console-consumer --bootstrap-server 10.42.4.19:9092 --topic genieacs_config

# Check success topic
kafka-console-consumer --bootstrap-server 10.42.4.19:9092 --topic genieacs_configured

# Check error topic
kafka-console-consumer --bootstrap-server 10.42.4.19:9092 --topic genieacs_config_error
```

## Troubleshooting

### Worker not processing messages
- Check Kafka connectivity: verify bootstrap servers
- Verify topic exists
- Check consumer group status

### GenieACS configuration failing
- Verify GenieACS credentials
- Check if device is connected to GenieACS
- Verify TR-069 parameter paths match your ONU model
- Check device_id format matches GenieACS device ID

### No Gotify notifications
- Verify Gotify URL and token
- Check Gotify server accessibility
- Review worker logs for errors

## Docker Hub

Image: `marcandres888/genieacs-config-worker:latest`

Pull and run:
```bash
docker pull marcandres888/genieacs-config-worker:latest
docker run -d --name genieacs-config-worker \
  -e KAFKA_BOOTSTRAP_SERVERS=10.42.4.19:9092 \
  -e GENIEACS_HOST=10.42.4.3 \
  marcandres888/genieacs-config-worker:latest
```

## Example Flow

1. User calls API: `POST /api/v1/pppoe/users/MYB-779/configwifi`
2. API publishes to Kafka topic `genieacs_config`
3. Worker consumes message
4. Worker routes to `handle_wifi_config_event()`
5. Worker configures 2.4GHz WiFi via GenieACS
6. Worker configures 5GHz WiFi via GenieACS
7. Worker publishes success to `genieacs_configured`
8. Worker sends Gotify notification
9. ONU applies new WiFi settings

## License

Part of the Apollo Device Provisioner project.
