import os
from pathlib import Path

DB = Path(os.getenv('SQLITE_PATH', 'data/iot.db'))
MQTT_HOST = os.getenv('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
TOPIC = os.getenv('MQTT_TOPIC', 'assignment/sensors')
INFLUX_URL = os.getenv('INFLUX_URL', '')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN', '')
INFLUX_ORG = os.getenv('INFLUX_ORG', 'iot-lab')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET', 'sensors')
SENSORS = ('temperature', 'humidity', 'light')
# Fixed engineering ranges: min-max scaling, intentionally not learned from future data.
RANGES = {'temperature': (-20., 60.), 'humidity': (0., 100.), 'light': (0., 2000.)}
