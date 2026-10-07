import os
import paho.mqtt.client as mqtt


def client(client_id, persistent=False, manual_ack=False):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                    clean_session=not persistent, manual_ack=manual_ack)
    if os.getenv('MQTT_USERNAME'):
        c.username_pw_set(os.environ['MQTT_USERNAME'], os.getenv('MQTT_PASSWORD'))
    if os.getenv('MQTT_TLS', '0') == '1':
        c.tls_set()
    c.reconnect_delay_set(min_delay=1, max_delay=30)
    return c
