import logging
import os
from iot import storage
from iot.config import MQTT_HOST, MQTT_PORT, TOPIC
from iot.ingest import ingest
from iot.mqtt import client


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    storage.init()
    c = client(os.getenv('COLLECTOR_ID', 'iot-assignment-collector'), persistent=True, manual_ack=True)

    def on_connect(c, userdata, flags, reason, properties):
        if reason.is_failure:
            logging.error('MQTT connection refused: %s', reason)
        else:
            c.subscribe(TOPIC, qos=1)
            logging.info('Subscribed %s', TOPIC)

    def on_message(c, userdata, msg):
        # Commit durable journal BEFORE ACK. A crash after commit yields a harmless duplicate.
        result = ingest(msg.payload)
        c.ack(msg.mid, msg.qos)
        logging.info('%s mid=%s', result, msg.mid)

    c.on_connect = on_connect
    c.on_message = on_message
    c.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    try:
        c.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        c.disconnect()


if __name__ == '__main__':
    main()
