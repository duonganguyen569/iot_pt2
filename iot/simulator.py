import argparse
import json
import math
import random
import threading
import time
import uuid
from iot.config import MQTT_HOST, MQTT_PORT, TOPIC


def sample(seq, run_id, sent_at, rng, device='python_sensor_01', faults=True):
    m = dict(device_id=device, run_id=run_id, seq=seq, sent_at=sent_at,
             temperature=round(28+2*math.sin(seq/30)+rng.gauss(0,.2), 2),
             humidity=round(65+8*math.sin(seq/45)+rng.gauss(0,1), 2),
             light=round(600+180*math.sin(seq/25)+rng.gauss(0,15), 2))
    # Deterministic scheduled faults, mutually independent and reproducible.
    if faults and seq % 17 == 8:
        m['humidity'] = None
    if faults and seq % 29 == 12:
        m['temperature'] = 95.
    return m


def main():
    from iot.mqtt import client
    p = argparse.ArgumentParser(description='MQTT sensor simulator; Ctrl+C to stop')
    p.add_argument('--interval', type=float, default=2)
    p.add_argument('--count', type=int, default=0, help='0 = forever')
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--device', default='python_sensor_01')
    p.add_argument('--clean', action='store_true', help='Disable scheduled fault injection')
    a = p.parse_args()
    if a.interval <= 0 or a.count < 0:
        p.error('interval must be >0 and count >=0')
    rng = random.Random(a.seed)
    run_id = uuid.uuid4().hex
    connected = threading.Event()
    c = client('sim-'+run_id[:12])

    def on_connect(c, u, f, reason, properties):
        if not reason.is_failure:
            connected.set()

    def on_disconnect(c, u, flags, reason, properties):
        connected.clear()

    c.on_connect, c.on_disconnect = on_connect, on_disconnect
    c.connect_async(MQTT_HOST, MQTT_PORT)
    c.loop_start()
    seq = 0
    try:
        while not a.count or seq < a.count:
            if not connected.wait(2):
                continue
            started = time.monotonic()
            m = sample(seq, run_id, time.time(), rng, a.device, not a.clean)
            if not a.clean and seq % 31 == 10:
                print('injected_skip', seq, flush=True)
            else:
                payload = json.dumps(m, allow_nan=False)
                if not a.clean and seq % 41 == 20:
                    payload = '{bad json'
                if not a.clean and seq % 37 == 15:
                    time.sleep(.8)  # sent_at already set: intentional transit delay
                copies = 2 if not a.clean and seq % 43 == 16 else 1
                for _ in range(copies):
                    info = c.publish(TOPIC, payload, qos=1, retain=False)
                    info.wait_for_publish(timeout=10)
                    if not info.is_published():
                        raise RuntimeError('Publish not acknowledged within 10 seconds')
                print('published', seq, payload, flush=True)
            seq += 1
            time.sleep(max(0, a.interval-(time.monotonic()-started)))
    except KeyboardInterrupt:
        pass
    finally:
        c.disconnect()
        c.loop_stop()


if __name__ == '__main__':
    main()
