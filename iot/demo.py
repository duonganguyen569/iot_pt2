"""Offline functional demo: calls the same ingestion and processing code, bypasses MQTT."""
import argparse
import json
import random
import time
import uuid
from iot import storage
from iot.simulator import sample
from iot.ingest import ingest
from iot.worker import cycle


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--count', type=int, default=600)
    p.add_argument('--seed', type=int, default=42)
    a = p.parse_args()
    if not 1 <= a.count <= 100000:
        p.error('count must be 1..100000')
    storage.init()
    run = uuid.uuid4().hex
    rng = random.Random(a.seed)
    start = time.time()-a.count*2-10
    for seq in range(a.count):
        sent = start+seq*2
        m = sample(seq, run, sent, rng)
        if seq % 31 == 10:
            continue
        payload = json.dumps(m) if seq % 41 != 20 else '{bad json'
        delay = .8 if seq % 37 == 15 else rng.uniform(.005, .050)
        ingest(payload, sent+delay)
        if seq % 43 == 16:
            ingest(payload, sent+delay+.001)
    cycle()
    storage.set_state('storage_mode', 'OFFLINE demo: synthetic latency, no MQTT or InfluxDB')
    print(json.dumps({'run_id': run, 'events_total': storage.event_counts()}, indent=2))
    print('Next: python -m streamlit run dashboard.py')


if __name__ == '__main__':
    main()
