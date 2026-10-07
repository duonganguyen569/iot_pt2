import json
from pathlib import Path
from iot import storage
from iot.metrics import summarize


def main():
    storage.init()
    target = Path('data/export')
    target.mkdir(parents=True, exist_ok=True)
    raw, processed = storage.raw_frame(), storage.processed_frame()
    raw.to_csv(target/'raw.csv', index=False)
    processed.to_csv(target/'processed.csv', index=False)
    report = summarize(raw)
    report['events'] = storage.event_counts()
    report['mode'] = storage.get_state('storage_mode')
    for key in ('processing_source', 'query_start', 'query_stop', 'output_start', 'output_stop'):
        report[key] = storage.get_state(key)
    (target/'metrics.json').write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    print('Exported to', target.resolve())


if __name__ == '__main__':
    main()
