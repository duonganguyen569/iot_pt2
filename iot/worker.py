import argparse
import logging
import time
import pandas as pd
from iot import storage
from iot.config import INFLUX_URL
from iot.preprocess import process
from iot.time_range import bounds


def cycle(sink=None, start=None, stop=None):
    if (start is None) != (stop is None):
        raise ValueError('Provide both start and stop')
    requested = bounds(start, stop, aligned=True) if start is not None else None
    raw = storage.raw_frame()
    if raw.empty:
        return
    times = pd.to_datetime(raw.sent_at, unit='s', utc=True)
    output_start, output_stop = requested or (
        times.min().floor('10s'), times.max().floor('10s') + pd.Timedelta(seconds=10))
    # Four preceding bins for rolling(5), plus interpolation context.
    query_start = output_start - pd.Timedelta(seconds=90)
    query_stop = output_stop + pd.Timedelta(seconds=40)
    expected = raw.loc[(times >= query_start) & (times < query_stop)]
    if sink:
        last = int(storage.get_state('influx_raw_id', '0'))
        pending = raw[raw.id > last]
        sink.raw(pending)
        if not pending.empty:
            storage.set_state('influx_raw_id', int(pending.id.max()))
        inputs = sink.read_raw(query_start, query_stop)
        keys = ['device_id', 'run_id', 'seq']
        wanted = pd.MultiIndex.from_frame(expected[keys])
        available = pd.MultiIndex.from_frame(inputs[keys])
        if not wanted.isin(available).all():
            raise RuntimeError('InfluxDB query is missing journal messages; keep old view and retry')
        # Only process this journal snapshot; values are read from InfluxDB.
        inputs = inputs.loc[available.isin(wanted)].copy()
        source = 'InfluxDB time-range query'
    else:
        # Explicit offline mode remains available for the demo and unit tests.
        inputs = expected
        source = 'SQLite offline time range'
    if inputs.empty:
        logging.info('No raw data in requested interval; historical view preserved')
        return
    processed = process(inputs)
    stamps = pd.to_datetime(processed.timestamp, utc=True)
    processed = processed.loc[(stamps >= output_start) & (stamps < output_stop)].copy()
    if sink:
        sink.processed(processed)
    selected_ids = raw.loc[(times >= output_start) & (times < output_stop), 'id']
    storage.merge_processed(processed, selected_ids, time.time())
    storage.set_state('worker_success_at', time.time())
    storage.set_state('worker_error', '')
    storage.set_state('storage_mode', 'MQTT / SQLite journal / InfluxDB time-range query' if sink else 'SQLite only')
    storage.set_state('processing_source', source)
    storage.set_state('query_start', query_start.isoformat())
    storage.set_state('query_stop', query_stop.isoformat())
    storage.set_state('output_start', output_start.isoformat())
    storage.set_state('output_stop', output_stop.isoformat())
    logging.info('Read source=%s range=[%s, %s) rows=%d; output=[%s, %s) bins=%d',
                 source, query_start.isoformat(), query_stop.isoformat(), len(inputs),
                 output_start.isoformat(), output_stop.isoformat(), len(processed))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--once', action='store_true')
    p.add_argument('--start', help='Inclusive UTC/offset timestamp, aligned to 10 seconds')
    p.add_argument('--stop', help='Exclusive UTC/offset timestamp, aligned to 10 seconds')
    a = p.parse_args()
    if (a.start is None) != (a.stop is None):
        p.error('--start and --stop must be supplied together')
    if a.start is not None:
        if not a.once:
            p.error('Use --once with an explicit time range')
        try:
            bounds(a.start, a.stop, aligned=True)
        except (ValueError, TypeError) as exc:
            p.error(str(exc))
    logging.basicConfig(level=logging.INFO)
    storage.init()
    sink = None
    if INFLUX_URL:
        from iot.influx import Sink
        sink = Sink()
    try:
        while True:
            try:
                cycle(sink, a.start, a.stop)
                logging.info('Processing completed')
            except Exception as exc:
                storage.set_state('worker_error', str(exc)[:300])
                logging.exception('Batch failed; retry next cycle')
                if a.once:
                    raise
            if a.once:
                break
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        if sink:
            sink.close()


if __name__ == '__main__':
    main()
