import math
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from iot.config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, SENSORS
from iot.time_range import bounds


RAW_QUERY = '''
from(bucket: query_bucket)
  |> range(start: time(v: query_start), stop: time(v: query_stop))
  |> filter(fn: (r) => r._measurement == "raw_sensor_data")
  |> filter(fn: (r) => contains(value: r._field, set: [
      "temperature", "humidity", "light", "received_at", "latency_ms",
      "temperature_missing", "humidity_missing", "light_missing"]))
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["device_id", "run_id"])
  |> sort(columns: ["_time", "seq"])
'''


def decode_raw(result, start, stop):
    """Normalize one or many Flux tables, including sensors missing in every row."""
    start, stop = bounds(start, stop)
    frames = result if isinstance(result, list) else [result]
    frames = [f for f in frames if not f.empty]
    columns = ['device_id', 'run_id', 'seq', 'sent_at', 'received_at',
               'latency_ms', *SENSORS]
    if not frames:
        return pd.DataFrame(columns=columns)
    df = pd.concat(frames, ignore_index=True)
    required = {'_time', 'device_id', 'run_id', 'seq', 'received_at', 'latency_ms'}
    if not required.issubset(df.columns):
        raise ValueError('Incomplete raw query schema')
    stamps = pd.to_datetime(df['_time'], utc=True)
    df = df.loc[(stamps >= start) & (stamps < stop)].copy()
    df['sent_at'] = stamps.loc[df.index].astype('datetime64[ns, UTC]').astype('int64') / 1e9
    df['seq'] = pd.to_numeric(df['seq'], errors='raise').astype('int64')
    for sensor in SENSORS:
        df[sensor] = pd.to_numeric(df.get(sensor, float('nan')), errors='raise')
        flag = sensor + '_missing'
        if flag not in df:
            raise ValueError('Missing validity field: ' + flag)
        if df[flag].isna().any():
            raise ValueError('Incomplete missing-value flags: ' + flag)
        df.loc[df[flag].eq(True), sensor] = float('nan')
        if (df[flag].eq(False) & df[sensor].isna()).any():
            raise ValueError('Incomplete numeric field: ' + sensor)
    if df.duplicated(['device_id', 'run_id', 'seq']).any():
        raise ValueError('Duplicate device/session/sequence in InfluxDB query')
    return df[columns].sort_values(['sent_at', 'device_id', 'run_id', 'seq']).reset_index(drop=True)


class Sink:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)
        self.writer = self.client.write_api(write_options=SYNCHRONOUS)

    def read_raw(self, start, stop):
        start, stop = bounds(start, stop)
        result = self.client.query_api().query_data_frame(
            RAW_QUERY, org=INFLUX_ORG,
            params={'query_bucket': INFLUX_BUCKET,
                    'query_start': start.isoformat(), 'query_stop': stop.isoformat()})
        return decode_raw(result, start, stop)

    def write(self, records):
        for i in range(0, len(records), 500):
            self.writer.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=records[i:i+500])

    def raw(self, df):
        points = []
        for r in df.to_dict('records'):
            # seq as tag ensures two messages with identical timestamps are not merged.
            p = (Point('raw_sensor_data').tag('device_id', r['device_id']).tag('run_id', r['run_id'])
                 .tag('seq', str(r['seq'])).time(int(r['sent_at']*1e9), WritePrecision.NS)
                 .field('received_at', float(r['received_at'])).field('latency_ms', float(r['latency_ms'])))
            for s in SENSORS:
                v = r[s]
                missing = v is None or pd.isna(v)
                p.field(s+'_missing', missing)
                if not missing:
                    p.field(s, float(v))
            points.append(p)
        self.write(points)

    def processed(self, df):
        points = []
        for r in df.to_dict('records'):
            p = (Point('processed_sensor_data').tag('device_id', r['device_id'])
                 .tag('run_id', r['run_id']).time(pd.Timestamp(r['timestamp']).to_pydatetime()))
            for k,v in r.items():
                if k in ('device_id','run_id','timestamp'):
                    continue
                # Explicit validity flag + placeholder overwrites formerly valid fields
                # when a recomputed batch changes its IQR. Never interpret missing as 0.
                valid = v is not None and math.isfinite(float(v))
                p.field(k+'_valid', valid).field(k, float(v) if valid else 0.)
            points.append(p)
        self.write(points)

    def close(self):
        self.writer.close()
        self.client.close()
