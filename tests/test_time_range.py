import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
import pandas as pd
from iot import storage
from iot.ingest import ingest
from iot.influx import Sink, decode_raw
from iot.worker import cycle
from iot.time_range import bounds


class ReadTests(unittest.TestCase):
    def frame(self, times, device='a'):
        return pd.DataFrame({'_time': pd.to_datetime(times, utc=True),
                             'device_id': device, 'run_id': 'run',
                             'seq': [str(i) for i in range(len(times))],
                             'received_at': 1., 'latency_ms': 2.,
                             'temperature': 28., 'temperature_missing': False,
                             'humidity_missing': True, 'light': 500., 'light_missing': False})

    def test_query_tables_missing_sensor_and_exclusive_stop(self):
        a=self.frame(['2026-10-07T00:00:00Z','2026-10-07T00:00:10Z'])
        b=self.frame(['2026-10-07T00:00:00Z'],device='b')
        result=decode_raw([a,b], '2026-10-07T00:00:00Z','2026-10-07T00:00:10Z')
        self.assertEqual(len(result),2)
        self.assertEqual(set(result.device_id),{'a','b'})
        self.assertTrue(result.humidity.isna().all())
        self.assertEqual(result.seq.tolist(),[0,0])
        self.assertEqual(result.sent_at.iloc[0],pd.Timestamp('2026-10-07T00:00:00Z').timestamp())
        self.assertTrue(decode_raw([], '2026-10-07T00:00:00Z','2026-10-07T00:00:10Z').empty)

    def test_parameter_binding_and_timestamp_units(self):
        sink=Sink.__new__(Sink);sink.client=Mock()
        f=self.frame(['2026-10-07T00:00:00Z'])
        f['_time']=f['_time'].astype('datetime64[us, UTC]')
        sink.client.query_api.return_value.query_data_frame.return_value=f
        result=sink.read_raw('2026-10-07T07:00:00+07:00','2026-10-07T07:00:10+07:00')
        call=sink.client.query_api.return_value.query_data_frame.call_args
        self.assertEqual(call.kwargs['params']['query_start'],'2026-10-07T00:00:00+00:00')
        self.assertIn('range(start:',call.args[0])
        self.assertEqual(result.sent_at.iloc[0],pd.Timestamp('2026-10-07T00:00:00Z').timestamp())

    def test_invalid_ranges_and_incomplete_records(self):
        for a,b in [('2026-10-07','2026-10-08'),('2026-10-07T00:00:10Z','2026-10-07T00:00:00Z'),('2026-10-07T00:00:01Z','2026-10-07T00:00:10Z')]:
            with self.assertRaises(ValueError):bounds(a,b,aligned=True)
        f=self.frame(['2026-10-07T00:00:00Z']).drop(columns='temperature')
        with self.assertRaises(ValueError):decode_raw(f,'2026-10-07T00:00:00Z','2026-10-07T00:00:10Z')


class WorkerRangeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.old=storage.DB
        storage.DB=Path(self.tmp.name)/'test.db';storage.init()
        self.base=pd.Timestamp('2026-10-07T00:00:00Z')
        for i in range(30):
            ts=self.base.timestamp()+i*10
            ingest(json.dumps(dict(device_id='a',run_id='run',seq=i,sent_at=ts,temperature=20+i*.1,humidity=65.,light=500.)),ts+.01)
        self.sink=Mock()
        def read(start,stop):
            r=storage.raw_frame();t=pd.to_datetime(r.sent_at,unit='s',utc=True)
            return r.loc[(t>=start)&(t<stop)].copy()
        self.sink.read_raw.side_effect=read

    def tearDown(self):
        storage.DB=self.old;self.tmp.cleanup()

    def test_influx_values_are_processing_input(self):
        frame=storage.raw_frame();frame['temperature']=42.
        self.sink.read_raw.side_effect=lambda start,stop:frame.copy()
        cycle(self.sink)
        self.assertTrue((storage.processed_frame().temperature==42.).all())
        self.assertTrue((storage.raw_frame().temperature<42.).all())
        self.assertEqual(storage.get_state('processing_source'),'InfluxDB time-range query')

    def test_interval_preserves_history_and_rolling_context(self):
        cycle(self.sink)
        before=storage.processed_frame().set_index('timestamp')
        start=self.base+pd.Timedelta(seconds=150);stop=start+pd.Timedelta(seconds=20)
        cycle(self.sink,start,stop)
        after=storage.processed_frame().set_index('timestamp')
        pd.testing.assert_frame_equal(before,after)
        written=self.sink.processed.call_args.args[0]
        self.assertEqual(written.timestamp.tolist(),['2026-10-07T00:02:30Z','2026-10-07T00:02:40Z'])
        self.assertAlmostEqual(written.temperature_rolling_mean.iloc[0],21.3)
        a,b=self.sink.read_raw.call_args.args
        self.assertEqual(a,start-pd.Timedelta(seconds=90))
        self.assertEqual(b,stop+pd.Timedelta(seconds=40))

    def test_only_selected_messages_get_first_processed_timestamp(self):
        cycle(self.sink,self.base+pd.Timedelta(seconds=150),self.base+pd.Timedelta(seconds=170))
        seen=storage.raw_frame().dropna(subset=['first_processed_at'])
        self.assertEqual(seen.seq.tolist(),[15,16])

    def test_query_failure_or_incomplete_result_preserves_view_and_retry(self):
        cycle(self.sink);before=storage.processed_frame()
        read=self.sink.read_raw.side_effect
        self.sink.read_raw.side_effect=RuntimeError('query unavailable')
        with self.assertRaises(RuntimeError):cycle(self.sink)
        pd.testing.assert_frame_equal(before,storage.processed_frame())
        self.sink.read_raw.side_effect=lambda start,stop:read(start,stop).iloc[:-1]
        with self.assertRaises(RuntimeError):cycle(self.sink)
        pd.testing.assert_frame_equal(before,storage.processed_frame())
        self.sink.read_raw.side_effect=read
        cycle(self.sink)

    def test_empty_interval_preserves_history(self):
        cycle(self.sink);before=storage.processed_frame()
        cycle(self.sink,self.base+pd.Timedelta(days=1),self.base+pd.Timedelta(days=2))
        pd.testing.assert_frame_equal(before,storage.processed_frame())


if __name__=='__main__':unittest.main()
