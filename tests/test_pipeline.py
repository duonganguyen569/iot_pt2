import json
import tempfile
import unittest
from pathlib import Path
import pandas as pd
from iot import storage
from iot.ingest import ingest
from iot.preprocess import process
from iot.worker import cycle
from iot.metrics import summarize


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old = storage.DB
        storage.DB = Path(self.tmp.name)/'test.db'
        storage.init()
        self.t = 1700000000.

    def tearDown(self):
        storage.DB = self.old
        self.tmp.cleanup()

    def msg(self, seq=0, **kwargs):
        m = dict(device_id='test', run_id='run', seq=seq, sent_at=self.t+seq*10,
                 temperature=28., humidity=65., light=500.)
        m.update(kwargs)
        return json.dumps(m)

    def test_validation_dedup_latency_and_session(self):
        self.assertEqual(ingest(self.msg(), self.t+.04), 'accepted')
        self.assertEqual(ingest(self.msg(), self.t+.05), 'duplicate')
        self.assertEqual(ingest(self.msg(run_id='new'), self.t+.06), 'accepted')
        for invalid in ('{bad', '[]', self.msg(temperature=True), self.msg(light=float('nan')),
                        self.msg(seq=-1), self.msg(sent_at=self.t+10)):
            self.assertEqual(ingest(invalid, self.t), 'invalid')
        self.assertAlmostEqual(storage.raw_frame().latency_ms.iloc[0], 40, places=2)
        self.assertEqual(storage.event_counts(), {'accepted':2, 'duplicate':1, 'invalid':6})

    def test_missing_outlier_and_long_gaps(self):
        for i in range(20):
            ingest(self.msg(i, temperature=95. if i == 5 else (None if i == 7 else 28.)), self.t+i*10+.01)
        result = process(storage.raw_frame())
        self.assertEqual(sum(result.temperature_outlier), 1)
        self.assertEqual(sum(result.temperature_missing), 1)
        self.assertTrue(all(v == 28. for v in result.temperature))
        self.assertEqual(sum(result.temperature_imputed), 2)
        raw = storage.raw_frame()
        raw = raw[~raw.seq.between(8,12)]
        out = process(raw)
        self.assertEqual(sum(out.empty_bin), 5)
        self.assertEqual(sum(v is None for v in out.temperature), 6)  # seq 7 null + 5 absent bins

    def test_leading_missing_and_all_missing_not_zero(self):
        for i in range(5):
            ingest(self.msg(i, temperature=None if i == 0 else 28., humidity=None), self.t+i*10+.01)
        out = process(storage.raw_frame())
        self.assertIsNone(out.temperature.iloc[0])
        self.assertTrue(all(v is None for v in out.humidity))
        storage.replace_processed(out)
        self.assertEqual(len(storage.processed_frame()), 5)

    def test_sequence_gaps_per_run_and_worker_retry(self):
        for seq in (0,2,3):
            ingest(self.msg(seq), self.t+seq*10+.01)
        self.assertEqual(summarize(storage.raw_frame())['sequence_gaps'], 1)

        class FailingSink:
            def raw(self, df):
                raise RuntimeError('network down')
        with self.assertRaises(RuntimeError):
            cycle(FailingSink())
        self.assertEqual(storage.get_state('influx_raw_id', '0'), '0')
        self.assertTrue(storage.processed_frame().empty)

        class Sink:
            def __init__(self): self.n = 0
            def raw(self, df): self.n += len(df)
            def processed(self, df): pass
            def read_raw(self, start, stop): return storage.raw_frame()
        sink = Sink()
        cycle(sink)
        cycle(sink)
        self.assertEqual(sink.n, 3)
        self.assertFalse(storage.processed_frame().empty)
        self.assertTrue(storage.raw_frame().first_processed_at.notna().all())

    def test_devices_not_mixed(self):
        for i in range(5):
            for name, temp in [('a',20.), ('b',40.)]:
                ingest(self.msg(i, device_id=name, temperature=temp), self.t+i*10+.01)
        out = process(storage.raw_frame())
        self.assertEqual(set(out[out.device_id=='a'].temperature), {20.})
        self.assertEqual(set(out[out.device_id=='b'].temperature), {40.})


if __name__ == '__main__':
    unittest.main()
