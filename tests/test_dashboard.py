"""Optional UI test: requires the full requirements.txt environment."""
import json
import tempfile
import unittest
from pathlib import Path
from iot import storage
from iot.ingest import ingest
from iot.worker import cycle


class DashboardTest(unittest.TestCase):
    def test_initial_render_and_sensor_change(self):
        from streamlit.testing.v1 import AppTest
        old = storage.DB
        with tempfile.TemporaryDirectory() as folder:
            try:
                storage.DB = Path(folder)/'ui.db'
                storage.init()
                for i in range(40):
                    m = dict(device_id='ui', run_id='test', seq=i, sent_at=1700000000.+i*2,
                             temperature=28., humidity=None if i == 8 else 65., light=500.)
                    ingest(json.dumps(m), m['sent_at']+.01)
                cycle()
                app = AppTest.from_file(str(Path(__file__).resolve().parents[1]/'dashboard.py'),
                                        default_timeout=30).run()
                self.assertEqual(len(app.exception), 0)
                self.assertGreaterEqual(len(app.metric), 7)
                app.selectbox[2].select('humidity').run()
                self.assertEqual(len(app.exception), 0)
            finally:
                storage.DB = old


if __name__ == '__main__':
    unittest.main()
