import json
import math
import re
from iot.config import SENSORS


def validate(payload, received):
    if len(payload) > 16384:
        raise ValueError('payload exceeds 16 KiB')
    m = json.loads(payload)
    if not isinstance(m, dict):
        raise ValueError('JSON object required')
    for key in ('device_id', 'run_id'):
        if not isinstance(m.get(key), str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', m[key]):
            raise ValueError(f'invalid {key}')
    if type(m.get('seq')) is not int or not 0 <= m['seq'] <= 2**53:
        raise ValueError('seq must be non-negative integer')
    t = m.get('sent_at')
    if type(t) not in (int, float) or not math.isfinite(t) or t < 1577836800 or t > received+5:
        raise ValueError('invalid UTC epoch sent_at / clock skew > 5s')
    for sensor in SENSORS:
        v = m.get(sensor)  # Missing key and JSON null are both valid missing observations.
        if v is not None and (type(v) not in (int, float) or not math.isfinite(v) or abs(v)>1e9):
            raise ValueError(f'{sensor} must be finite number or null')
        m[sensor] = None if v is None else float(v)
    return m
