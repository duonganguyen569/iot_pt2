import time
from iot import storage
from iot.validation import validate


def ingest(payload, received=None):
    received = time.time() if received is None else received
    try:
        m = validate(payload, received)
    except (ValueError, TypeError, UnicodeError) as exc:
        storage.event('invalid', str(exc), received)
        return 'invalid'
    return 'accepted' if storage.insert_raw(m, received) else 'duplicate'
