"""Durable ingress journal, deduplication and dashboard materialized view."""
import json
import sqlite3
from contextlib import contextmanager
import pandas as pd
from iot.config import DB

SCHEMA = '''
CREATE TABLE IF NOT EXISTS raw (
 id INTEGER PRIMARY KEY, device_id TEXT NOT NULL, run_id TEXT NOT NULL,
 seq INTEGER NOT NULL, sent_at REAL NOT NULL, received_at REAL NOT NULL,
 temperature REAL, humidity REAL, light REAL, latency_ms REAL NOT NULL, first_processed_at REAL,
 UNIQUE(device_id, run_id, seq));
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY, at REAL NOT NULL, kind TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS processed (
 device_id TEXT, run_id TEXT, timestamp TEXT, payload TEXT NOT NULL,
 PRIMARY KEY(device_id, run_id, timestamp));
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
'''

@contextmanager
def connect():
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute('PRAGMA busy_timeout=30000')
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def init():
    with connect() as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript(SCHEMA)
        if 'first_processed_at' not in {r[1] for r in c.execute('PRAGMA table_info(raw)')}:
            c.execute('ALTER TABLE raw ADD COLUMN first_processed_at REAL')


def event(kind, detail, at):
    with connect() as c:
        c.execute('INSERT INTO events(at,kind,detail) VALUES(?,?,?)',
                  (at, kind, str(detail)[:500]))


def insert_raw(m, received):
    cols = ('device_id', 'run_id', 'seq', 'sent_at', 'temperature', 'humidity', 'light')
    with connect() as c:
        cursor = c.execute('INSERT OR IGNORE INTO raw (' + ','.join(cols) +
                           ',received_at,latency_ms) VALUES(?,?,?,?,?,?,?,?,?)',
                           tuple(m[k] for k in cols) + (received, (received-m['sent_at'])*1000))
        inserted = cursor.rowcount == 1
        c.execute('INSERT INTO events(at,kind,detail) VALUES(?,?,?)',
                  (received, 'accepted' if inserted else 'duplicate',
                   f"{m['device_id']}/{m['run_id']}/{m['seq']}"))
        return inserted


def raw_frame():
    with connect() as c:
        return pd.read_sql_query('SELECT * FROM raw ORDER BY sent_at,id', c)


def processed_frame():
    with connect() as c:
        rows = c.execute('SELECT payload FROM processed ORDER BY timestamp').fetchall()
    return pd.DataFrame([json.loads(r[0]) for r in rows])


def replace_processed(df, max_raw_id=None, completed_at=None):
    # Atomically refresh a small assignment dataset; readers never see a partial batch.
    records = df.to_dict(orient='records')
    with connect() as c:
        c.execute('DELETE FROM processed')
        c.executemany('INSERT INTO processed VALUES(?,?,?,?)',
                      [(r['device_id'], r['run_id'], r['timestamp'],
                        json.dumps(r, allow_nan=False)) for r in records])
        if max_raw_id is not None:
            c.execute('UPDATE raw SET first_processed_at=? WHERE id<=? AND first_processed_at IS NULL',
                      (completed_at, max_raw_id))


def get_state(key, default=''):
    with connect() as c:
        row = c.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
    return row[0] if row else default


def set_state(key, value):
    with connect() as c:
        c.execute('INSERT OR REPLACE INTO state VALUES(?,?)', (key, str(value)))


def event_counts():
    with connect() as c:
        return dict(c.execute('SELECT kind,COUNT(*) FROM events GROUP BY kind').fetchall())


def merge_processed(df, raw_ids, completed_at):
    """Upsert selected bins only; preserve history outside the requested range."""
    records = df.to_dict(orient='records')
    with connect() as c:
        c.executemany('INSERT OR REPLACE INTO processed VALUES(?,?,?,?)',
                      [(r['device_id'], r['run_id'], r['timestamp'],
                        json.dumps(r, allow_nan=False)) for r in records])
        c.executemany('UPDATE raw SET first_processed_at=? '
                      'WHERE id=? AND first_processed_at IS NULL',
                      [(completed_at, int(i)) for i in raw_ids])
