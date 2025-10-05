
import os
import json
import time
import uuid
import datetime as dt
from typing import Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

EVENTS_LOG = os.path.join("data", "events.log")
LAKE_DIR = os.path.join("lake", "watch_agg")  # partitioned by minute
FLUSH_EVERY_SECS = 5
MAX_WINDOW_LAG_SECS = 10  # allow late events within 10s

def trunc_to_minute(ts: dt.datetime) -> dt.datetime:
    return ts.replace(second=0, microsecond=0)

def ensure_dirs():
    os.makedirs(os.path.dirname(EVENTS_LOG), exist_ok=True)
    os.makedirs(LAKE_DIR, exist_ok=True)

def write_parquet(window_start: dt.datetime, rows: List[Dict]):
    if not rows:
        return
    dt_str = window_start.strftime("%Y-%m-%dT%H-%M")
    out_dir = os.path.join(LAKE_DIR, f"dt={dt_str}")
    os.makedirs(out_dir, exist_ok=True)
    file_path = os.path.join(out_dir, f"part-{uuid.uuid4().hex}.parquet")
    table = pa.Table.from_pandas(pd.DataFrame(rows))
    pq.write_table(table, file_path)
    print(f"[flush] wrote {len(rows)} rows -> {file_path}")

def stream():
    ensure_dirs()
    print(f"[stream] tailing {EVENTS_LOG} ...")
    # If file not present yet, wait
    while not os.path.exists(EVENTS_LOG):
        print("[stream] waiting for events.log ...")
        time.sleep(1)

    f = open(EVENTS_LOG, "r", encoding="utf-8")
    f.seek(0, os.SEEK_END)  # tail from current end

    # windows: { window_start -> { user_id -> total_watch_seconds } }
    windows: Dict[dt.datetime, Dict[str, float]] = {}
    last_flush = time.time()

    while True:
        line = f.readline()
        if not line:
            # no new data; periodic flush
            if time.time() - last_flush > FLUSH_EVERY_SECS:
                now = dt.datetime.utcnow()
                safe_before = now - dt.timedelta(seconds=MAX_WINDOW_LAG_SECS)
                to_flush = []
                for wstart in sorted(list(windows.keys())):
                    if wstart <= trunc_to_minute(safe_before):
                        # materialize rows
                        rows = [{"window_start": wstart, "user_id": uid, "total_watch_seconds": sec}
                                for uid, sec in windows[wstart].items()]
                        write_parquet(wstart, rows)
                        to_flush.append(wstart)
                for wstart in to_flush:
                    del windows[wstart]
                last_flush = time.time()
            time.sleep(0.2)
            continue

        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
            ts = dt.datetime.fromisoformat(ev.get("ts").replace("Z","+00:00")).astimezone(dt.timezone.utc).replace(tzinfo=None)
            wstart = trunc_to_minute(ts)
            uid = str(ev.get("user_id"))
            watch_seconds = float(ev.get("watch_seconds", 0))
            if watch_seconds < 0:
                continue
            windows.setdefault(wstart, {}).setdefault(uid, 0.0)
            windows[wstart][uid] += watch_seconds
        except Exception as e:
            print(f"[warn] failed to parse/process line: {e} :: {line[:120]}")

if __name__ == "__main__":
    stream()
