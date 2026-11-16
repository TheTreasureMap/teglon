"""Status records for background runs started from the API: `planned` runs (POST /planned_runs) and `setup` runs
(an instrument Teglon hasn't tiled yet for an event, started by /suggested_pointings).

Each run is a small JSON file, written by the API when the run is queued and updated by the worker, so status calls
don't depend on Celery's `rpc://` result backend. Standard library only, so the API can import it cheaply.
"""

import fcntl
import json
import os
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

RUNS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "events", "_planned_runs")
TASK_ID_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")
ACTIVE_STATES = ("PENDING", "STARTED")
ACTIVE_MAX_AGE = timedelta(hours=2)   # an "active" run older than this is assumed dead (e.g. worker restarted)
FAILURE_HOLD = timedelta(hours=1)     # a failed setup is reported back for this long instead of retried per click


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _age(timestamp):
    return datetime.now(timezone.utc) - datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _path(task_id):
    return os.path.join(RUNS_DIR, "%s.json" % task_id)


def read_record(task_id):
    """A run's record, or None for an unknown or malformed task id."""
    if not TASK_ID_RE.match(task_id or ""):
        return None
    try:
        with open(_path(task_id)) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def update_record(task_id, **fields):
    record = read_record(task_id) or {"task_id": task_id}
    record.update(fields)
    os.makedirs(RUNS_DIR, exist_ok=True)
    with open(_path(task_id) + ".tmp", "w") as fh:
        json.dump(record, fh)
    os.replace(_path(task_id) + ".tmp", _path(task_id))   # atomic: readers never see half a file
    return record


def discard(task_id):
    os.remove(_path(task_id))


def new_record(kind, graceid, instrument_id, state="PENDING"):
    now = now_iso()
    return update_record(str(uuid.uuid4()), kind=kind, graceid=graceid, instrument_id=instrument_id, state=state,
                         started_at=now, finished_at=now if state == "SUCCESS" else None, error=None)


def _latest(kind, graceid, instrument_id):
    """The newest run of this kind for this event and instrument, or None."""
    records = [read_record(f[:-5]) for f in os.listdir(RUNS_DIR) if f.endswith(".json")] \
        if os.path.isdir(RUNS_DIR) else []
    records = [r for r in records if r and (r.get("kind"), r.get("graceid"), r.get("instrument_id"))
               == (kind, graceid, instrument_id)]
    return max(records, key=lambda r: r["started_at"], default=None)


def find_active(kind, graceid, instrument_id):
    record = _latest(kind, graceid, instrument_id)
    if record and record["state"] in ACTIVE_STATES and _age(record["started_at"]) < ACTIVE_MAX_AGE:
        return record
    return None


def recent_failure(kind, graceid, instrument_id):
    record = _latest(kind, graceid, instrument_id)
    if record and record["state"] == "FAILURE" and _age(record["finished_at"]) < FAILURE_HOLD:
        return record
    return None


def public_view(record):
    return {k: record.get(k) for k in ("task_id", "kind", "graceid", "instrument_id", "state", "started_at",
                                       "finished_at", "error")}


@contextmanager
def lock(name):
    """Serialize work on one event (runs clear and reload its planned pointings and link tiles to its map) or on
    adding one instrument (so two events asking at once don't add it twice)."""
    os.makedirs(RUNS_DIR, exist_ok=True)
    with open(os.path.join(RUNS_DIR, "%s.lock" % name), "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        yield
