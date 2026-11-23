"""HTTP API that Treasure Map (gwtm) calls for suggested pointings and coverage-adjusted contours.

Serve with `uvicorn web.project:app --workers 1`. The request and response shapes are the Teglon Suggested
Pointings API spec agreed with gwtm. Every reply, errors included, is JSON; errors are {"message": ...}. Every
route except /health needs `Authorization: Bearer $TEGLON_API_TOKEN`.

Slow work (adding an instrument and building its tile grid, syncing planned pointings, extracting a table) runs as
a "run" on one background thread. Run records live in this process, so run a single worker.
"""

import hmac
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from web.src.output import tiles_json
from web.src.services import tm_tiling
from web.src.utilities.get_TM_pointings import SAFE_ID

logger = logging.getLogger("teglon.api")

ACTIVE_STATES = ("PENDING", "STARTED")
ACTIVE_MAX_AGE = timedelta(hours=2)   # an active run older than this is assumed dead
FAILURE_HOLD = timedelta(hours=1)     # a failed setup is reported back for this long instead of retried per click

# ponytail: one in-memory run queue on one thread. A single thread serialises every run, which is what stops two
# planned syncs of one event, or two adds of one instrument, from colliding. Records are lost on restart. Move to
# a shared store and k8s Jobs if users start queueing behind each other.
_executor = ThreadPoolExecutor(max_workers=1)
_runs = {}
_runs_lock = threading.Lock()


class ApiError(Exception):
    def __init__(self, status_code, message, **extra):
        self.status_code = status_code
        self.body = dict(message=message, **extra)


def _now():
    return datetime.now(timezone.utc)


def _iso(when):
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def _age(timestamp):
    return _now() - datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _public(run):
    return {k: run.get(k) for k in ("task_id", "kind", "graceid", "instrument_id", "state", "started_at",
                                    "finished_at", "error")}


def _latest(kind, graceid, instrument_id):
    matching = [r for r in _runs.values() if (r["kind"], r["graceid"], r["instrument_id"]) ==
                (kind, graceid, instrument_id)]
    return max(matching, key=lambda r: r["started_at"], default=None)


def _active(kind, graceid, instrument_id):
    run = _latest(kind, graceid, instrument_id)
    if run and run["state"] in ACTIVE_STATES and _age(run["started_at"]) < ACTIVE_MAX_AGE:
        return run
    return None


def _recent_failure(kind, graceid, instrument_id):
    run = _latest(kind, graceid, instrument_id)
    if run and run["state"] == "FAILURE" and _age(run["finished_at"]) < FAILURE_HOLD:
        return run
    return None


def _new_run(kind, graceid, instrument_id, state="PENDING"):
    now = _iso(_now())
    run = {"task_id": str(uuid.uuid4()), "kind": kind, "graceid": graceid, "instrument_id": instrument_id,
           "state": state, "started_at": now, "finished_at": now if state == "SUCCESS" else None, "error": None}
    _runs[run["task_id"]] = run
    return run


def _execute(run):
    run["state"] = "STARTED"
    try:
        tm_tiling.write_suggested_pointings(run["graceid"], run["instrument_id"],
                                            exclude_planned=run["kind"] == "planned")
    except Exception as e:
        logger.exception("Teglon run %s failed", run["task_id"])
        run.update(state="FAILURE", error=str(e), finished_at=_iso(_now()))
    else:
        run.update(state="SUCCESS", finished_at=_iso(_now()))


def _queue(kind, graceid, instrument_id):
    """(run, created): the run of this kind already going for this event and instrument, or a newly queued one."""
    with _runs_lock:
        run = _active(kind, graceid, instrument_id)
        if run:
            return run, False
        run = _new_run(kind, graceid, instrument_id)
    _executor.submit(_execute, run)
    return run, True


def _check_graceid(graceid):
    if not SAFE_ID.match(graceid):   # it goes into SQL, so only plain identifiers
        raise ApiError(400, "graceid is required and must be alphanumeric")


def _latest_map_file(graceid):
    _check_graceid(graceid)
    map_id, healpix_file = tm_tiling.latest_map(graceid)
    if map_id is None:
        raise ApiError(404, "Teglon hasn't processed this event yet")
    return healpix_file


def _table_path(graceid, instrument_id, healpix_file, exclude_planned):
    detector = tm_tiling.detector_for_instrument(instrument_id)
    return detector and tm_tiling.tile_file_path(graceid, detector[1], healpix_file, exclude_planned)


def require_token(authorization: str = Header(default="")):
    token = os.environ.get("TEGLON_API_TOKEN", "")
    if not token:
        raise ApiError(503, "TEGLON_API_TOKEN is not set on the Teglon API")
    if not hmac.compare_digest(authorization, "Bearer " + token):
        raise ApiError(401, "Missing or wrong API token")


app = FastAPI(title="Teglon API", docs_url=None, redoc_url=None)
router = APIRouter(dependencies=[Depends(require_token)])


@app.exception_handler(ApiError)
async def api_error(request: Request, e: ApiError):
    return JSONResponse(e.body, status_code=e.status_code)


@app.exception_handler(StarletteHTTPException)
async def http_error(request: Request, e: StarletteHTTPException):
    return JSONResponse({"message": e.detail}, status_code=e.status_code)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, e: RequestValidationError):
    problems = "; ".join("%s: %s" % (err["loc"][-1], err["msg"]) for err in e.errors())
    return JSONResponse({"message": "Bad request: %s" % problems}, status_code=400)


@app.exception_handler(Exception)
async def unexpected_error(request: Request, e: Exception):
    logger.exception("Request failed")
    return JSONResponse({"message": "Teglon error: %s" % e}, status_code=500)


@app.get("/health")
def health():
    """Readiness: 503 while the database is unreachable. query_db otherwise hides a DB failure as "no rows", which
    the routes would report as "Teglon hasn't processed this event yet"."""
    try:
        tm_tiling.query_db(["SELECT 1;"], raise_on_error=True)
    except Exception as e:
        raise ApiError(503, "Teglon database unavailable: %s" % e)
    return {"ok": True}


@router.get("/suggested_pointings")
def suggested_pointings(instrument_id: int, graceid: str = "", exclude_planned: bool = False):
    """The table Teglon already wrote for this instrument on the event's latest map. With no completed-only table,
    Teglon hasn't set the instrument up for this event: a setup run starts and the reply is a 404 with its run."""
    healpix_file = _latest_map_file(graceid)
    path = _table_path(graceid, instrument_id, healpix_file, exclude_planned)
    if path and os.path.exists(path):
        return dict(graceid=graceid, healpix_file=healpix_file, **tiles_json.tile_file_to_json(path))
    if exclude_planned:   # gwtm starts a planned run alongside this request, which sets the instrument up
        raise ApiError(404, "No table that excludes planned pointings yet")

    failed = _recent_failure("setup", graceid, instrument_id)
    if failed:
        raise ApiError(404, "Teglon couldn't set up this instrument: %s" % failed["error"], **_public(failed))
    run, _ = _queue("setup", graceid, instrument_id)
    raise ApiError(404, "Teglon is setting up this instrument for this event; suggested pointings should be ready "
                        "in a few minutes.", **_public(run))


@router.post("/planned_runs")
def start_planned_run(instrument_id: int, graceid: str = ""):
    """Rebuild the table that also avoids planned pointings. Nothing runs if that table was written after the last
    sync that changed the event's pointings. A run already going for the same pair is returned instead."""
    healpix_file = _latest_map_file(graceid)
    path = _table_path(graceid, instrument_id, healpix_file, exclude_planned=True)
    if path and tm_tiling.table_is_current(path, graceid):
        with _runs_lock:
            run = _new_run("planned", graceid, instrument_id, state="SUCCESS")
        return JSONResponse(dict(reused=True, **_public(run)), status_code=200)
    run, created = _queue("planned", graceid, instrument_id)
    return JSONResponse(dict(reused=False, **_public(run)), status_code=202 if created else 200)


@router.get("/planned_runs/{task_id}")
def planned_run_status(task_id: str):
    run = _runs.get(task_id)
    if run is None:
        raise ApiError(404, "No run with id %s" % task_id[:64])
    return _public(run)


@router.get("/contours")
def contours(graceid: str = "", include_planned: bool = False):
    """The event's contours with sky already covered removed: completed pointings, or completed and planned."""
    _check_graceid(graceid)
    try:
        return tm_tiling.event_contours(graceid, include_planned=include_planned)
    except LookupError as e:
        raise ApiError(404, str(e))


app.include_router(router)
