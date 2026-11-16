from celery import Celery
from celery.utils.log import get_task_logger

from web.src.services.teglon import *

logger = get_task_logger(__name__)

app = Celery('teglon_worker_tasks',
             broker='amqp://admin:mypass@rabbit:5672',
             backend='rpc://')

@app.task()
def load_map_batch_process(superevent_id):
    logger.info('Got Request - Starting work ')

    teglon = Teglon()

    # # First load a map...
    print("loading map `%s`..." % superevent_id)
    teglon.load_map(gw_id=superevent_id, clobber=True)
    print("... Done loading `%s`!!" % superevent_id)

    print("extracting map `%s`..." % superevent_id)
    teglon.extract_tiles(gw_id=superevent_id)
    print("... Done extracting `%s`!!" % superevent_id)

    print("plotting map `%s`..." % superevent_id)
    teglon.plot_teglon(gw_id=superevent_id)
    print("... Done plotting `%s`!!" % superevent_id)

    logger.info('Work Finished ')
    return 0



def _ensure_telescope(instrument_id):
    """Teglon's (Detector.id, Name) for a Treasure Map instrument, adding it and building its tile grid first
    (`teglon add-telescope`) if Teglon doesn't have one yet."""
    from web.src.services import planned_runs
    from web.src.services.teglon_cli import build_parser
    from web.src.utilities.get_TM_pointings import SKIP_TM_INSTRUMENT_IDS, tm_get, treasuremap_settings

    def gridded():
        rows = query_db(["SELECT d.id, d.Name FROM Detector d WHERE d.TM_id = %d AND EXISTS "
                         "(SELECT 1 FROM StaticTile st WHERE st.Detector_id = d.id);" % instrument_id])[0]
        return (int(rows[0][0]), rows[0][1]) if rows else None

    with planned_runs.lock("tm_%d" % instrument_id):
        if gridded() is None:
            token, endpoint = treasuremap_settings()
            info = tm_get(endpoint, "instruments", {"api_token": token, "id": instrument_id})
            kind = str((info or [{}])[0].get("instrument_type", "")).lower()   # e.g. "photometric", "spectroscopic"
            if instrument_id in SKIP_TM_INSTRUMENT_IDS or "spectro" in kind or kind == "2":
                raise RuntimeError("Teglon only tiles photometric (UV/optical/IR) instruments.")
            args = build_parser().parse_args(["add-telescope", "--tm-detector-id", str(instrument_id)])
            if args.func(args) != 0 or gridded() is None:
                raise RuntimeError("Adding instrument %d to Teglon failed; see the Teglon worker log." % instrument_id)
        return gridded()


def _run(task_id, graceid, instrument_id, exclude_planned):
    """Set the instrument up for the event if needed (telescope, tile grid, tiles linked to the event's latest
    map), then write its suggested-pointings table. With exclude_planned, first sync every group's planned
    pointings for the event and write the `_exclplanned` table instead. Progress goes to the run's record."""
    from web.src.services import planned_runs
    from web.src.utilities.get_TM_pointings import latest_map, sync_pointings

    planned_runs.update_record(task_id, state="STARTED")
    try:
        detector_id, detector_name = _ensure_telescope(instrument_id)
        map_id, healpix_file = latest_map(graceid)
        if map_id is None:
            raise RuntimeError("Teglon hasn't processed this event yet")
        with planned_runs.lock(graceid):
            Teglon().link_detector_to_map(map_id, detector_id)
            if exclude_planned and sync_pointings([graceid], status="planned") != 0:
                raise RuntimeError("Syncing planned pointings from Treasure Map failed; see the Teglon worker log.")
            if Teglon().extract_tiles(gw_id=graceid, healpix_file=healpix_file, tele=detector_name,
                                      exclude_observed=True, exclude_planned=exclude_planned) == 1:
                raise RuntimeError("Extracting tiles for %s failed; see the Teglon worker log." % detector_name)
    except Exception as e:
        logger.exception("Teglon run %s failed", task_id)
        planned_runs.update_record(task_id, state="FAILURE", finished_at=planned_runs.now_iso(), error=str(e))
        raise
    planned_runs.update_record(task_id, state="SUCCESS", finished_at=planned_runs.now_iso())


@app.task()
def setup_run(task_id, graceid, instrument_id):
    _run(task_id, graceid, instrument_id, exclude_planned=False)


@app.task()
def planned_run(task_id, graceid, instrument_id):
    _run(task_id, graceid, instrument_id, exclude_planned=True)
