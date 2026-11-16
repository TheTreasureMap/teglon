from flask import Flask, jsonify, send_from_directory
from flask import request
import os
from celery import Celery
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

teglon_worker = Celery('teglon_worker',
                    broker='amqp://admin:mypass@rabbit:5672',
                    backend='rpc://')


@app.route('/load_map/<superevent_id>')
def call_method(superevent_id):
    app.logger.info("Invoking load_map_batch_process()")
    r = teglon_worker.send_task('web.src.services.teglon_worker_tasks.load_map_batch_process',
                                kwargs={'superevent_id': superevent_id})
    app.logger.info(r.backend)
    return r.id


@app.route('/load_status/<task_id>')
def get_status(task_id):
    status = teglon_worker.AsyncResult(task_id, app=teglon_worker)
    print("Invoking Method ")
    return "Status of the Task " + str(status.state)


# @app.route('/simple_task_result/<task_id>')
# def task_result(task_id):
#     result = simple_app.AsyncResult(task_id).result
#     return "Result of the Task " + str(result)







@app.errorhandler(Exception)
def error_as_json(e):
    """Every error as {"message": ...}: gwtm turns any non-JSON reply into a 502."""
    if isinstance(e, HTTPException):
        return jsonify(message=e.description), e.code
    app.logger.exception("Request failed")
    return jsonify(message="Teglon error: %s" % e), 500


def _event_and_instrument():
    """Validated (graceid, instrument_id, detector name or None, healpix file) for the request, or an error reply."""
    from web.src.utilities.get_TM_pointings import SAFE_ID, latest_map
    from web.src.utilities.Database_Helpers import query_db

    graceid = request.args.get("graceid", "")
    if not SAFE_ID.match(graceid):   # it goes into SQL, so only plain identifiers
        return None, (jsonify(message="graceid is required and must be alphanumeric"), 400)
    try:
        tm_id = int(request.args["instrument_id"])
    except (KeyError, ValueError):
        return None, (jsonify(message="instrument_id (int) is required"), 400)
    map_id, healpix_file = latest_map(graceid)
    if map_id is None:
        return None, (jsonify(message="Teglon hasn't processed this event yet"), 404)
    detector = query_db(["SELECT Name FROM Detector WHERE TM_id = %d;" % tm_id])[0]
    return (graceid, tm_id, detector[0][0] if detector else None, healpix_file), None


def _queue_run(kind, graceid, tm_id):
    """The `setup` or `planned` run already going for this event and instrument, or a newly queued one."""
    from web.src.services import planned_runs

    record = planned_runs.find_active(kind, graceid, tm_id)
    if record is None:
        record = planned_runs.new_record(kind, graceid, tm_id)
        try:
            teglon_worker.send_task("web.src.services.teglon_worker_tasks.%s_run" % kind, task_id=record["task_id"],
                                    kwargs={"task_id": record["task_id"], "graceid": graceid, "instrument_id": tm_id})
        except Exception:   # never queued: drop the record so the next request tries again
            planned_runs.discard(record["task_id"])
            raise
    return planned_runs.public_view(record)


@app.route("/suggested_pointings")
def suggested_pointings():
    """The suggested-pointings table Teglon already wrote for this instrument on the event's latest map, as JSON
    (query parameters graceid, instrument_id, exclude_planned). If there's no completed-only table, Teglon hasn't
    tiled the instrument for this event yet: a setup run is started and the reply is a 404 with its task_id."""
    from web.src.services import planned_runs
    from web.src.output.tiles_json import tile_file_path, tile_file_to_json

    found, error = _event_and_instrument()
    if error:
        return error
    graceid, tm_id, detector_name, healpix_file = found
    exclude_planned = request.args.get("exclude_planned", "false").lower() == "true"

    path = detector_name and tile_file_path(graceid, detector_name, healpix_file, exclude_planned)
    if path and os.path.exists(path):
        return jsonify(graceid=graceid, healpix_file=healpix_file, **tile_file_to_json(path))
    if exclude_planned:   # the planned run the UI starts alongside this request sets the instrument up if needed
        return jsonify(message="No table that excludes planned pointings yet"), 404

    failed = planned_runs.recent_failure("setup", graceid, tm_id)
    if failed:
        return jsonify(message="Teglon couldn't set up this instrument: %s" % failed["error"],
                       **planned_runs.public_view(failed)), 404
    return jsonify(message="Teglon is setting up this instrument for this event; suggested pointings should be "
                           "ready in a few minutes.", **_queue_run("setup", graceid, tm_id)), 404


@app.route("/planned_runs", methods=["POST"])
def start_planned_run():
    """Start a planned-pointings run (query parameters graceid, instrument_id). If this instrument's `_exclplanned`
    table was written after the last sync that changed the event's pointings, nothing runs: the reply is already
    SUCCESS. A run already going for the same pair is returned instead of starting another."""
    from web.src.services import planned_runs
    from web.src.output.tiles_json import tile_file_path
    from web.src.utilities.get_TM_pointings import pointings_changed_at

    found, error = _event_and_instrument()
    if error:
        return error
    graceid, tm_id, detector_name, healpix_file = found

    table = detector_name and tile_file_path(graceid, detector_name, healpix_file, exclude_planned=True)
    if table and os.path.exists(table) and os.path.getmtime(table) > (pointings_changed_at(graceid) or 0):
        return jsonify(planned_runs.public_view(planned_runs.new_record("planned", graceid, tm_id, "SUCCESS"))), 200
    run = _queue_run("planned", graceid, tm_id)
    return jsonify(run), 202 if run["state"] == "PENDING" else 200


@app.route("/planned_runs/<task_id>")
def planned_run_status(task_id):
    """Status of a planned or setup run."""
    from web.src.services import planned_runs

    record = planned_runs.read_record(task_id)
    if record is None:
        return jsonify(message="No run with id %s" % task_id[:64]), 404
    return jsonify(planned_runs.public_view(record))


@app.route("/")
def hello_world():
    test = 1
    return jsonify(hello="world")





# @app.route("/")
# def load_map():
#     test = 1
#
#     # 1. Do work
#     # 2. What to return?
#
#     pass
#     # return jsonify(hello="world")
#
# @app.route("/static/<path:filename>")
# def staticfiles(path, filename):
#     test = 1
#     return send_from_directory("/app/web/events/%s" % path, filename)
#
# @app.route("/get_tiles", methods=["GET"])
# def get_tiles():
#     # def extract_tiles(gw_id, healpix_file, s_band='r', t_band='r', g_band='r', s_exp_time=60.0, t_exp_time=120.0,
#     #                   g_exp_time=120.0, extinct=0.5, prob_type='4D', s_cum_prob=0.9, t_cum_prob=0.9, s_min_ra=-1,
#     #                   s_max_ra=-1, s_min_dec=-1, s_max_dec=-1, t_min_ra=-1, t_max_ra=-1, t_min_dec=-1, t_max_dec=-1,
#     #                   g_min_ra=-1, g_max_ra=-1, g_min_dec=-1, g_max_dec=-1, galaxies_detector='NICKEL', num_gal=10000):
#
#     telescope = request.args.get('telescope')
#
#     gw_id = request.args.get('gw_id')
#     healpix_file = request.args.get('healpix_file')
#     prob_type = request.args.get('prob_type')
#     try:
#         s_cum_prob = float(request.args.get('s_cum_prob'))
#         t_cum_prob = float(request.args.get('t_cum_prob'))
#     except:
#         return jsonify({
#             gw_id: gw_id,
#             healpix_file: healpix_file,
#             telescope: telescope,
#             msg: "Wrong s_cum_prob/t_cum_prob type (must be a float)"
#         })
#
#     csv_output_path = extract_tiles(gw_id=gw_id, healpix_file=healpix_file, prob_type=prob_type, s_cum_prob=s_cum_prob,
#                   t_cum_prob=t_cum_prob)
#
#     output_tokens = csv_output_path.split("/")
#
#     return jsonify({
#         "gw_id": gw_id,
#         "healpix_file": healpix_file,
#         "telescope": telescope,
#         "file_output": "http://0.0.0.0:1337/static/%s/%s" % (output_tokens[-2], output_tokens[-1])
#     })