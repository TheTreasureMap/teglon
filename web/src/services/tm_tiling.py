"""Building blocks for serving suggested pointings to Treasure Map (gwtm). Plain functions with no web framework
or task queue, so the API can call them from a request handler or a background task as it sees fit.
See docs/suggested_pointings_api.md.
"""

import os

from web.src.output.tiles_json import tile_file_path
from web.src.utilities.Database_Helpers import query_db
from web.src.utilities.get_TM_pointings import (SKIP_TM_INSTRUMENT_IDS, latest_map, pointings_changed_at,
                                                sync_pointings, tm_get, treasuremap_settings)


def detector_for_instrument(instrument_id):
    """(Detector.id, Name) of the Teglon telescope for a Treasure Map instrument id, if it has a tile grid; else None."""
    rows = query_db(["SELECT d.id, d.Name FROM Detector d WHERE d.TM_id = %d AND EXISTS "
                     "(SELECT 1 FROM StaticTile st WHERE st.Detector_id = d.id);" % int(instrument_id)])[0]
    return (int(rows[0][0]), rows[0][1]) if rows else None


def table_is_current(path, graceid):
    """True if this tile table exists and was written after the last sync that changed the event's pointings."""
    return os.path.exists(path) and os.path.getmtime(path) > (pointings_changed_at(graceid) or 0)


def ensure_telescope(instrument_id):
    """(Detector.id, Name) for a Treasure Map instrument, first adding it to Teglon and building its tile grid
    (`teglon add-telescope`) if needed. Slow the first time (minutes, longer for small fields of view).
    Raises RuntimeError for non-photometric instruments or if adding it fails."""
    from web.src.services.teglon_cli import build_parser

    detector = detector_for_instrument(instrument_id)
    if detector:
        return detector
    token, endpoint = treasuremap_settings()
    info = tm_get(endpoint, "instruments", {"api_token": token, "id": instrument_id})
    kind = str((info or [{}])[0].get("instrument_type", "")).lower()   # e.g. "photometric", "spectroscopic"
    if instrument_id in SKIP_TM_INSTRUMENT_IDS or "spectro" in kind or kind == "2":
        raise RuntimeError("Teglon only tiles photometric (UV/optical/IR) instruments.")
    args = build_parser().parse_args(["add-telescope", "--tm-detector-id", str(instrument_id)])
    if args.func(args) != 0 or detector_for_instrument(instrument_id) is None:
        raise RuntimeError("Adding instrument %d to Teglon failed; see the Teglon log." % instrument_id)
    return detector_for_instrument(instrument_id)


def write_suggested_pointings(graceid, instrument_id, exclude_planned=False):
    """Write this instrument's suggested-pointings table for the event's latest map and return its path. Sets the
    instrument up first if needed (ensure_telescope, then links its tiles to the map). With exclude_planned, first
    syncs every group's planned pointings for the event from Treasure Map and writes the `_exclplanned` table.
    Raises RuntimeError on failure."""
    from web.src.services.teglon import Teglon

    detector_id, detector_name = ensure_telescope(instrument_id)
    map_id, healpix_file = latest_map(graceid)
    if map_id is None:
        raise RuntimeError("Teglon hasn't processed this event yet")
    Teglon().link_detector_to_map(map_id, detector_id)
    if exclude_planned and sync_pointings([graceid], status="planned") != 0:
        raise RuntimeError("Syncing planned pointings from Treasure Map failed; see the Teglon log.")
    if Teglon().extract_tiles(gw_id=graceid, healpix_file=healpix_file, tele=detector_name,
                              exclude_observed=True, exclude_planned=exclude_planned) == 1:
        raise RuntimeError("Extracting tiles for %s failed; see the Teglon log." % detector_name)
    return tile_file_path(graceid, detector_name, healpix_file, exclude_planned)
