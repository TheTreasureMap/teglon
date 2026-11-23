"""Building blocks for serving suggested pointings to Treasure Map (gwtm). Plain functions with no web framework
or task queue, so the API can call them from a request handler or a background task as it sees fit.
See docs/suggested_pointings_api.md.
"""

import os

from web.src.output.tiles_json import tile_file_path
from web.src.utilities.Database_Helpers import query_db
from web.src.utilities.get_TM_pointings import (SKIP_TM_INSTRUMENT_IDS, TM_SOURCE_PLANNED, latest_map,
                                                pointings_changed_at, sync_pointings, tm_get, treasuremap_settings)

CONTOUR_COLORS = {50: "#e6194B", 90: "#f58231"}


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


def coverage_contours(prob, covered, levels=(50, 90)):
    """[(level, polygons)] credible-region outlines of a RING-ordered probability map after zeroing the `covered`
    pixel indices and renormalising. Each polygon is a list of [ra, dec] in degrees. Empty if all of the map is
    covered."""
    import numpy as np
    from ligo.skymap.postprocess import contour, find_greedy_credible_levels

    p = np.array(prob, dtype=float)
    p[np.asarray(covered, dtype=int)] = 0.0
    total = p.sum()
    if total <= 0:
        return []
    credible = 100 * find_greedy_credible_levels(p / total)
    paths = contour(credible, list(levels), nest=False, degrees=True, simplify=True)
    return [(level, [[[float(ra), float(dec)] for ra, dec in poly] for poly in polys])
            for level, polys in zip(levels, paths)]


def contours_path(graceid, map_id, include_planned):
    return "./web/events/%s/%s_contours_%d_%s.json" % (graceid, graceid, map_id,
                                                       "pandc" if include_planned else "completed")


def event_contours(graceid, include_planned=False):
    """The event's 4D (galaxy-weighted) contours for its latest map with sky covered by Treasure Map pointings
    removed: completed pointings only, or completed and planned. Returns the `/contours` body
    ({"detection_overlays": [...]}, the shape gwtm's renormalize-skymap overlay uses). Cached next to the tile
    tables until a sync changes the event's pointings. Raises LookupError if Teglon hasn't loaded the event."""
    import json
    from web.src.services.teglon_cli import reconstruct_reweighted_map

    map_id, _ = latest_map(graceid)
    if map_id is None:
        raise LookupError("Teglon hasn't processed this event yet")
    path = contours_path(graceid, map_id, include_planned)
    if table_is_current(path, graceid):
        with open(path) as fh:
            return json.load(fh)

    nside = int(query_db(["SELECT RescaledNSIDE FROM HealpixMap WHERE id = %d;" % map_id])[0][0][0])
    rows = query_db(["SELECT hp.Pixel_Index, hpc.NetPixelProb FROM HealpixPixel hp "
                     "JOIN HealpixPixel_Completeness hpc ON hpc.HealpixPixel_id = hp.id "
                     "WHERE hp.HealpixMap_id = %d;" % map_id])[0]
    prob = reconstruct_reweighted_map(rows, nside, prob_index=1, pixindex_index=0)
    source_filter = "" if include_planned else \
        "AND (ot.Source IS NULL OR ot.Source <> '%s')" % TM_SOURCE_PLANNED
    covered = [int(r[0]) for r in query_db([
        "SELECT DISTINCT hp.Pixel_Index FROM ObservedTile_HealpixPixel ot_hp "
        "JOIN ObservedTile ot ON ot.id = ot_hp.ObservedTile_id "
        "JOIN HealpixPixel hp ON hp.id = ot_hp.HealpixPixel_id "
        "WHERE ot_hp.HealpixMap_id = %d %s;" % (map_id, source_filter)])[0]]

    body = {"detection_overlays": [
        {"name": "%d%%" % level, "color": CONTOUR_COLORS.get(level),
         "contours": [{"polygon": polygon} for polygon in polygons]}
        for level, polygons in coverage_contours(prob, covered)]}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as fh:
        json.dump(body, fh)
    os.replace(path + ".tmp", path)
    return body
