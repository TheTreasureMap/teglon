"""Serve the tile files `teglon extract` writes as JSON."""

import math
import os

import numpy as np
from astropy.table import Table


def tile_file_path(gw_id, detector_name, healpix_file, exclude_planned=False, prob_type="4D", cum_prob=0.9,
                   healpix_dir="./web/events/{GWID}"):
    """Path of the tile file `extract_tiles` writes for these settings (the defaults are what `tm-ingest` writes)."""
    suffix = "_exclplanned" if exclude_planned else ""
    return os.path.join(healpix_dir.replace("{GWID}", gw_id), "%s_%s_%s_%s_%s%s.txt"
                        % (gw_id, detector_name, prob_type, cum_prob, healpix_file, suffix))


def _clean(value):
    """Plain Python value for JSON: numpy scalars unwrapped; NaN and masked values become None."""
    if value is None or value is np.ma.masked:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def tile_file_to_json(path):
    """A tile file as JSON: its header settings, the skymap header it records, and one row per suggested
    pointing with the file's own column names, in the file's order."""
    table = Table.read(path, format="ascii.ecsv")
    return {
        "tile_file": os.path.basename(path),
        "settings": {k: _clean(v) for k, v in table.meta.get("comments", {}).items()},
        "skymap_header": {k: _clean(v) for k, v in table.meta.get("keywords", {}).items()},
        "pointings": [{col: _clean(row[col]) for col in table.colnames} for row in table],
    }
