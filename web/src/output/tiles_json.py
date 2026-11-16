"""Serve the tile files `teglon extract` writes as JSON (the shape gwtm's suggested-pointings view expects)."""

import math
import os
from datetime import datetime, timezone

import numpy as np
from astropy.table import Table


def tile_file_path(gw_id, detector_name, healpix_file, exclude_planned=False):
    """The tile file `extract` writes with its default settings (4D, 0.9), which is what `tm-ingest` runs."""
    return "./web/events/%s/%s_%s_4D_0.9_%s%s.txt" % (gw_id, gw_id, detector_name, healpix_file,
                                                     "_exclplanned" if exclude_planned else "")


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
    """A tile file's rows and run settings. Rows keep the file's order (ranked by probability); `prob` is the
    file's `Prob` (the field's own probability) and `cum_prob` its `Percentile` (the running total)."""
    table = Table.read(path, format="ascii.ecsv")
    settings = table.meta.get("comments", {})
    return {
        "generated_at": datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
                                .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "detector": settings.get("TELESCOPE"),
        "prob_type": settings.get("PROB_TYPE"),
        "cum_prob": _clean(settings.get("CUM_PROB")),
        "cum_prob_cut": _clean(settings.get("CUM_PROB_CUT")),
        "exclude_observed": _clean(settings.get("EXCLUDE_OBSERVED")),
        "exclude_planned": _clean(settings.get("EXCLUDE_PLANNED")),
        "pointings": [
            {
                "field_name": _clean(row["Field_Name"]),
                "ra": _clean(row["RA"]),
                "dec": _clean(row["Dec"]),
                "prob": _clean(row["Prob"]),
                "cum_prob": _clean(row["Percentile"]),
                "ebv": _clean(row["EBV"]),
                "a_lambda": _clean(row["A_lambda"]),
                "lum_dist": _clean(row["Lum_Dist"]),
            }
            for row in table
        ],
    }
