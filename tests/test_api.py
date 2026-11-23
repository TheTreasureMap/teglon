"""Tests for the gwtm-facing HTTP API (web/project/__init__.py) and the coverage contours behind /contours.

No database or network: tm_tiling's DB-backed helpers are replaced with fakes, and runs execute on the API's
real background thread (tests wait for it by queueing a no-op behind them).
"""
import importlib.util
import json
import os
import tempfile
import threading
import unittest
from unittest import mock

from tests.test_tm_pipeline import _import_without_db

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None and importlib.util.find_spec("httpx") is not None
HAS_SKYMAP = importlib.util.find_spec("ligo") is not None and importlib.util.find_spec("healpy") is not None
if HAS_FASTAPI:
    from fastapi.testclient import TestClient
api = _import_without_db("web.project") if HAS_FASTAPI else None
tm_tiling = api.tm_tiling if HAS_FASTAPI else _import_without_db("web.src.services.tm_tiling")

TOKEN = "test-token"
AUTH = {"Authorization": "Bearer " + TOKEN}


@unittest.skipUnless(HAS_FASTAPI, "fastapi/httpx not installed")
class TestApi(unittest.TestCase):
    def setUp(self):
        self.api = api
        api._runs.clear()
        self.client = TestClient(api.app)
        self.tmp = tempfile.TemporaryDirectory()
        self.table = os.path.join(self.tmp.name, "table.txt")
        self.detector = None   # (Detector.id, Name) once the instrument has a grid
        for patch in (
            mock.patch.dict(os.environ, {"TEGLON_API_TOKEN": TOKEN}),
            mock.patch.object(tm_tiling, "latest_map", lambda g: (7, "bayestar.fits") if g == "S1" else (None, None)),
            mock.patch.object(tm_tiling, "detector_for_instrument", lambda i: self.detector),
            mock.patch.object(tm_tiling, "tile_file_path", lambda *a: self.table),
            mock.patch.object(api.tiles_json, "tile_file_to_json", lambda p: {"pointings": [{"ra": 1.0}]}),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(self.tmp.cleanup)

    def drain(self):
        self.api._executor.submit(lambda: None).result(timeout=5)

    def hold_worker(self):
        """Block the run thread until the returned event is set, so queued runs stay PENDING."""
        release = threading.Event()
        self.api._executor.submit(release.wait, 5)
        self.addCleanup(release.set)
        return release

    def get(self, url, **kw):
        return self.client.get(url, headers=AUTH, **kw)

    def test_health_needs_no_token(self):
        with mock.patch.object(tm_tiling, "query_db", lambda q, raise_on_error=False: [[(1,)]]):
            self.assertEqual(self.client.get("/health").json(), {"ok": True})

    def test_health_fails_without_db(self):
        with mock.patch.object(tm_tiling, "query_db", mock.Mock(side_effect=RuntimeError("Unknown server host"))):
            r = self.client.get("/health")
        self.assertEqual(r.status_code, 503)
        self.assertIn("Unknown server host", r.json()["message"])

    def test_token_required(self):
        r = self.client.get("/suggested_pointings?graceid=S1&instrument_id=5")
        self.assertEqual(r.status_code, 401)
        self.assertIn("message", r.json())

    def test_bad_params_and_unknown_event(self):
        self.assertEqual(self.get("/suggested_pointings?graceid=S1;drop&instrument_id=5").status_code, 400)
        self.assertEqual(self.get("/suggested_pointings?graceid=S1&instrument_id=x").status_code, 400)
        r = self.get("/suggested_pointings?graceid=S2&instrument_id=5")
        self.assertEqual((r.status_code, r.json()["message"]), (404, "Teglon hasn't processed this event yet"))

    def test_unknown_route_is_json(self):
        self.assertIn("message", self.get("/nope").json())

    def test_setup_run_then_table(self):
        def write(graceid, instrument_id, exclude_planned=False):
            self.detector = (1, "SWOPE")
            open(self.table, "w").close()
        with mock.patch.object(tm_tiling, "write_suggested_pointings", write):
            release = self.hold_worker()
            first = self.get("/suggested_pointings?graceid=S1&instrument_id=5")
            again = self.get("/suggested_pointings?graceid=S1&instrument_id=5")
            release.set()
            self.drain()
        self.assertEqual(first.status_code, 404)
        self.assertEqual((first.json()["kind"], first.json()["state"]), ("setup", "PENDING"))
        self.assertEqual(again.json()["task_id"], first.json()["task_id"])   # not queued twice
        status = self.get("/planned_runs/" + first.json()["task_id"]).json()
        self.assertEqual(status["state"], "SUCCESS")
        r = self.get("/suggested_pointings?graceid=S1&instrument_id=5")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["graceid"], "S1")
        self.assertEqual(r.json()["pointings"], [{"ra": 1.0}])

    def test_failed_setup_is_held(self):
        calls = []

        def fail(*a, **k):
            calls.append(a)
            raise RuntimeError("Teglon only tiles photometric (UV/optical/IR) instruments.")
        with mock.patch.object(tm_tiling, "write_suggested_pointings", fail):
            self.get("/suggested_pointings?graceid=S1&instrument_id=9")
            self.drain()
            r = self.get("/suggested_pointings?graceid=S1&instrument_id=9")
        self.assertEqual(r.json()["state"], "FAILURE")
        self.assertIn("photometric", r.json()["message"])
        self.assertEqual(len(calls), 1)

    def test_planned_run_reused_when_current(self):
        self.detector = (1, "SWOPE")
        with mock.patch.object(tm_tiling, "table_is_current", lambda p, g: True):
            r = self.client.post("/planned_runs?graceid=S1&instrument_id=5", headers=AUTH)
        self.assertEqual(r.status_code, 200)
        self.assertEqual((r.json()["reused"], r.json()["state"]), (True, "SUCCESS"))

    def test_planned_run_queued_once(self):
        self.detector = (1, "SWOPE")
        seen = []
        with mock.patch.object(tm_tiling, "table_is_current", lambda p, g: False), \
                mock.patch.object(tm_tiling, "write_suggested_pointings",
                                  lambda g, i, exclude_planned=False: seen.append(exclude_planned)):
            release = self.hold_worker()
            first = self.client.post("/planned_runs?graceid=S1&instrument_id=5", headers=AUTH)
            second = self.client.post("/planned_runs?graceid=S1&instrument_id=5", headers=AUTH)
            release.set()
            self.drain()
        self.assertEqual((first.status_code, first.json()["state"]), (202, "PENDING"))
        self.assertEqual((second.status_code, second.json()["task_id"]), (200, first.json()["task_id"]))
        self.assertEqual(seen, [True])

    def test_contours(self):
        body = {"detection_overlays": []}
        with mock.patch.object(tm_tiling, "event_contours", lambda g, include_planned: body):
            self.assertEqual(self.get("/contours?graceid=S1&include_planned=true").json(), body)
        with mock.patch.object(tm_tiling, "event_contours", mock.Mock(side_effect=LookupError("not loaded"))):
            self.assertEqual(self.get("/contours?graceid=S1").status_code, 404)


@unittest.skipUnless(HAS_SKYMAP, "ligo.skymap/healpy not installed")
class TestCoverageContours(unittest.TestCase):
    def test_covered_sky_moves_contours(self):
        import healpy as hp
        import numpy as np

        nside = 64
        ra, dec = hp.pix2ang(nside, np.arange(hp.nside2npix(nside)), lonlat=True)
        prob = np.exp(-0.5 * ((ra - 180) ** 2 + dec ** 2) / 5 ** 2)
        prob /= prob.sum()

        def max_ra(levels):
            return {level: max(pt[0] for poly in polys for pt in poly) for level, polys in levels}

        uncovered = tm_tiling.coverage_contours(prob, [])
        covered = tm_tiling.coverage_contours(prob, np.where(ra > 180)[0])
        self.assertEqual([level for level, _ in covered], [50, 90])
        self.assertGreater(max_ra(uncovered)[90], 185)
        self.assertLess(max_ra(covered)[90], 182)   # nothing left east of the covered edge (one pixel's slack)
        json.dumps(covered)   # plain [ra, dec] floats, ready for the response

    def test_all_covered(self):
        self.assertEqual(tm_tiling.coverage_contours([0.5, 0.5], [0, 1]), [])


if __name__ == "__main__":
    unittest.main()
