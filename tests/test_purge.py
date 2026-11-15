"""Prefix-purge tests: boundary retention, watermark rules, restart safety.

Service-level tests use only the stdlib; the HTTP class skips itself when
the web stack is unavailable, mirroring tests/test_api.py.
"""

import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path

from app.db import connect, init_db
from app.service import (
    PurgeRegressionError,
    PurgedSampleIdError,
    ingest_batch,
    purge_prefix,
    purge_status,
    query_traffic,
)
from app.timeline import Record, analyze_timeline


def r(sid: int, local: str, count: int) -> Record:
    return Record(sid, local, count)


INITIAL = [
    r(100, "2024-11-03T00:45:00", 120),
    r(128, "2024-11-03T01:20:00", 340),
    r(148, "2024-11-03T01:55:00", 300),
    r(160, "2024-11-03T02:15:00", 260),
    r(180, "2024-11-03T03:10:00", 210),
]
RESOLVER = r(127, "2024-11-03T01:20:00", 84)


def reference_verdicts(records, keep_above):
    """Un-purged reference: full analysis restricted to ids > keep_above."""

    result = analyze_timeline(records)
    assert result.feasible
    out = {}
    for rec in records:
        if rec.sample_id <= keep_above:
            continue
        out[rec.sample_id] = (
            result.earliest[rec.sample_id].isoformat(),
            result.latest[rec.sample_id].isoformat(),
        )
    return out


def view_verdicts(view):
    """Verdicts of a query_traffic payload as {id: (earliest, latest)}."""

    out = {}
    for d in view["details"]:
        out[d["sample_id"]] = (d["utc_time"], d["utc_time"])
    for p in view["pending"]:
        out[p["sample_id"]] = (
            p["possible_utc_earliest"],
            p["possible_utc_latest"],
        )
    return out


class PurgeServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.db"
        self.conn = connect(self.db_path)
        self.audit_conn = connect(self.db_path)
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self.audit_conn.close()
        self.tmp.cleanup()

    def _ingest(self, records):
        return ingest_batch(self.conn, records, audit_conn=self.audit_conn)

    def _count(self):
        return self.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def test_purge_deletes_prefix_and_reports_count(self):
        self._ingest(INITIAL)
        result = purge_prefix(self.conn, 148)
        self.assertEqual(result.deleted_count, 3)
        self.assertEqual(result.watermark, 148)
        self.assertEqual(result.boundary_sample_id, 148)
        # 148 was pending; its earliest candidate is the EDT pass
        self.assertEqual(result.boundary_utc, "2024-11-03T05:55:00+00:00")
        self.assertEqual(self._count(), 2)

        status = purge_status(self.conn)
        self.assertEqual(status["watermark"], 148)
        self.assertEqual(status["boundary_sample_id"], 148)
        self.assertEqual(status["boundary_utc"], "2024-11-03T05:55:00+00:00")
        self.assertEqual(len(status["history"]), 1)
        self.assertEqual(status["history"][0]["deleted_count"], 3)

        view = query_traffic(self.conn)
        self.assertEqual(view["purge_watermark"], 148)
        self.assertEqual(view["total_records"], 2)
        self.assertEqual(view["pending_sample_ids"], [])
        by_id = {d["sample_id"]: d["utc_time"] for d in view["details"]}
        self.assertEqual(by_id[160], "2024-11-03T07:15:00+00:00")
        self.assertEqual(by_id[180], "2024-11-03T08:10:00+00:00")

    def test_boundary_inside_repeated_hour_keeps_pending_verdicts(self):
        # Truncating at 128 (an ambiguous wall time) must not resolve 148.
        self._ingest(INITIAL)
        result = purge_prefix(self.conn, 128)
        self.assertEqual(result.boundary_utc, "2024-11-03T05:20:00+00:00")

        view = query_traffic(self.conn)
        self.assertEqual(view["pending_sample_ids"], [148])
        pending = view["pending"][0]
        self.assertEqual(
            pending["possible_utc_earliest"], "2024-11-03T05:55:00+00:00"
        )
        self.assertEqual(
            pending["possible_utc_latest"], "2024-11-03T06:55:00+00:00"
        )
        self.assertEqual(
            view_verdicts(view), reference_verdicts(INITIAL, 128)
        )

    def test_purge_after_resolution_boundary_uses_pinned_utc(self):
        self._ingest(INITIAL + [RESOLVER])
        result = purge_prefix(self.conn, 148)
        # after disambiguation 148 is pinned to the EST pass
        self.assertEqual(result.boundary_utc, "2024-11-03T06:55:00+00:00")

        # 150@01:58 is ambiguous on its own, but the retained boundary only
        # leaves the late pass -> determined, exactly as without the purge.
        self._ingest([r(150, "2024-11-03T01:58:00", 95)])
        view = query_traffic(self.conn)
        self.assertEqual(view["pending_sample_ids"], [])
        by_id = {d["sample_id"]: d["utc_time"] for d in view["details"]}
        self.assertEqual(by_id[150], "2024-11-03T06:58:00+00:00")

    def test_purged_ids_reject_whole_batch(self):
        self._ingest(INITIAL)
        purge_prefix(self.conn, 148)

        with self.assertRaises(PurgedSampleIdError):
            self._ingest([r(128, "2024-11-03T01:20:00", 340)])
        # a batch is rejected whole even if only one id is purged
        with self.assertRaises(PurgedSampleIdError):
            self._ingest([
                r(100, "2024-11-03T00:45:00", 120),
                r(200, "2024-11-03T04:00:00", 10),
            ])
        self.assertEqual(self._count(), 2)  # 200 never landed
        # the rejections are audited like any other rejected batch
        statuses = [
            row[0]
            for row in self.conn.execute(
                "SELECT status FROM batch_audit ORDER BY id"
            ).fetchall()
        ]
        self.assertEqual(statuses, ["accepted", "rejected", "rejected"])

    def test_watermark_only_moves_forward(self):
        self._ingest(INITIAL)
        purge_prefix(self.conn, 148)

        with self.assertRaises(PurgeRegressionError):
            purge_prefix(self.conn, 148)  # equal is not forward
        with self.assertRaises(PurgeRegressionError):
            purge_prefix(self.conn, 100)

        # failed purges left no trace: rows, watermark, boundary, history
        self.assertEqual(self._count(), 2)
        status = purge_status(self.conn)
        self.assertEqual(status["watermark"], 148)
        self.assertEqual(status["boundary_sample_id"], 148)
        self.assertEqual(len(status["history"]), 1)

        # moving forward still works and re-anchors the boundary
        result = purge_prefix(self.conn, 160)
        self.assertEqual(result.deleted_count, 1)
        self.assertEqual(result.boundary_sample_id, 160)
        self.assertEqual(result.boundary_utc, "2024-11-03T07:15:00+00:00")
        view = query_traffic(self.conn)
        self.assertEqual(
            view["details"],
            [{
                "sample_id": 180,
                "local_time": "2024-11-03T03:10:00",
                "pass_count": 210,
                "utc_time": "2024-11-03T08:10:00+00:00",
            }],
        )

    def test_purge_beyond_all_records_keeps_boundary_with_empty_warehouse(self):
        self._ingest(INITIAL)
        result = purge_prefix(self.conn, 180)  # truncates every stored reading
        self.assertEqual(result.deleted_count, 5)
        self.assertEqual(result.boundary_sample_id, 180)
        self.assertEqual(result.boundary_utc, "2024-11-03T08:10:00+00:00")

        view = query_traffic(self.conn)
        self.assertEqual(view["total_records"], 0)
        self.assertEqual(view["details"], [])
        self.assertEqual(view["pending_sample_ids"], [])

        # the warehouse is empty but the boundary still constrains backfills
        with self.assertRaises(Exception) as ctx:
            self._ingest([r(190, "2024-11-03T03:05:00", 10)])  # 08:05Z < 08:10Z
        self.assertEqual(getattr(ctx.exception, "status_code"), 422)
        self._ingest([r(190, "2024-11-03T03:20:00", 10)])
        view = query_traffic(self.conn)
        self.assertEqual(
            view["details"][0]["utc_time"], "2024-11-03T08:20:00+00:00"
        )
        # and purged ids stay rejected
        with self.assertRaises(PurgedSampleIdError):
            self._ingest([r(180, "2024-11-03T03:10:00", 210)])

    def test_purge_on_empty_warehouse_has_no_boundary(self):
        result = purge_prefix(self.conn, 50)
        self.assertEqual(result.deleted_count, 0)
        self.assertIsNone(result.boundary_sample_id)
        self.assertIsNone(result.boundary_utc)

        # no boundary -> nothing constrains new readings beyond their ids
        self._ingest([r(60, "2024-11-03T00:01:00", 5)])
        view = query_traffic(self.conn)
        self.assertEqual(view["determined_count"], 1)
        with self.assertRaises(PurgedSampleIdError):
            self._ingest([r(50, "2024-11-03T00:02:00", 5)])

    def test_verdicts_match_unpurged_reference_for_many_cut_points(self):
        scenarios = {
            "unresolved": INITIAL,
            "resolved": INITIAL + [RESOLVER],
        }
        for name, records in scenarios.items():
            for cut in (90, 100, 127, 128, 148, 149, 160, 180, 500):
                with self.subTest(scenario=name, cut=cut):
                    other = connect(self.db_path.parent / f"{name}-{cut}.db")
                    init_db(other)
                    try:
                        ingest_batch(other, records)
                        purge_prefix(other, cut)
                        view = query_traffic(other)
                        self.assertNotIn("error", view)
                        self.assertEqual(
                            view_verdicts(view),
                            reference_verdicts(records, cut),
                        )
                    finally:
                        other.close()

    def test_backfill_parity_with_unpurged_reference(self):
        backfills = [
            (r(150, "2024-11-03T01:58:00", 95), True),
            (r(149, "2024-11-03T01:30:00", 50), False),  # before the boundary
            (r(200, "2024-11-03T04:00:00", 10), True),
        ]
        for cut in (128, 148):
            with self.subTest(cut=cut):
                reference = connect(self.db_path.parent / f"ref-{cut}.db")
                purged = connect(self.db_path.parent / f"parity-{cut}.db")
                init_db(reference)
                init_db(purged)
                try:
                    records = INITIAL + [RESOLVER]
                    ingest_batch(reference, records)
                    ingest_batch(purged, records)
                    purge_prefix(purged, cut)

                    for rec, expect_ok in backfills:
                        ref_ok = purged_ok = True
                        try:
                            ingest_batch(reference, [rec])
                        except Exception:
                            ref_ok = False
                        try:
                            ingest_batch(purged, [rec])
                        except Exception:
                            purged_ok = False
                        self.assertEqual(
                            purged_ok, expect_ok, f"backfill {rec.sample_id}"
                        )
                        self.assertEqual(ref_ok, purged_ok)

                    # surviving ids keep identical verdicts in both worlds
                    ref_view = query_traffic(reference)
                    my_view = query_traffic(purged)
                    ref_survivors = {
                        sid: v
                        for sid, v in view_verdicts(ref_view).items()
                        if sid > cut
                    }
                    self.assertEqual(view_verdicts(my_view), ref_survivors)
                finally:
                    reference.close()
                    purged.close()

    def test_restart_keeps_watermark_and_boundary(self):
        self._ingest(INITIAL + [RESOLVER])
        purge_prefix(self.conn, 148)
        self.conn.close()
        self.audit_conn.close()

        # fresh connections against the same file: nothing is cached in memory
        self.conn = connect(self.db_path)
        self.audit_conn = connect(self.db_path)

        with self.assertRaises(PurgedSampleIdError):
            self._ingest([r(128, "2024-11-03T01:20:00", 340)])
        with self.assertRaises(PurgeRegressionError):
            purge_prefix(self.conn, 148)
        with self.assertRaises(Exception) as ctx:
            self._ingest([r(149, "2024-11-03T01:30:00", 50)])
        self.assertEqual(getattr(ctx.exception, "status_code"), 422)

        self._ingest([r(150, "2024-11-03T01:58:00", 95)])
        view = query_traffic(self.conn)
        by_id = {d["sample_id"]: d["utc_time"] for d in view["details"]}
        self.assertEqual(by_id[150], "2024-11-03T06:58:00+00:00")
        self.assertEqual(view["purge_watermark"], 148)

    def test_purge_races_backfill_without_mixing(self):
        self._ingest(INITIAL + [RESOLVER])
        barrier = threading.Barrier(3)
        errors: list[BaseException] = []
        lock = threading.Lock()

        def purger():
            conn = connect(self.db_path)
            barrier.wait()
            try:
                purge_prefix(conn, 148)
            except BaseException as exc:  # noqa: BLE001 - collected below
                with lock:
                    errors.append(exc)
            finally:
                conn.close()

        def uploader(rec):
            conn = connect(self.db_path)
            audit = connect(self.db_path)
            barrier.wait()
            try:
                ingest_batch(conn, [rec], audit_conn=audit)
            except Exception:
                pass  # rejection is a legitimate outcome of the race
            finally:
                conn.close()
                audit.close()

        threads = [
            threading.Thread(target=purger, daemon=True),
            threading.Thread(
                target=uploader, args=(r(150, "2024-11-03T01:58:00", 95),),
                daemon=True,
            ),
            threading.Thread(
                target=uploader, args=(r(200, "2024-11-03T04:00:00", 10),),
                daemon=True,
            ),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        self.assertEqual(errors, [])
        status = purge_status(self.conn)
        self.assertEqual(status["watermark"], 148)
        self.assertEqual(len(status["history"]), 1)
        stored_ids = {
            row[0]
            for row in self.conn.execute("SELECT sample_id FROM records")
        }
        self.assertTrue(all(sid > 148 for sid in stored_ids))
        self.assertTrue({160, 180} <= stored_ids)

        # whatever the interleaving, the visible verdicts equal one of the
        # two serial orders evaluated with the retained boundary
        view = query_traffic(self.conn)
        self.assertNotIn("error", view)
        self.assertEqual(
            view["total_records"],
            view["determined_count"] + view["pending_count"],
        )
        boundary = datetime.fromisoformat(status["boundary_utc"])
        self.assertEqual(boundary.isoformat(), "2024-11-03T06:55:00+00:00")
        remaining = [
            r(sid, local, cnt)
            for sid, local, cnt in [
                (150, "2024-11-03T01:58:00", 95),
                (160, "2024-11-03T02:15:00", 260),
                (180, "2024-11-03T03:10:00", 210),
                (200, "2024-11-03T04:00:00", 10),
            ]
            if sid in stored_ids
        ]
        expected = analyze_timeline(remaining, earliest_before=boundary)
        self.assertTrue(expected.feasible)
        self.assertEqual(
            view_verdicts(view),
            {
                rec.sample_id: (
                    expected.earliest[rec.sample_id].isoformat(),
                    expected.latest[rec.sample_id].isoformat(),
                )
                for rec in remaining
            },
        )


class PurgeApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            from fastapi.testclient import TestClient
        except Exception as exc:  # pragma: no cover - environment dependent
            raise unittest.SkipTest(f"web stack unavailable: {exc}")

        import app.db as db_module

        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_file = Path(cls.tmp.name) / "api.db"
        cls._orig = db_module.DB_PATH
        db_module.DB_PATH = cls.db_file

        from app.main import app

        cls.client = TestClient(app)
        cls.client.__enter__()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "client"):
            import app.db as db_module

            cls.client.__exit__(None, None, None)
            db_module.DB_PATH = cls._orig
            cls.tmp.cleanup()

    def _upload(self, name: str, content: bytes):
        return self.client.post(
            "/upload", files={"file": (name, content, "text/csv")}
        )

    def test_purge_flow_over_http(self):
        initial = (
            "sample_id,local_time,pass_count\n"
            "100,2024-11-03T00:45:00,120\n"
            "128,2024-11-03T01:20:00,340\n"
            "148,2024-11-03T01:55:00,300\n"
            "160,2024-11-03T02:15:00,260\n"
            "180,2024-11-03T03:10:00,210\n"
        ).encode()
        r = self._upload("initial.csv", initial)
        self.assertEqual(r.status_code, 200, r.text)
        r = self._upload(
            "backfill.csv",
            b"sample_id,local_time,pass_count\n127,2024-11-03T01:20:00,84\n",
        )
        self.assertEqual(r.status_code, 200, r.text)

        # purge the settled prefix; the truncation point sits in the
        # repeated hour and 148 is pinned to 06:55Z by the resolver row
        r = self.client.post("/purge", json={"truncate_upto_sample_id": 148})
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertTrue(body["purged"])
        self.assertEqual(body["deleted_count"], 4)
        self.assertEqual(body["watermark"], 148)
        self.assertEqual(body["boundary_sample_id"], 148)
        self.assertEqual(body["boundary_utc"], "2024-11-03T06:55:00+00:00")

        r = self.client.get("/purge").json()
        self.assertEqual(r["watermark"], 148)
        self.assertEqual(r["boundary_utc"], "2024-11-03T06:55:00+00:00")
        self.assertEqual(len(r["history"]), 1)

        # survivors keep their un-purged verdicts
        r = self.client.get("/traffic").json()
        self.assertEqual(r["purge_watermark"], 148)
        self.assertEqual(r["total_records"], 2)
        self.assertEqual(r["pending_sample_ids"], [])
        by_id = {d["sample_id"]: d["utc_time"] for d in r["details"]}
        self.assertEqual(by_id[160], "2024-11-03T07:15:00+00:00")
        self.assertEqual(by_id[180], "2024-11-03T08:10:00+00:00")

        # a batch touching purged ids is rejected whole (409)
        r = self._upload("initial.csv", initial)
        self.assertEqual(r.status_code, 409)
        self.assertFalse(r.json()["accepted"])

        # the truncation number cannot move backwards (409)
        r = self.client.post("/purge", json={"truncate_upto_sample_id": 100})
        self.assertEqual(r.status_code, 409)
        self.assertFalse(r.json()["purged"])
        r = self.client.post("/purge", json={"truncate_upto_sample_id": 148})
        self.assertEqual(r.status_code, 409)

        # post-purge backfill: forced onto the late pass by the boundary
        r = self._upload(
            "after.csv",
            b"sample_id,local_time,pass_count\n150,2024-11-03T01:58:00,95\n",
        )
        self.assertEqual(r.status_code, 200, r.text)
        r = self.client.get("/traffic").json()
        by_id = {d["sample_id"]: d["utc_time"] for d in r["details"]}
        self.assertEqual(by_id[150], "2024-11-03T06:58:00+00:00")

        # a backfill contradicting the retained boundary is still illegal
        r = self._upload(
            "illegal.csv",
            b"sample_id,local_time,pass_count\n149,2024-11-03T01:30:00,50\n",
        )
        self.assertEqual(r.status_code, 422)

        # batch audit kept working through all of the above
        audits = self.client.get("/batches").json()["batches"]
        self.assertEqual(
            [a["status"] for a in audits],
            ["accepted", "accepted", "rejected", "accepted", "rejected"],
        )

    def test_purge_validates_body(self):
        r = self.client.post("/purge", json={})
        self.assertEqual(r.status_code, 422)
        r = self.client.post("/purge", json={"truncate_upto_sample_id": -3})
        self.assertEqual(r.status_code, 422)


if __name__ == "__main__":
    unittest.main()
