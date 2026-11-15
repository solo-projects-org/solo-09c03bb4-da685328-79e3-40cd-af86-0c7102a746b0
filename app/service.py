"""Business logic: batch ingestion rules, prefix purges, ambiguity-aware queries.

No web-framework imports here -- the layer is plain Python so it can be unit
tested directly against SQLite.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime

from .db import read_transaction, write_transaction
from .timeline import Record, analyze_timeline


class IngestionError(Exception):
    """Base error; ``status_code`` mirrors the HTTP response."""

    status_code = 400

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class ConflictError(IngestionError):
    """Same sample id, different content."""

    status_code = 409


class UnresolvableTimelineError(IngestionError):
    """No legal monotonic UTC timeline exists."""

    status_code = 422


class PurgedSampleIdError(IngestionError):
    """Batch carries sample ids at or below the purge watermark."""

    status_code = 409


class PurgeError(IngestionError):
    """Base for errors raised by the purge operation itself."""


class PurgeRegressionError(PurgeError):
    """Truncation number does not move the purge watermark forward."""

    status_code = 409


class WarehouseInfeasibleError(PurgeError):
    """Stored records admit no legal timeline (should never happen)."""

    status_code = 500


@dataclass
class IngestionResult:
    batch_ref: str
    accepted: bool
    inserted: list[int]
    skipped_duplicates: list[int]
    record_count: int


@dataclass
class PurgeResult:
    watermark: int
    deleted_count: int
    boundary_sample_id: int | None
    boundary_utc: str | None  # ISO-8601, offset-aware


def _all_rows(conn: sqlite3.Connection) -> dict[int, sqlite3.Row]:
    rows = conn.execute(
        "SELECT sample_id, local_time, pass_count FROM records"
    ).fetchall()
    return {row["sample_id"]: row for row in rows}


def _purge_state(
    conn: sqlite3.Connection,
) -> tuple[int, int | None, datetime | None]:
    """(watermark, boundary sample id, boundary UTC) from the singleton row."""

    row = conn.execute(
        "SELECT watermark, boundary_sample_id, boundary_utc "
        "FROM purge_state WHERE id = 1"
    ).fetchone()
    boundary_utc = (
        datetime.fromisoformat(row["boundary_utc"])
        if row["boundary_utc"] is not None
        else None
    )
    return row["watermark"], row["boundary_sample_id"], boundary_utc


def _write_audit(
    audit_conn: sqlite3.Connection | None,
    batch_ref: str,
    status: str,
    reason: str | None,
    count: int,
) -> None:
    """Persist audit on the independent autocommit connection.

    Audit rows therefore survive the rollback of a rejected business batch.
    When no audit connection is supplied (unit tests) the entry is skipped.
    """

    if audit_conn is None:
        return
    audit_conn.execute(
        "INSERT INTO batch_audit (batch_ref, status, reason, record_count) "
        "VALUES (?, ?, ?, ?)",
        (batch_ref, status, reason, count),
    )


def ingest_batch(
    conn: sqlite3.Connection,
    records: list[Record],
    audit_conn: sqlite3.Connection | None = None,
) -> IngestionResult:
    """Atomically accept or reject one CSV batch.

    Rules:
      * any sample id at or below the purge watermark -> whole batch
        rejected (409): purged rows are gone, so even content comparison
        is impossible;
      * same sample id + same content -> idempotent skip (accepted once);
      * same sample id + different content -> whole batch rejected (409);
      * merged set admits no legal timeline -> whole batch rejected (422).
    """

    if not records:
        raise IngestionError("batch contains no records")

    batch_ref = uuid.uuid4().hex

    class _Rollback(Exception):
        def __init__(self, error: IngestionError):
            super().__init__(error.reason)
            self.error = error

    rejection: IngestionError | None = None
    accepted_result: IngestionResult | None = None

    try:
        with write_transaction(conn) as tx:
            # Full-table snapshot: feasibility must hold across *all*
            # warehoused records, not only ids touched by this batch.
            existing = _all_rows(tx)
            watermark, _, boundary_utc = _purge_state(tx)

            # 0) purged-prefix check: ids at or below the watermark can never
            #    come back, whatever their content
            purged_ids = sorted(
                {rec.sample_id for rec in records if rec.sample_id <= watermark}
            )
            if purged_ids:
                raise _Rollback(PurgedSampleIdError(
                    f"sample id(s) {purged_ids} are at or below the purge "
                    f"watermark {watermark}; purged readings are settled and "
                    f"cannot be re-ingested"
                ))

            # 1) conflict check against already-warehoused records
            for rec in records:
                row = existing.get(rec.sample_id)
                if row is not None and (
                    row["local_time"] != rec.local_time
                    or row["pass_count"] != rec.pass_count
                ):
                    raise _Rollback(ConflictError(
                        f"sample_id {rec.sample_id} already stored with different "
                        f"content (local_time={row['local_time']}, "
                        f"pass_count={row['pass_count']})"
                    ))

            # 2) merge the whole warehouse with this batch, then validate
            merged = [
                Record(
                    sample_id=row["sample_id"],
                    local_time=row["local_time"],
                    pass_count=row["pass_count"],
                )
                for row in existing.values()
            ]
            known_keys = {r.key() for r in merged}
            new_records: list[Record] = []
            skipped: list[int] = []
            for rec in records:
                if rec.key() in known_keys:
                    skipped.append(rec.sample_id)
                    continue
                merged.append(rec)
                new_records.append(rec)
                known_keys.add(rec.key())

            if not analyze_timeline(
                merged, earliest_before=boundary_utc
            ).feasible:
                raise _Rollback(UnresolvableTimelineError(
                    "no legal timeline: sample ids must map to strictly "
                    "increasing UTC instants across the DST transition"
                ))

            # 3) commit the new rows
            tx.executemany(
                "INSERT INTO records (sample_id, local_time, pass_count) "
                "VALUES (?, ?, ?)",
                [(r.sample_id, r.local_time, r.pass_count) for r in new_records],
            )
            accepted_result = IngestionResult(
                batch_ref=batch_ref,
                accepted=True,
                inserted=[r.sample_id for r in new_records],
                skipped_duplicates=sorted(set(skipped)),
                record_count=len(records),
            )
    except _Rollback as exc:
        rejection = exc.error

    # The write lock is released here; the audit connection can always write,
    # and rejection entries survive the business transaction rollback.
    if rejection is not None:
        _write_audit(
            audit_conn, batch_ref, "rejected", rejection.reason, len(records)
        )
        raise rejection

    _write_audit(audit_conn, batch_ref, "accepted", None, len(records))
    assert accepted_result is not None
    return accepted_result


def purge_prefix(conn: sqlite3.Connection, truncate_upto: int) -> PurgeResult:
    """Delete every raw reading with ``sample_id <= truncate_upto``.

    The watermark only moves forward.  The purge keeps a timeline boundary --
    the earliest candidate UTC of the last purged sample that really existed
    -- so the surviving records keep their un-purged determined/pending
    verdicts and later backfills face exactly the un-purged constraints, even
    when the truncation point sits inside the repeated wall-clock hour or no
    readings remain at all.

    Everything (delete, boundary update, audit row) commits in one
    BEGIN IMMEDIATE transaction: a failure leaves no partial purge, and a
    concurrent backfill either runs entirely before or entirely after.
    """

    with write_transaction(conn) as tx:
        watermark, boundary_id, boundary_utc = _purge_state(tx)
        if truncate_upto <= watermark:
            raise PurgeRegressionError(
                f"truncation number {truncate_upto} does not move the purge "
                f"watermark forward (current watermark {watermark})"
            )

        rows = tx.execute(
            "SELECT sample_id, local_time, pass_count FROM records "
            "ORDER BY sample_id"
        ).fetchall()
        records = [
            Record(
                sample_id=row["sample_id"],
                local_time=row["local_time"],
                pass_count=row["pass_count"],
            )
            for row in rows
        ]

        # Verdicts are computed against the *current* boundary, so the new
        # boundary value equals the one the un-purged warehouse would have.
        analysis = analyze_timeline(records, earliest_before=boundary_utc)
        if records and not analysis.feasible:  # defensive: ingestion guards this
            raise WarehouseInfeasibleError(
                "stored records admit no legal timeline; purge aborted"
            )

        victims = [r for r in records if r.sample_id <= truncate_upto]
        if victims:
            last = victims[-1]  # rows are ordered by sample_id
            boundary_id = last.sample_id
            boundary_utc = analysis.earliest[last.sample_id]

        tx.execute(
            "DELETE FROM records WHERE sample_id <= ?", (truncate_upto,)
        )
        boundary_utc_text = (
            boundary_utc.isoformat() if boundary_utc is not None else None
        )
        tx.execute(
            "UPDATE purge_state SET watermark = ?, boundary_sample_id = ?, "
            "boundary_utc = ? WHERE id = 1",
            (truncate_upto, boundary_id, boundary_utc_text),
        )
        tx.execute(
            "INSERT INTO purge_audit "
            "(watermark, deleted_count, boundary_sample_id, boundary_utc) "
            "VALUES (?, ?, ?, ?)",
            (truncate_upto, len(victims), boundary_id, boundary_utc_text),
        )

    return PurgeResult(
        watermark=truncate_upto,
        deleted_count=len(victims),
        boundary_sample_id=boundary_id,
        boundary_utc=boundary_utc_text,
    )


def purge_status(conn: sqlite3.Connection) -> dict:
    """Current watermark/boundary plus the committed purge history."""

    with read_transaction(conn) as tx:
        watermark, boundary_id, boundary_utc = _purge_state(tx)
        rows = tx.execute(
            "SELECT watermark, deleted_count, boundary_sample_id, "
            "boundary_utc, created_at FROM purge_audit ORDER BY id"
        ).fetchall()
    return {
        "watermark": watermark,
        "boundary_sample_id": boundary_id,
        "boundary_utc": boundary_utc.isoformat() if boundary_utc else None,
        "history": [dict(row) for row in rows],
    }


def query_traffic(conn: sqlite3.Connection) -> dict:
    """Return determined passenger details plus the still-pending sample ids.

    Everything is computed from one read snapshot in a single transaction, so
    a concurrent backfill can never produce half-new, half-stale verdicts.
    The purge state is read in the same snapshot, so the retained boundary is
    always the one that matches the rows being analyzed.
    """

    with read_transaction(conn) as tx:
        rows = tx.execute(
            "SELECT sample_id, local_time, pass_count FROM records "
            "ORDER BY sample_id"
        ).fetchall()
        watermark, _, boundary_utc = _purge_state(tx)

    records = [
        Record(
            sample_id=row["sample_id"],
            local_time=row["local_time"],
            pass_count=row["pass_count"],
        )
        for row in rows
    ]
    result = analyze_timeline(records, earliest_before=boundary_utc)
    if not result.feasible:  # defensive: ingestion never commits an infeasible set
        return {
            "total_records": len(records),
            "determined_count": 0,
            "pending_count": 0,
            "details": [],
            "pending_sample_ids": [],
            "pending": [],
            "purge_watermark": watermark,
            "error": "stored records admit no legal timeline; contact data steward",
        }

    details: list[dict] = []
    pending: list[dict] = []
    for rec in records:
        earliest = result.earliest[rec.sample_id]
        latest = result.latest[rec.sample_id]
        if earliest == latest:
            details.append(
                {
                    "sample_id": rec.sample_id,
                    "local_time": rec.local_time,
                    "pass_count": rec.pass_count,
                    "utc_time": earliest.isoformat(),
                }
            )
        else:
            pending.append(
                {
                    "sample_id": rec.sample_id,
                    "local_time": rec.local_time,
                    "pass_count": rec.pass_count,
                    "possible_utc_earliest": earliest.isoformat(),
                    "possible_utc_latest": latest.isoformat(),
                }
            )

    return {
        "total_records": len(records),
        "determined_count": len(details),
        "pending_count": len(pending),
        "details": details,
        "pending_sample_ids": [p["sample_id"] for p in pending],
        "pending": pending,
        "purge_watermark": watermark,
    }


def list_audit(conn: sqlite3.Connection) -> list[dict]:
    with read_transaction(conn) as tx:
        rows = tx.execute(
            "SELECT batch_ref, status, reason, record_count, created_at "
            "FROM batch_audit ORDER BY id"
        ).fetchall()
    return [dict(row) for row in rows]
