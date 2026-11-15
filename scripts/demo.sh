#!/usr/bin/env bash
# End-to-end demo against a running container (or local uvicorn on :8000).
# Shows the query before and after the disambiguating backfill, plus the two
# whole-batch rejection paths, then the settled-prefix purge: the retained
# timeline boundary keeps post-purge backfills exactly as constrained as
# before the purge. Run against a FRESH container (docker compose down -v &&
# docker compose up --build) -- the purge watermark only moves forward, so a
# second run against the same volume stops at the purge step by design.
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8000}"
SAMPLES="$(cd "$(dirname "$0")/../samples" && pwd)"

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

say "health"
curl -fsS "$BASE/health"; echo

say "1) initial fallback-day batch (expect 5 rows, 2 pending)"
curl -fsS -F "file=@$SAMPLES/01_initial_fallback_day.csv" "$BASE/upload"; echo

say "GET /traffic BEFORE backfill: only UTC-unique records are served"
curl -fsS "$BASE/traffic" | python3 -m json.tool

say "2) re-uploading the identical file -> idempotent, 0 inserted"
curl -fsS -F "file=@$SAMPLES/01_initial_fallback_day.csv" "$BASE/upload"; echo

say "3) conflict batch: same sample id, different pass_count -> HTTP 409, whole batch rejected"
curl -sS -o /tmp/conflict.json -w "HTTP %{http_code}\n" \
  -F "file=@$SAMPLES/03_conflict_same_id_different_value.csv" "$BASE/upload"
cat /tmp/conflict.json; echo

say "4) illegal timeline batch: 3 readings share one ambiguous wall time -> HTTP 422, rolled back"
curl -sS -o /tmp/illegal.json -w "HTTP %{http_code}\n" \
  -F "file=@$SAMPLES/04_illegal_timeline.csv" "$BASE/upload"
cat /tmp/illegal.json; echo

say "5) disambiguating backfill: one more reading at the repeated 01:20 wall time"
curl -fsS -F "file=@$SAMPLES/02_backfill_resolves_ambiguity.csv" "$BASE/upload"; echo

say "GET /traffic AFTER backfill: pending list empty, every UTC pinned"
curl -fsS "$BASE/traffic" | python3 -m json.tool

say "6) purge settled prefix up to sample 148 (truncation point sits in the repeated hour)"
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d '{"truncate_upto_sample_id": 148}' "$BASE/purge"; echo

say "GET /purge: watermark moved to 148; boundary = purged 148's pinned UTC 06:55Z"
curl -fsS "$BASE/purge" | python3 -m json.tool

say "GET /traffic AFTER purge: 160/180 keep their determined UTC, purged rows gone"
curl -fsS "$BASE/traffic" | python3 -m json.tool

say "7) re-uploading a file with purged sample ids -> HTTP 409, whole batch rejected"
curl -sS -o /tmp/purged.json -w "HTTP %{http_code}\n" \
  -F "file=@$SAMPLES/01_initial_fallback_day.csv" "$BASE/upload"
cat /tmp/purged.json; echo

say "8) moving the truncation number backwards -> HTTP 409, nothing deleted"
curl -sS -o /tmp/regress.json -w "HTTP %{http_code}\n" -X POST \
  -H 'Content-Type: application/json' \
  -d '{"truncate_upto_sample_id": 100}' "$BASE/purge"
cat /tmp/regress.json; echo

say "9) post-purge backfill 150@01:58: the retained boundary (148 pinned at 06:55Z)"
say "   forces it onto the late EST pass -> determined 06:58Z, exactly as un-purged"
curl -fsS -F "file=@$SAMPLES/05_post_purge_backfill.csv" "$BASE/upload"; echo
curl -fsS "$BASE/traffic" | python3 -m json.tool

say "10) backfill contradicting the retained boundary (149@01:30, both UTC slots"
say "    <= the purged 148's 06:55Z) -> HTTP 422, rolled back"
curl -sS -o /tmp/postpurge.json -w "HTTP %{http_code}\n" \
  -F "file=@$SAMPLES/06_post_purge_illegal.csv" "$BASE/upload"
cat /tmp/postpurge.json; echo

say "batch audit trail (rejections persisted despite rollback)"
curl -fsS "$BASE/batches" | python3 -m json.tool
