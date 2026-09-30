# Snapshot lossless size v1

PROJECT=MOEX_Bot

Owner request: reduce both the persisted snapshot and JSON delivered to chat,
without cutting history or information. Base: `7dd7f6a77a81660b756da692e1fb5d82e681f53f`.

## Representation contract

`rub_snapshot_storage.v1` contains the complete canonical UTF-8 JSON of the
primary `rub_chat_analysis_snapshot.v1`, compressed with zlib and base64 encoded.
It records logical schema, exact expanded byte count and SHA-256. Decode is
bounded and rejects corrupt, truncated, trailing or mismatched data. This is a
representation integrity check, never source admission. All original evidence,
source bytes, identities, clocks, refusals and historical observations survive.

`rub_snapshot_references.v1` is the readable delivery representation. Its `data`
contains the complete logical snapshot or factual package. A sole
`$snapshot_ref` object denotes an exact duplicate at an RFC 6901 pointer within
`data`; the target remains in the same document. Resolve it before interpreting
the logical schema. A sole `$snapshot_literal` preserves an original object
whose key would otherwise collide with a representation marker. Reference
cycles, missing targets, excessive expansion, changed length and digest fail
closed. No external fetch, sidecar or history truncation is involved. Chat can
read distinct values directly and follow explicit in-document references.

Both representations are deterministic and conditional: only documents at least
256 KiB, at most 128 MiB, and with at least 10% actual savings are encoded.
Other documents retain their full legacy representation; these are encoding
bounds, never history limits. Canonical readers return the same expanded logical
schema as before, then apply unchanged source replay/admission and consumption
freshness checks. Fast-market/user-position writes and frozen audit exports keep
their existing bytes and formats. Current export and HTTP JSON serialization use
the same readable reference format; routes, authentication, MCP and services are
unchanged. The existing MCP bridge forwards the self-describing JSON unchanged.

## Scope

- `src/moex_data/rub_snapshot_serialization.py`: representation only.
- `src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot.py`: primary
  snapshot atomic persistence and canonical expansion before validation.
- `src/moex_research/external_data/rosstat_cpi_factual.py` and
  `rosstat_polling_retention.py`: decode current snapshot before discovering pins;
  corruption retains evidence/fails closed.
- `src/misc/rub_factual_snapshot_http_server.py`: JSON serializer only.
- `src/moex_data/rub_factual_release.py`: current export serializer and offline
  snapshot input reader. Frozen export hashes/bytes are unchanged.
- `src/moex_data/rub_factual_release_acceptance.py`: decode local/API input before
  the existing completeness checks.
- `src/moex_data/rub_production_source_matrix.py`: decode its existing offline
  snapshot input, retaining the checksum of the supplied carrier bytes.
- `tests/unit/test_rub_snapshot_serialization.py` and
  `tests/test_snapshot_lossless_size_v1.py`: codec and real-refresh tests.
- Existing `tests/test_stage9_analysis_bundle_v2.py`,
  `tests/test_futoi_live_date_identity_repair_v1.py` and
  `tests/unit/test_usd_cets_reference.py`: decode the new carrier before their
  unchanged semantic assertions.
- `tests/unit/test_exact_comparison_sources.py`: the same carrier migration for
  saved current JSON, retaining full 1/5/20 Price/OI, 1/5 basis/carry, immutable
  replay, repeated-observation identity and publication-expiry assertions.
- `tests/unit/test_rub_factual_snapshot_http_server.py`: optional test-client
  timeout for real-builder serialization tests, default unchanged. Production
  deadlines/TTLs are untouched.
- Direct consumer documentation: `RUB_SNAPSHOT_EXPORT.md`,
  `docs/MOEX_BOT_RUB_SNAPSHOT_MANUAL_EXPORT.md`,
  `docs/MOEX_BOT_RUB_FACTUAL_SNAPSHOT_NETWORK_API_v1.md`,
  `docs/MOEX_BOT_RUB_FACTUAL_SNAPSHOT_CHATGPT_MCP_BRIDGE_v1.md`,
  `docs/USDRUBF_RUB_INTELLIGENCE_S7_3_CHAT_ANALYSIS_SNAPSHOT_V1.md`,
  `docs/USDRUBF_RUB_INTELLIGENCE_S7_3_ANALYSIS_CHAT_CONSUMER_CONTRACT_V1.md`,
  `docs/USDRUBF_RUB_INTELLIGENCE_S7_3_DAILY_ANALYSIS_CHAT_CONTRACT_V1.md`, and
  `docs/USDRUBF_RUB_INTELLIGENCE_S7_3_WEEKLY_ANALYSIS_CHAT_CONTRACT_V1.md`.
  These describe logical-root expansion and the existing canonical current-export
  command, not new transport or analysis authority.
- This task document.

No source acquisition/backfill, TTL change, admission/governance expansion,
accepted-pointer migration, trading authority or transport redesign is included.

## Validation status

Initial production copy: 20,260,771 canonical bytes -> 7,774,143 storage bytes;
existing factual package: 2,265,729 -> 895,483 readable delivery bytes. Full
canonical byte equality after expansion passed for both frozen inputs.
Pre-publication new tests: `PYTHONPATH=.:src pytest -q
tests/unit/test_rub_snapshot_serialization.py tests/test_snapshot_lossless_size_v1.py`
passed **25 tests in 16.61 s**, using the server venv in an isolated GitHub clone
with the real Parquet engine. Tests cover byte equality, all history rows, large
integer identity, literal/reference collisions, independent restored copies,
corruption/expansion refusal, Rosstat retention fail-closed behavior, real heavy
refresh, canonical legacy/encoded parity, actual HTTP JSON/current export parity,
source matrix CLI, unchanged accepted pointers and current TTL expiry with dated
weekly context retained.

P1 review correction: migrated the quick-reference command and daily/weekly/common
raw fallback contracts, plus API/MCP representation documentation. The tested
`python -m moex_data.rub_snapshot_serialization --expand FILE` command restores
the complete logical root, validates the carrier before printing and does not
refresh or mutate input. The 2-second unit HTTP client deadline was unsuitable
for the real-builder integration under suite load; that one test now requests
30 seconds explicitly, without changing production timeouts or TTL. Correction
validation across codec, real builder and HTTP tests passed **58 tests in 17.71 s**.

The first full CI run (36687247806 at 229567fd14ddd998588913a38a3d33f55aeef58d)
compiled successfully and reported 7,694 passed, 37 subtests passed and one failure
in the exact-comparison regression's direct raw-JSON assertion. The two saved
snapshot reads in that test now expand the carrier; frozen audit input reads
remain unchanged. This was a test migration failure, not an infrastructure fault.
The complete exact-comparison test file then passed **36 tests in 95.82 s**,
including saved JSON, canonical/Stage9/compact views, full frozen export replay,
repeat capture and publication TTL expiry. Final exact-head CI remains required.

Final-head review, complete compileall/pytest CI, merge and applied-state evidence
are recorded on the task PR after verification. They are mandatory apply gates.
Deployment re-encodes the existing generation under the existing refresh locks;
it must preserve its complete expanded SHA and source/acceptance times. Rollback
to a pre-codec revision also restores the backed-up original snapshot carrier
under those locks. No backfill or artificial fresh generation is needed.
