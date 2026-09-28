# Stage9 daily/weekly consistent bundle v2

PROJECT=MOEX_Bot

Task ID: stage9_daily_weekly_consistent_bundle_v2

Base: `402181ffef71ff6dd8f58ca6dc0bb9c87f569aef`.

Owner scope: compose the existing daily/weekly analysis bundle from already
admitted components, with explicit section selection/freshness at publication
and reading. No new sources, history backfill, TTL increases, historical pointer
changes, Stage5/model/trading permissions, scheduler or deployment infrastructure.

## Selection and storage

The executable selection contract is
`contracts/intelligence/rub_analysis_bundle_selection_v2.json`; the dataset and
configuration declarations are `contracts/datasets/rub_analysis_bundle.v2.yaml`
and `configs/datasets/step9_rub_analysis_bundle.v2.yaml`.

The existing snapshot refresh owns both bundles. Stage9 captures the accepted
Stage7 generation once per refresh; daily shares the same D1 bytes with weekly.
No second source collector or separate accepted pointer is introduced.

`components.stage9_daily.data.sections` and `components.stage9_weekly.data.sections`
contain three distinct sections:

* `current_market`: existing Price/OI and individual admitted basis/carry metrics,
  plus versioned Si/CR root FUTOI current pairs. Full publication/raw replay and
  existing consumer governance precede projection. Price/OI keeps 60 seconds;
  FUTOI keeps 1200 seconds for original source/receipt clocks. Producer flags do
  not grant consumer authority. A failed root/current pair does not remove the
  other root or independently admitted historical context.
* `completed_periods`: accepted D1 and (weekly only) W1 OHLCV/technical evidence.
  Dates, original availability/build clocks, actual row coverage and age remain
  visible. Calendar period elapsed is not proof of a complete trading session.
  Technical values require the exact accepted OHLCV run/period/identity.
* `historical_comparisons`: independently admitted dated FUTOI Si/CR comparisons
  and descriptive statistics, contract Price/OI pairs, historical basis/carry and
  bounded Stage7 observed-period comparisons. Exact lag refusals and actual
  sample counts remain visible. Current-anchored aliases are excluded from this
  dated selection; the existing canonical consumer still exposes their separate
  scopes. The earliest existing acceptance/anchor deadline governs the section;
  no TTL or first economic acceptance time is renewed.

Frozen Stage7 envelopes retain base64 original pointer, manifest, quality report
and Parquet buffers. SHA checks and row selection use those exact buffers.
Reading does not follow a subsequently advanced mutable pointer. Corruption,
outer identity/value mismatch or daily/weekly generation mismatch yields an
explicit independent refusal, never another date, v1 or EOD fallback.

Expensive full evidence checks run before the final live publication clock.
Final projection reuses verified periods, checks current clocks, filters external
facts against the native finalizer and reselects news at publication time.
The canonical snapshot reader and factual export replay the same contract.
The compact HTTP/manual package includes both bundles with versioned evidence
references and digests; raw base64 buffers remain in the canonical snapshot/full
audit and are excluded from compact output.
Original source identity, proof references, times and refusal evidence remain in
the snapshot; aggregate completeness is `PARTIAL`, never `READY` while required
external sources, risk or data are absent. `selection_policy_ready` describes
the implemented selection policy only.

External macro/oil/news context and missing external requirements are separate
from current market. Explicit Stage8 validated input remains optional; an
explicit manual position alone does not prove a full account/risk state.

The existing Stage9 CLI defaults to v2 reading the canonical saved snapshot;
Python callers select `schema_version="rub_analysis_bundle.v2"` explicitly.
The historical Python API default remains v1 with its existing 20/24 blocks.
Stage10 and its smoke calls are unchanged. Old v1 contracts and accepted
historical artifacts are not rewritten.

## Scope and validation

Changes are limited to Stage9 policy/declarations, its existing snapshot callers,
canonical factual projection/export and
focused regression tests. Generic source resolvers, registries, existing
governance/admission engines and scheduler behavior are unchanged.

Initial focused run: `PYTHONPATH=.:src python -m pytest -q
tests/test_stage9_analysis_bundle_v2.py tests/test_step9_rub_analysis_bundle.py`
plus `tests/test_live_market_publication_freshness.py` — **40 passed** using the server's standard Python/real PyArrow in an isolated
checkout. Fixtures are synthetic/reconstructed, including the Si +6 rejection;
they are not production replay. Full CI and final exact-head review are required
before merge/apply; results will be recorded in the PR.

Expanded focused regression run: **527 passed**, including the new real
refresh/JSON/reader/compact/export chain, FUTOI identity inventory, CR dated and
statistics refusal classification, malformed macro components, factual package
and the unchanged Stage10 scheduler tests. The initial full isolated run found
18 implementation/compatibility failures; these were corrected and the affected
regressions pass. Five environment-sensitive failures were independently traced
to the test checkout being under `/tmp` (two path guards), the existing production
API on port 8765 (two transport tests), and the required clean Git executing tree
(one tracked-policy test). None of those tests were disabled or modified.

Independent preliminary review findings corrected: publication-clock external
fact expiry, evidence I/O after final publication clock, cross-scope D1 generation
consistency, complete frozen Stage7 carrier, missing compact bundles and raw
buffer leakage into compact output. Final exact-head review and CI remain gates.
The reverse completeness gate now validates Stage9 scope/section inventories,
source values, identity, full evidence and refusals. A final **98 passed** focused
run covers factual release/package and new Stage9 tests, including omitted,
altered and extra projection elements and direct frozen compact refusal.
Exact-head review of `7bb7e39de0f49bf43604d550e720909e6aa2d517` additionally found
that the new reverse oracle received the original snapshot clock. The corrective
change passes the reconciled read-time view, including independent current v2
revocations, and tests frozen acceptance at +1, +61 and +1201 seconds. The
corrected head requires a new independent review and complete CI checks.

Server apply is authorized only for the confirmed merged SHA. Runtime acceptance
must inspect a newly saved snapshot and canonical read, daily/weekly section
dates/statuses, unchanged accepted pointer hashes and APPLIED_CODE_SHA. Correct
stale/refused current data or incomplete external/risk coverage is an expected
limitation, not permission to extend TTL or claim full READY.
