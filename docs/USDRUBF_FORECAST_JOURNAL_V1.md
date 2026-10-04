# USDRUBF forecast cycle — current runbook

PROJECT=MOEX_Bot; Task `usdrubf_forecast_journal_v1`; PR #561.

## Current supported workflow

The downstream interface is `moex_research.consumers.usdrubf_forecast_cycle`.
It reuses the canonical snapshot reader and the existing journal. The server
producer still supplies facts only. No forecast generation, order placement,
new scheduler, model training, promotion or paper execution is enabled.
The implemented mode is **FORECAST_OBSERVATION**; paper P&L is null.

Configuration: `configs/research/usdrubf_forecast_cycle.v1.json`. Its server
research path is a new suggestion, not a claim of an existing production journal.
Always pass `--root`. New journal directories are private (0700); use an
owner-controlled private parent. Never commit actual snapshots or positions.

The commands below assume the existing activated environment, repository working
directory and `PYTHONPATH=.:src`. Run each separately. Redirected request/ref files
belong in a private working directory; choose fresh filenames for new versions.

### 1. Capture the actual canonical input

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 capture --current --id input-001 > input-ref.json
```

`--source exact-export.json` imports an existing logical snapshot or the existing
`rub_snapshot_references.v1` / `rub_snapshot_storage.v1` carrier. Original transport
bytes and expanded logical bytes have separate hashes. The existing decoder
checks expansion length, digest and schema. No history is truncated; over 64 MiB
is refused. A standalone import is **EXTERNAL_UNVERIFIED** for baseline origin;
the original source admission is not proved by a self-asserted package field.
`--current` uses the repository canonical reader and records
`CANONICAL_READER_OUTPUT`; only this route grants `CANONICAL_FIELD` baseline status.
This is local process provenance, not a signature or source-level PIT audit.

The reference is bound to exact USDRUBF/source identity, LAST field and timestamp.
Registration has no maximum baseline age (`canonical_baseline_max_age_seconds:
null`). It uses the price and source-usability decision preserved in the frozen
input without reapplying a live-reader TTL at `issued_at`. The original
`reference_price_at` is retained. Future, foreign, contradictory, missing or
explicitly unusable observations are refused; source observation, receipt,
availability and issue times must remain ordered.
RFUD USDRUBF LAST uses the independently checked RUB/USD instrument mapping in
`contracts/datasets/position_risk_scenarios.v1.json`; conflicting declared units
are refused. An imported package does not inherit live freshness. This registration
policy does not change producer/live-reader freshness rules or recover a price
omitted from an export. A package with no admitted USDRUBF price still cannot register.

### 2. Prepare and register the analyst's forecast

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 template forecast > forecast-request.json
```

Fill the nulls with the actual original forecast, interpretation, method version,
DAY/WEEK label, exact horizon boundaries, neutral band, explicit observation grid
and its provenance/completeness scope. A grid is not inferred from weekdays or
observed rows. The approved factual adapter consumes explicitly listed 5-minute
intervals spanning day/1–3-day or week/1–4-week horizons. Arbitrary calendar or
intrabar completion is not inferred. Scenario objects retain the legacy schema
below: direction, optional activation/confirmation, targets and invalidation.
No probability or trading size is invented. `issued_at` is optional and filled
from the local registration clock. Historical imports should explicitly set
RETROSPECTIVE; actual late registration cannot become forward evidence regardless
of a claimed older issue time. Synthetic fixtures must set SYNTHETIC.

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 register --id forecast-001 --input-ref input-ref.json --request forecast-request.json > forecast-ref.json
```

Instrument, source references, price/time binding and hashes are automatic.
External context is separately frozen with the legacy `capture` API below and
linked through `external_inputs` plus `context.external_context` entries
`{"input": <returned ref>, "interpretation": "..."}`. Their source metadata must
state actual availability and limitations; download time is not publication proof.

### 3. Attach explicitly supplied position and risk

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 template risk > risk-request.json
```

Enter a dated manual position, account limits, freshness policy, verified applicable
instrument parameters and explicit assumptions. No documented broker-export
format was supplied, so unknown broker formats are unsupported. `position: null`
means POSITION_ABSENT. Empty positions require `explicit_empty: true`; stale
positions yield POSITION_STALE. Neither is silently replaced by a remembered
account. Position IDs/versions are immutable; a changed position requires a higher
version and a risk predecessor/reason. The legacy supplied Stage 8 input may be
attached as `stage8_supplied`; its conservative gross semantics remain unchanged.

Each position has `id`, `instrument`, signed integer `contracts`, current account
`mark_price`, and ordered `tranches`. A tranche has `id`, signed `contracts_delta`,
and `assumed_fill_price`. These are supplied hypothetical fills, not orders.
The current price reference is the supplied account mark, so P&L is incremental
scenario change, not a fabricated broker variation-margin statement.

Money inputs are bounded exact decimal strings/integers (no binary floats).
The USDRUBF multiplier is 1000 RUB per 1 RUB/USD move; tick size 0.01 costs 10 RUB.
These parameters were checked against [MOEX](https://www.moex.com/ru/derivatives/perpetual-futures/usdrubf)
on 2026-09-30. Historical applicability is not inferred from today's page.
Supply an applicable specification interval covering the scenario horizon.
Commissions, funding/roll and slippage are explicit total per-scenario assumptions;
unknown cost yields null net P&L. Unknown margin yields null modeled free funds.
Future funding is not a known rate. Stops do not guarantee fills across gaps.
Separate prefix portfolios, supplied account P&L and conservative gross exposure
are reported independently. Breaches are displayed; no reductions are executed.

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 risk --id risk-001 --forecast-ref forecast-ref.json --request risk-request.json > risk-ref.json
```

### 4. Observe closed horizons and replay/report

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 template observation > observation-request.json
```

Fill the existing data root and the exact admitted Stage2 history bounds. The
adapter calls `accepted_quote_history` and reuses its real physical validator;
the underlying accepted-history resolver requires the full admitted date range.
Only bounded horizon-relevant partitions are decoded/frozen. Missing admitted
data produces retryable NOT_EVALUABLE, never source substitution or a backfill.

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 observe --request observation-request.json --forecast-ref forecast-ref.json > observation-result.json
```

For multiple forecasts, supply their previously returned references in `forecasts`
and omit `--forecast-ref`; at most 100 forecasts per pass. An unfinished horizon
is PENDING. Closed source bytes, admission anchors and facts are frozen before
assessment. Restart resumes those bytes, checks their integrity and avoids duplicate
results. A null watermark means no closed bar was observed.

Corrected facts require an explicit new assessment: request `revision` contains
`supersedes` (the previous observation ref) and `reason`, with exactly one forecast.
Old facts/assessments stay readable. The revision is independently immutable and
does not replace the first observation. Report only one chosen version per root
forecast; duplicates/revisions cannot inflate the denominator.

```sh
python -m moex_research.consumers.usdrubf_forecast_cycle --root /home/trader/moex_bot/data/research/usdrubf_forecast_journal_v1 report --run-result observation-result.json > forecast-report.json
```

Reporting reproduces saved evaluations with no source refetch. It separates
method/horizon/registration class, coverage, direction, scenario outcomes and
unverifiable reasons. Accuracy, monetary risk and paper P&L are not merged.
Direct replay also supports `reproduce --evaluation-ref FILE`.

## Replay, revisions and validation boundary

Forecast v1 remains readable; v2 adds original text/context/grid provenance and
baseline binding. Evaluation v2 records all executable Python/.inc helpers,
contracts/config hashes, Git revision, Python and installed dependency versions.
Reproduction refuses a different runtime. Use the recorded code/environment for
old assessments; old records are never rewritten. Source hashes detect tampering,
not privileged owner rewriting. Preserve returned references outside the journal.

Identical retries preserve original clocks. Different content under the same ID
fails. New directories and retry acknowledgements fsync directory entries; tests
exercise call ordering/fault boundaries/concurrency, not actual power failures.
The CLI emits one JSON result to stdout, exit 0; malformed/schema/integrity/I/O
errors exit 2 without traceback. PENDING and retryable NOT_EVALUABLE are valid
results. Templates contain nulls deliberately and must be completed by the owner.

Aggregate resource gates apply in addition to individual array limits: declared
bars times the sum of `(1 + target_count)` across scenarios cannot exceed 2,000,000.
Risk output is limited to 10,000 prefix/level rows and 2,000,000 conservative
prefix/level/portfolio-term work units. Oversized combinations fail before
scenario scoring or risk-result materialization, with the same clean exit 2.

## Existing research evidence

`research --id ID --request FILE` freezes existing Phase06/06A/07 or S7.2 outputs.
Request schema `usdrubf.research_evidence_request.v1` has `kind` (PHASE06, PHASE06A,
PHASE07, S7.2), exact `directory`, `inputs` mapping and optional `reproduction`
with exact comparison `directory` and `artifact_names`. It does not run training.
`experiment --id ID --request FILE` preregisters explicit hypothesis/rules,
horizons, baseline, metrics, costs, exclusions, samples and prior experiment refs.
Viewed/historical samples cannot become fresh OOS; overlapping OOS is refused.
Overlap checks cover declared and linked samples, not unknown external research.

On 2026-09-30 the unchanged Phase06, Phase06A and Phase07 runners reproduced the
existing 2026-09-16 archive from their exact six hash-verified inputs. All 16
compared substantive outputs matched byte-for-byte; run manifests naturally have
different run identities. Phase07 keeps three independent entry dates, all 5/10/20
session horizons and the full stop/sizing grid; it does not select a best stop.
No new edge, OOS result or strategy promotion is claimed.

No S7.2 runtime outputs were found in the bounded server research/validation
inventory. Its code and contract remain unchanged. The reader requires its
yearly/sample/sparse-vs-complete policy fields and marks legacy manifests without
original partition hashes BLOCKED_MISSING_EVIDENCE. Majority-class reference
remains post-hoc descriptive. Artifact capture alone is not research acceptance.

No actual owner forecast/position or completed real forecast cycle was supplied.
Synthetic tests establish mechanics only. Current source bundle remains PARTIAL;
optional source gaps do not manufacture missing prices, positions or outcomes.
The data-refresh incident #539 and general futures refresh remain separate.

---

# Legacy explicit-input API reference (v1 forecasts)

PROJECT=MOEX_Bot

Task ID: `usdrubf_forecast_journal_v1`

## Boundary

Research-only, explicit-input registration and evaluation. This is not a forecast
generator, a new collector, a replacement for Stage 8, a trading policy, or a
broker/scheduler/Telegram integration. No existing live bridge or snapshot
consumer is modified. Import the exact JSON already used by the analyst; do not
refetch a source later and call it the original input.

Original core implementation (additional files are listed in `USDRUBF_FORECAST_CYCLE_SCOPE_V1.md`):
- `src/moex_research/intelligence/usdrubf_forecast_journal.py`
- `src/moex_research/intelligence/usdrubf_forecast_evaluation.py`
- `tests/unit/test_usdrubf_forecast_journal.py`

The existing `ShadowJsonStore` and `evaluate_intelligence_quality()` are not
used as an append-only journal or scenario evaluator: their runtime/decision
contracts differ. They remain unchanged. No artificial confidence, position,
trade state, or zero portfolio is manufactured for compatibility.

## Three record kinds

`input`: an exact JSON byte object plus source metadata. SHA-256 covers the raw
bytes, including formatting. Each external addition is captured as another
input record. Metadata has exactly:
`source_ref`, `schema_version`, `code_revision`, `data_as_of`, `available_at`,
`received_at`, `quality_limitations` (an explicit list, possibly empty).
Source clocks satisfy data <= available <= received <= registrar clock.

`forecast`: an immutable structured forecast referencing exact input record
hashes, with method version, issue time, reference price/time, explicit horizon,
bias/range/scenarios and an explicit observation grid.

`evaluation`: an independent immutable record linking the exact forecast, exact
future-facts bytes and metadata, evaluator version, runtime inventory,
registration classification, and the deterministic criterion report.
A new evaluation never edits the forecast. A corrected factual source requires
a new evaluation ID; the old assessment remains available.

Every reference has exactly `kind`, `id`, `sha256`. The digest covers the whole
stored record, including the local registration timestamp and source references.
Store returned references outside the journal to retain an independent hash anchor.

## Persistence and trust

The caller must explicitly supply the journal root. There is no default server
path, environment-based discovery, directory scanning, `latest` selection or
mutable current pointer.

POSIX only. The journal creates `objects/` and `records/` inside the supplied,
owner-controlled root. Temporary writes are flushed and fsynced, then published
using an atomic no-replace hard link. The directory is fsynced. Final files are
read-only. Regular-file/no-final-symlink reads and digest verification are used.
Concurrent identical submissions return the same reference. Conflicting contents
under an existing ID fail; retries do not move the original timestamp.
Interrupted submissions may leave unreferenced objects; they do not become
committed records. No automatic cleanup is implemented.

This is not a WORM device, an access-control service, or external notarization.
A privileged filesystem owner can modify files and recompute hashes. Parent
directories must remain controlled and must not be replaced concurrently.
The clock is sampled again after validation, with rollback rejection. The
registration time is a local timestamp near publication, not an independently
certified wall-clock receipt; publication can lag that timestamp.

`PROSPECTIVE_LOCAL` means the stored registrar timestamp is no later than the
frozen horizon start. `RETROSPECTIVE` means registration happened later.
Old chat imports do not become prospective by claiming an earlier `issued_at`.
Neither classification asserts externally verified point-in-time provenance.

Source times/schema/code metadata are preserved caller declarations. The journal
does not infer or validate arbitrary nested source schemas, certify source clock
accuracy, prove the reference price from a JSON path, or audit hidden future
information inside an opaque package. Existing canonical source/schema/PIT
validation remains necessary. Declared future inputs relative to `issued_at`
are rejected. Do not call this boundary a full source-level leakage audit.

## Forecast schema

Schema: `usdrubf.forecast.v1`. All fields are required; absence of an optional
analytical claim is represented explicitly by null or an empty list, not guessed:

| Field | Meaning |
| --- | --- |
| `schema_version` | Exact forecast schema above |
| `instrument`, `contract` | Both exactly `USDRUBF` in this slice |
| `issued_at` | Analyst's declared issue/cutoff time |
| `horizon_start`, `horizon_end` | Explicit aware timestamps; start < end |
| `reference_price`, `reference_price_at` | Frozen analytical baseline, not a simulated fill |
| `bias` | `BULLISH_USD`, `BEARISH_USD`, `NEUTRAL`, or null |
| `neutral_band_bps` | Explicit inclusive neutral band, 0..10000 |
| `range` | `{lower, upper}` for full-grid containment, or null |
| `method_version` | Explicit analyst/method version |
| `inputs` | Nonempty, distinct input record references |
| `observation_grid` | Ordered `[open_at, close_at]` pairs |
| `scenarios` | Scenario objects, or an empty list |
| `supersedes`, `revision_reason` | Both null for an original; exact prior reference and reason for a revision |

Reference time <= issue time <= horizon start. Every linked source receipt must
be <= issue time. A revision must use a new ID, reference an existing forecast
for the same contract, and cannot predate it. It never changes the original.
A mid-horizon update must declare its own remaining horizon and observation grid.

Prices and bands are decimal strings or integers, never binary floats or bools.
The resource bound is 30 coefficient digits, exponent -12..12, value <= 1e12;
prices are strictly positive. Decimal evaluation uses an explicit arithmetic
context, independent of the caller's precision, rounding or traps.
JSON duplicates, nonfinite/overflowed numbers and unknown schema fields fail.
Artifacts are bounded at 64 MiB; arrays at 100000 entries.

## Explicit observation grid, not an invented calendar

The grid must start/end at the declared horizon boundaries. Each interval is
positive, ordered and nonoverlapping. Gaps are allowed only as explicitly
declared in the frozen plan (for example, nontrading intervals). The code does
not infer weekdays, exchange sessions or holidays.

`COMPLETE_FOR_DECLARED_GRID` is completeness against this plan only, not proof
that every real market interval is represented. A grid must be sourced and
reviewed before registration. No after-the-fact denominator selection is added.

## Scenario semantics

Each scenario has exactly `id`, `direction`, `activation`, `confirmation`,
`targets`, `invalidation`. IDs are unique; direction is bullish or bearish USD.
Targets are distinct positive decimal levels. Invalidation is a level or null.
For bullish scenarios it must be below all targets; for bearish, above them.

Activation and confirmation are either null or
`{"op":"GE"|"LE", "level":"decimal", "closes":positive_integer}`.
They operate on scheduled closed bars only. The close count resets when the
condition fails. Consecutive means consecutive grid observations, including
across explicitly declared session gaps.

Null activation starts at the declared horizon start. Null confirmation confirms
at activation. Otherwise confirmation requires later bar closes: the activation
bar is never reused as a confirmation bar.

Invalidation is cancellation of the scenario from the start of its horizon,
including before activation or confirmation. Pre-trigger cancellation is not a
trade loss. Once confirmed, target/invalidation touches use subsequent bar
highs/lows. An activation/confirmation bar's earlier intrabar extremes do not
become post-confirmation hits. A target already passed at confirmation close is
`NOT_EVALUABLE`, not a fabricated winning entry.

Each target is evaluated independently against the first later invalidation:
`TARGET_FIRST`, `INVALIDATION_FIRST`, `UNKNOWN_ORDER`, or `NOT_REACHED`.
Pre-activation/unconfirmed/cancelled scenarios have `NOT_APPLICABLE` targets.
If target and invalidation occur within the same OHLC bar, v1 conservatively
reports `UNKNOWN_ORDER`; it does not invent intrabar ordering, even from the open.
Touch evidence identifies a bar interval, not an invented exact tick timestamp.

Scenario statuses distinguish `NOT_ACTIVATED`, `NOT_CONFIRMED`,
`CANCELLED_BEFORE_ACTIVATION`, `CANCELLED_BEFORE_CONFIRMATION`,
`ACTIVE_AT_HORIZON_END`, `TARGETS_REACHED`, `TARGETS_BEFORE_INVALIDATION`,
`INVALIDATED`, `AMBIGUOUS`, and `NOT_EVALUABLE`.

This bounded condition language is not a parser for arbitrary natural-language
retests, tick-path patterns, execution stops or exchange order types.

## Factual outcomes and per-criterion exclusions

Factual schema: `usdrubf.forecast_facts.v1`, with exactly
`schema_version`, `instrument`, `contract`, `bars`.
Every bar has `open_at`, `close_at`, `open`, `high`, `low`, `close`.
Require exact instrument/contract, known grid intervals, causal closed bars,
chronological order, unique intervals (including timezone-equivalent instants)
and valid OHLC geometry. Foreign, duplicate, out-of-grid and future rows fail;
they are not silently filtered.

The horizon must have ended before evaluation. Missing planned bars are listed.
Any declared factual source quality limitation prevents criterion evaluation.

Direction compares the exact terminal bar close with the frozen reference
price. Neutral includes both band boundaries. A missing middle bar does not
prevent endpoint direction evaluation; a missing terminal bar does.
A null predicted bias has `correct=null`, never a made-up neutral prediction.

Range means containment of ALL grid lows/highs inside the declared interval.
Missing bars prevent range and scenario evaluation, even if some observed
touches look favorable. Missing or ambiguous outcomes are not zero returns,
successes or losses. No aggregate win rate, P&L, confidence calibration,
cost model, position sizing or trading recommendation is produced.

## API and command entrypoint

`ForecastJournal(root)` exposes:
`capture(id, exact_json_bytes, metadata)`,
`register(id, forecast_spec)`,
`evaluate(id, forecast_reference, exact_facts_json_bytes, metadata)`,
`read(reference)` and `reproduce(evaluation_reference)`.

The module is executable with
`python -m src.moex_research.intelligence.usdrubf_forecast_journal`.
Required `--root` precedes the subcommand.
- `capture`: `--id`, `--source`, `--metadata`
- `register`: `--id`, `--spec`
- `evaluate`: `--id`, `--forecast-ref`, `--facts`, `--metadata`
- `reproduce`: `--evaluation-ref`

All file arguments identify explicit existing local files. The first three
commands print a JSON record reference; save that exact output for the next step.
`reproduce` verifies all linked bytes and the evaluator fingerprint and prints
the recomputed report, rejecting a mismatch. No network access is used.
The injectable library clock exists for tests; the CLI does not accept a
backdated registration-clock argument.

## Validation and acceptance boundary

Tests contain synthetic fixtures only, including a real subprocess CLI round trip.
They cover append-only/idempotent/conflicting/concurrent writes, hash tampering,
symlinks, clock rollback, retrospective classification, revisions, source causality,
invalid JSON/prices/grids/OHLC, partial horizons/data, neutral boundaries, bullish
and bearish touches, confirmation causality, and unknown same-bar ordering.

A synthetic round trip validates mechanics, not real forecast performance.
Independent exact-head review and full repository CI are separate gates.
A real prospective forecast cycle and server application have not been performed
by adding this implementation. Merge and server apply need separate authority.
