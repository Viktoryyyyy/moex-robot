# Live FUTOI date and source-identity repair

PROJECT=MOEX_Bot

- Task ID: `futoi_live_date_identity_repair_v1`
- Issue: #547; related operational incident: #539.
- Action: change (implementation includes a mandatory read-only preflight).
- Route: `browser_controlled_github_route`.
- Inspected main: `748462b282441b362e43bed1c07b565b97f681ac`.
- Task branch: `task/futoi-live-date-identity-repair-v1`.
- Merge: manual, separately authorized against the reviewed full head SHA.
- Server apply / source refresh / historical backfill: not authorized.
- Status at introduction: task contract only; implementation and tests are not yet complete.

The owner requested direct handling of the PM L2 report on 2026-09-24 rather than manual transfer between chats. This document records the bounded PM L2 change scope and PM L1 integration decision. It is not evidence of a successful implementation, executed tests, source replay or runtime acceptance.

## Decision

Separate the observed-date witness, source FUTOI identity and contract-series metadata.

1. Use the existing observed TradeStats resolver with explicit witness instrument `usdrubf_futures_family` / `USDRUBF`. Its dates are candidates only, not proof of Si/CR FUTOI availability or quality.
2. Retain canonical `instrument_id` values `si_futures_family` and `cr_futures_family` and source ID `moex_algopack_futoi`. New live source identity is `source_ticker=si/cr` with `source_identity_scope=source_ticker_root`.
3. Do not copy registry `SiU6`/`CRU6`, mechanically replace U6 with Z6, or assign any other series SECID to root-scoped FUTOI. The observed source response has ticker but no individual-contract SECID. No contract-series field is needed for this live raw identity. Any retained optional contract context must be separately labelled non-source metadata and cannot participate in raw keys, source matching or factual authority.
4. Implement opt-in v2 inside the existing materializer and existing live refresh commands. Do not add a parallel producer, scheduler or refresh CLI.

## Compatibility and storage boundary

Historical raw v1 remains unchanged: default materializer API, required columns, source key `(trade_date, sess_id, seqnum, secid, clgroup)`, paths, partitions, manifests, content pins, accepted pointers and SHA-linked evidence. Do not rewrite, migrate, re-hash as original evidence, or relabel historical v1 artifacts.

The explicit v2 raw key is `(trade_date, sess_id, seqnum, source_ticker, clgroup)` inside an instrument/source-separated dataset. V2 declares its version and root scope in both metadata and the reader contract. Unknown versions, incompatible metadata or a version/path mismatch must fail closed.

The approved v2 storage namespace is separate from v1:

- Raw: `${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/schema_version=v2/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`.
- Quality: `${MOEX_DATA_ROOT}/state/quality/dataset_id=futures_futoi_raw/schema_version=v2/run_date={YYYY-MM-DD}/run_id={RUN_ID}/quality_report.json`.
- Manifest: `${MOEX_DATA_ROOT}/state/refresh/dataset_id=futures_futoi_raw/schema_version=v2/run_date={YYYY-MM-DD}/run_id={RUN_ID}/manifest.json`.

These are proposed repository contract patterns, not claims that corresponding server files exist. Declare them consistently in the new contracts and the existing data-lake configuration before using them. Preserve the current-main accepted Stage2 history state; do not overwrite it with older PR configuration.

The existing content-addressed live evidence mechanism is reused. Every new proof must carry enough explicit raw-version/source-scope information to replay the exact SHA-verified byte buffer. A known historical v1 proof continues through the v1 validator; new v2 proofs use v2 validation. Missing version metadata is accepted only under the known legacy v1 envelope, never as a generic permissive default.

An explicitly selected v2 artifact that is missing, rejected, malformed or unreadable must not trigger an implicit v1/EOD fallback. Existing explicitly identified historical v1/accepted-EOD paths retain their original scope and admission rules. Version selection must be propagated through affected readers; it must not depend on a directory scan, guessed cutover date or whichever file happens to validate.

Live context envelopes require explicit v2 schemas. Existing persisted v1 envelopes remain readable as dated legacy evidence; no old retained fact may become FRESH for a new requested date. Preserve strict equality checks when recomputing factual from frozen bytes.

## Execution preflight and ownership

PM L1 gives #547 integration priority for its bounded live repair. Coordination notices on #369 and #371 place further shared-file mutation/integration on hold without modifying, closing, rebasing or superseding their existing branches.

Directly inspected overlapping PRs:

- #369, head `228c73f69b2c9201090074561de49be060cadd34`: `materialize_futoi_instrument.py` and `futures_data_lake.v1.yaml`.
- #371, head `ed2a28e6f8b1c3becf67652be6905c0be981bb61`: `futures_data_lake.v1.yaml`; its v1 documentation/contract changes are not imported into this repair.

`contracts/registries/contract_registry.v1.yaml` is absent from the inspected main and is NOT part of this change. Do not create a new registry subsystem or pull in the old #309 registry stack. Use bounded declarations in the existing data-lake config.

Open PR state, old commits or a hold notice do not prove that an executor has stopped. Before production-code mutation, the one assigned implementation executor must:

- reconcile current main, this branch, this PR and #547; check for existing attempts, competing active/timed-out executors and new movement of relevant heads;
- enumerate current open PR changed files and reconcile actual overlaps, not just titles or aggregated search summaries;
- perform a local checkout reverse-dependency search for `source_identity`, `latest_aligned_factual`, `_materialize_target`, materializer path helpers, raw/context schema constants and version-sensitive serializers;
- verify storage/source/contract declarations and all affected tests/callers against this exact checkout;
- record the checked paths and resulting dependency/overlap matrix here or in the PR.

The earlier connector code search missed known symbols and does not prove absence of other callers. This complete checkout preflight is still required. If correctness requires a path outside the allowlist, or an active owner cannot be reconciled, stop before production mutation and report the precise file/caller or ownership conflict to PM L2/PM L1 in this same PR. Do not widen scope, start a replacement branch/PR or ask the owner to relay a new chat prompt.

Only one executor owns this branch. The browser controller's initial mutation is this task document; it hands production mutation ownership to the single explicitly dispatched implementation task. No simultaneous browser/Flowise/second-Codex mutation is authorized.

## Exact allowlist

Only these existing files may change:

1. `src/moex_data/futures/materialize_futoi_instrument.py` — explicit live-v2 normalization, exact key/version dispatch, isolated paths and correctly ordered source receipt/ingest clocks; preserve v1 defaults.
2. `src/moex_data/futures/futoi_live_factual_refresh_source_native.py` — root identity, stable date witness, explicit version-aware factual validation/provenance and bounded failed-attempt evidence.
3. `src/moex_data/futures/futoi_intraday_previous_session_context.py` — v2 role envelopes and independent current/previous date resolution, materialization and errors, including the quality-probe path.
4. `src/moex_data/futures/futoi_intraday_previous_session_context_fast.py` — USDRUBF candidate witness and independently admitted role results without old-date substitution.
5. `src/moex_data/futures/futoi_delta_statistics_context.py` — explicit exact-date raw version selection/revalidation and no invalid-v2 fallback; preserve historical v1/accepted-EOD rules.
6. `src/moex_data/rub_si_futoi_dated_context.py` — replay versioned root identity from the same SHA-verified byte buffer and propagate explicit loader version; no admission expansion.
7. `src/moex_data/rub_cr_futoi_dated_context.py` — corresponding version propagation/replay, preserving CR current and dated admission boundaries and source-rejection behavior.
8. `src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_current_context.py` — role/schema/source-identity propagation and independent status attachment only.
9. `src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_futoi.py` — explicit known v1/v2 dispatch in `_load_candidate`; its current mandatory registry-SECID checks otherwise reject v2.
10. `configs/datasets/futures_data_lake.v1.yaml` — bounded live-v2 contract/storage declarations only; preserve existing Stage2 accepted-state content and all unrelated datasets.

New files permitted:

11. `contracts/sources/futures/moex_algopack_futoi.v2.yaml`
12. `contracts/datasets/futures_futoi_raw.v2.yaml`
13. `contracts/datasets/futures_futoi_quality_report.v2.yaml`
14. `contracts/datasets/futures_futoi_refresh_manifest.v2.yaml`
15. `contracts/datasets/futoi_live_factual_context.v2.yaml`
16. `contracts/datasets/futoi_intraday_previous_session_context.v2.yaml`
17. `docs/data/futoi_live_date_identity_repair_v1.md`
18. `tests/test_futoi_live_date_identity_repair_v1.py`
19. `tests/test_futoi_live_identity_compatibility_v1.py`

Do not change every allowed file merely to exhaust the list. If existing tests genuinely require a version-contract migration outside these two new test files, identify their exact paths and request a bounded PM L2 scope amendment in this PR before editing them. Do not weaken or disable existing tests to preserve the defect or obtain green CI.

## Required behavior

For fast snapshot, regular current/previous and separate source-native factual refresh, each role follows: witnessed candidate date, exact-date query for its own ticker, strict source validation, latest-pair selection, admission. A successful USDRUBF witness never substitutes for a successful FUTOI response.

Current and previous must be independent in both directions, including witness failures and `_quality_probe_both_dates`. A missing or rejected current must not erase an independently witnessed/validated previous. A previous failure must not erase a valid current. The instrument-level wrapper must isolate Si and CR failures.

No weekday, weekend, holiday, CETS-calendar or contract-expiry inference determines FUTOI availability. Preserve observed weekend dates. EMPTY, no witness, transport timeout, auth rejection, source ERROR_MESSAGE, malformed payload and wrong ticker/date remain distinguishable. An exact-date failure must not select an older convenient date.

Select the actual newest source timestamp/revision first, then validate the pair. Invalid newest data must not be filtered away in order to return an earlier valid pair. Preserve ambiguity checks for groups/sessions/revisions and conflicting source-record duplicates. Preserve source identifiers exactly, including `seqnum > 2**53`; do not pass them through float. Record selected FIZ/YUR revision identities and source clocks in versioned factual/provenance.

The inspected live `_as_int` currently converts via float. The v2 materialization must also not label a pre-request clock as the HTTP receipt: event, source publication, receipt/availability, ingest and validation/last-success clocks must be causally ordered. Retain the existing TTL and refusal behavior. All tests use injected clocks or frozen synthetic data, not live production calls.

Reuse the existing publication audit for both Si and CR to preserve rejected latest-pair evidence and exact frozen raw provenance. Do not change its admission policy or `futoi_current_pair_authority.py`. If a change to either engine is actually essential, stop with the concrete dependency for a scope decision; do not silently modify it.

For CR, retain `current_intraday_latest_pair_only`, governance/evidence digest checks, causal clocks and TTL. Legacy previous/delta consumer authority stays denied. Separately admitted dated/statistics consumers retain their existing limits. A valid pair is not session completeness, historical PIT, model usability, Stage5 readiness or trade authority.

## Saved diagnostic regression

Issue #547 records owner-supplied observations, not immutable full HTTP bytes.

- 2026-09-23, latest 23:50:00 Europe/Moscow: sess_id 7654, seqnum 201, systime 23:50:08. Si net FIZ/YUR +724653/-724653; CR +3510000/-3510000.
- 2026-09-24, latest 10:35:00 Europe/Moscow: sess_id 7655, seqnum 43, systime 10:35:10. Si +726369/-726363 is an imbalance of +6; total long 5224776 versus absolute total short 5224770. CR +3541989/-3541989.

Fixtures derived from this summary MUST be labelled synthetic/reconstructed. Do not claim production replay or invent unobserved fields as captured facts. Reconstruct missing fields explicitly as synthetic values consistent with the intended test.

The Si +6 newest pair must fail with no tolerance, rounding, older balanced pair or earlier-date substitution. A valid 2026-09-23 previous remains separately dated and independently usable only within its existing admission scope.

## Done when

- Expiration regression covers 2026-09-23/24 with unchanged old U6 registry metadata in all three live chains.
- Observed weekends, missing witness, EMPTY, timeout/auth/source errors, malformed payload, wrong ticker/date and independent instrument/role failures are tested.
- Latest +6 rejects even with an earlier balanced pair; failed-attempt provenance remains available; previous is not relabelled current FRESH.
- Duplicate/conflicting keys, ambiguous groups/sessions/revisions and exact identifiers above 2**53 are tested; no float-based identity loss.
- Source/receipt/ingest/validation clocks and current-pair TTL have positive and negative tests.
- V1 default API/key/path behavior and frozen-byte revalidation remain compatible. V2 paths cannot overwrite v1; unknown/mismatched versions and corrupt v2 fail without fallback.
- Source-native snapshot candidate reader, current-context attachment, dated captures and statistics loaders are covered through actual call paths. Existing historical proof equality and CR admission tests pass.
- No unrelated contracts, source grants, accepted pointers, scheduler policy, runtime settings or historical artifacts change.
- The executor reports exact changed paths, test commands/results and remaining failures. Independent current-head review and exact-head CI are separate gates; a draft PR or document is not implementation acceptance.

## Out of scope

Instrument registry/automatic roll, generic observed-date resolver changes, old v1 source/raw contracts, registry-stack cleanup, Stage3/4/5/7/10 orchestration, governance grants, accepted pointers, historical backfill/migration, Brent/oil, trading/model permission, server commands/apply, production API probes and refresh runs.

Keep issue #547 open until its required code and separately authorized operational acceptance are established. Do not claim that merge alone changes server state.

## Owner-authorized continuation, 2026-09-27

The owner separately authorized finishing this PR, independent final-head review,
successful final-head CI, guarded merge of PR #548 and exact merged-SHA server apply
with minimal native FUTOI/snapshot refresh. These operational stages supersede the
original change-only operational exclusion above; unrelated service/data policy stays
out of scope. No second implementation task was dispatched. The prior automated
execution reported DONE/NOT_PUBLISHED in issuecomment-5857941646; its local commit
was not imported. Work resumed from published head
`33dc068a0f1394b5867f0a83bb7096bcab8b48cf`, base
`748462b282441b362e43bed1c07b565b97f681ac`.

Scope amendments recorded by the owner:

- `src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_live_market_oi.py`:
  explicit v2 FUTOI calls only (issuecomment-5819295974).
- `src/moex_data/step10_rub_refresh_entrypoint.py`: explicit v2 native refresh and
  matching failure schema only, preserving nonblocking Stage7 behavior.
- `src/moex_data/rub_si_futoi_observed_statistics.py` and
  `src/moex_data/rub_cr_futoi_observed_statistics.py`: version/identity propagation,
  frozen replay and refusal evidence, no selected-v2 fallback or governance expansion
  (owner's explicit approval in the current execution; issuecomment-5858024651).
- `tests/test_rub_dated_hour_source.py` and
  `tests/test_usdrubf_s7_3_chat_analysis_snapshot_current_context_delta.py`: owner
  approved minimal migration of the old FUTOI mock/call expectation to explicit
  version and admission clock arguments; retain clock-causality and call-order checks.
- `tests/unit/test_futures_calendar_clock_context.py`,
  `tests/unit/test_historical_basis_carry_context.py`, and
  `tests/unit/test_si_futoi_dated_comparisons.py`: owner explicitly approved the
  same bounded mock/clock migration (issuecomment-5858572880). All original causal
  ordering, completion timestamps and refusal-to-publish assertions remain enforced.

The complete-checkout caller/serializer preflight found the regular snapshot runner,
fast live-market runner and independent Step10 FUTOI call. Their root-v2 selection is
now explicit through producer, native snapshot reader, delta builder and attachment.
Si/CR dated and statistics captures receive the selected version and preserve exact
root/session/revision identity through retained and compact-row serialization.
Default v1 APIs and their historical bytes, pointers, keys and hashes stay unchanged.
The isolated v2 declarations now truthfully mark explicit live callers enabled.

Review 5316858234 / inline 4103857780 is addressed by requiring the complete explicit
outer previous identity before replay. Copied factual/provenance cannot repair a
missing version, wrong instrument/source/ticker/scope or contract SECID. Native replay
requires the complete hashed publication audit as well as raw/quality/manifest bytes.
Replay errors and selected-role failed-attempt evidence remain diagnosable; an
explicit selected failure cannot fall back to an earlier canonical partition/EOD.
Current and previous admission are independently attached per instrument. CR keeps
its existing current-pair-only audit/TTL authority and separate dated/statistics grants.

Independent preliminary review additionally found nested receipt/ingest clocks in
economic identity comparisons. Retention now ignores receipt-only changes at both
levels while preserving source event/publication/session/revision changes. Statistics
proofs carry the revision identity that the compact numeric row format cannot encode.
No publication-audit engine, current-pair authority engine or grant was changed.

Independent complete-diff review also found that the native snapshot component
could retain a prior v1 component after selected-v2 candidate failure. Retention is
now limited to explicitly/default selected v1. Six actual-reader regressions cover
missing/corrupt/rejected v2 with ready prior v1/v2 components. Review of published
`32c0f57b2015ea8f2d100e287e761f13f9f167f6` passed; subsequent test-only changes
require fresh exact-head review and checks before merge.

Validation uses normal imports and real PyArrow Parquet in an isolated Linux checkout
with a separate temporary data root and cleaned environment. The two allowed test
files cover source-native plus regular/fast real refresh to saved snapshot/consumer,
both role-failure directions, Si/CR isolation, strict reconstructed +6 with an older
balanced pair present, full audit corruption, explicit outer identity, large IDs,
v1 compatibility, dated/statistics capture and repeated-observation first acceptance.
Fixtures are synthetic/reconstructed, not production replay. Full compileall/pytest,
independent exact published-head review, exact-head CI and operational results must
be recorded with actual SHAs in PR #548; this document does not certify pending gates.

Confirmed prepublication validation: **305 passed** (17.93s, Python 3.12,
PyArrow 24.0.0) using `PYTHONPATH=.:src pytest -q` with
`tests/test_futoi_live_date_identity_repair_v1.py`,
`tests/test_futoi_live_identity_compatibility_v1.py`,
`tests/test_rub_dated_hour_source.py`, and
`tests/test_usdrubf_s7_3_chat_analysis_snapshot_current_context_delta.py`.
`python -m compileall -q src tests` and `git diff --check` passed. The full
initial full run finished with **7404 passed, 12 failed, 37 subtests passed**
(1000.59s). Four failures were corrected in the 305-test run. Six were old attachment
mocks in the three subsequently approved test files. Both remaining MCP failures
were occupied production ports, not stopped for testing. The existing authorized
`moex-test-isolated` helper supplies a separate loopback namespace: **308 passed**
(18.69s) across both main FUTOI test files and both MCP test files, including the six
new snapshot refusal cases. All **210 tests** in the three final migrated files
passed (61.50s). A subsequent full run finished with **7,421 passed, 1 failed,
37 subtests passed** (920.43s): the remaining policy test correctly rejected
tracked pytest/bytecode files modified inside the temporary test checkout. The
final isolated checkout excludes generated caches from its Git baseline; the
policy check is unchanged. Full-suite and GitHub Python 3.11 checks remain separate
merge gates; actual final SHAs/results are recorded in PR #548.

Inline review 4116398275 found that direct v2 materialization could reuse the same
instrument/date/run ID and overwrite its existing attempt metadata. Any existing
v2 quality or manifest slot now rejects before fetch or writes, even when its
identity matches. V1 behavior is unchanged. Twelve real-file regressions cover
Si/CR, successful/failed first attempts, and both/quality-only/manifest-only slots;
they verify no fetch and byte-for-byte preservation of all retained evidence.
Both main FUTOI files plus duplicate-retry, block-selection and publication-time
materializer tests passed: **322 passed** (16.60s).

Historical failed run 36126639359 has a check annotation stating that the hosted
runner lost communication. Its job logs return BlobNotFound. This establishes the
reported termination condition, not its underlying cause and not a code-test pass.
Fresh final-head checks remain required. Overlapping PRs #369/#371 and their held
shared-file lanes were inspected and left unchanged. No AGENTS.md or the two exact
named v2 operating/parallel-lane documents were present in the complete checkout.
