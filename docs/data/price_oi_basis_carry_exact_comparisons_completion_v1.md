# PROJECT=MOEX_Bot

Task ID: `price_oi_basis_carry_exact_comparisons_completion_v1`
Action: change. Issue: #553. Base: `db6333ad6c954cea5733c25c7baa4a244a23ce20`.
Branch: `codex/price-oi-basis-exact-comparisons-v1`.
Route: browser_controlled_github_route. Mutation owner: owner's current Codex session.

## Result and bounded scope

The existing heavy snapshot refresh now supplements missing exact Price/market-OI
1/5/20 bases and dated own-leg basis/carry 1/5 bases. Existing admitted Stage3/4
evidence remains first choice. Official exact-SECID TradeStats and the existing
CETS 1m-to-5m normalization supply missing targets under a separate versioned
admission. This does not manufacture accepted Stage3/4 runs or rewrite their
admissions, archives or pointers.

Mutation allowlist finalized from the actual callers:

- `contracts/intelligence/exact_comparison_sources_v1.json`
- `src/moex_data/rub_exact_comparison_sources.py`
- `src/moex_data/rub_exact_comparisons.py`
- `src/moex_data/rub_contract_price_market_oi_observed.py`
- `src/moex_data/rub_historical_basis_carry_context.py`
- `src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_live_market_oi.py`
- `tests/unit/test_exact_comparison_sources.py`
- this task document.

No HTTP/MCP transport, services, scheduler, Stage10 dispatcher, FUTOI history,
Stage5/PIT/model/trading authority or TTL changes. No full-history backfill.
Source GETs occur only in the existing heavy refresh. Canonical reads, final
publication, Stage9 and compact export replay the saved evidence without GETs.

## Selection and evidence

The shared accepted observed-date witness determines the targets before source
lookup; no nearest-date substitution occurs. A current role binding selects exact
SECIDs; it is not proof of historical front/next roles. Four contract identities
and applicable expiries are replayed from the existing original FORTS reference
bytes. Supplementary sources are limited to the selected dated/current targets,
at most seven dates and 37 individual source/date keys within 45 calendar days.
The cache is `state/evidence/exact_comparison_sources_v1`; the snapshot envelope is
`accepted_exact_comparison_sources` (`exact_comparison_evidence.v1`). Existing v1
source and historical envelopes remain readable and unchanged.

Each original HTTP response has SHA256, exact request identity, start/receipt
clocks and full pagination inventory. Replay parses the same verified bytes.
Rows are exact SECID/date/bar; native OI is integral and unscaled, with integers
above 2**53 retained exactly. Ambiguous large floating OI is refused. Selection
precedes numerical quality: bad latest observations cannot expose older rows.
Basis metrics use their own two-leg timestamp intersection, normalization and
expiry tenor. Missing CETS only blocks dependent metrics. USD spot remains
outside admission. Sparse observed endpoints never claim session completion.

Each comparison carries exact target, original baseline, numeric changes or a
specific refusal, and evidence references. Each horizon has explicit available /
required coverage; Stage9 sees partial coverage instead of AVAILABLE anchors
concealing missing lags. First economic acceptance survives repeated captures
and expiry. A separate failed-attempt clock controls retries; failures do not
renew the evidence lifetime. The shared byte budget is sticky before subsequent
requests or cache writes and includes original binding buffers.

## Actual source verification, 2026-09-29

Candidate code was exercised in an isolated directory against a copy of the
server snapshot and official responses. Production code was not changed by this
check. Dated anchor: **2026-09-28**. Exact lag targets: **2026-09-27 / 2026-09-23 /
2026-09-06**. These are runtime evidence, not hardcoded targets or synthetic data.

| SECID | Anchor price / OI | Lag 1 base price / OI | Lag 5 base price / OI | Lag 20 base price / OI |
|---|---|---|---|---|
| SiZ6 | 85950 / 10747936 | 85523 / 10678370 | 86712 / 10378834 | 87051 / 3919072 |
| SiH7 | 87512 / 84510 | 87058 / 83020 | 88198 / 51956 | 88834 / 28398 |
| CRZ6 | 12.840 / 40776638 | 12.750 / 40645420 | 12.928 / 42861364 | 13.038 / 17488606 |
| CRH7 | 13.031 / 2373362 | 12.944 / 2357348 | 13.134 / 1364730 | 13.255 / 201966 |

Price/OI coverage: **4/4 at each of 1/5/20** (12 numeric comparisons).
Basis/carry coverage: **14/30 at lag 1**, **22/30 at lag 5**. CETS 2026-09-27
returned EMPTY; USD spot is not admitted on either target. Independent futures,
perpetual and calendar metrics remain numeric. No attempt to manufacture 30/30
READY. CRH7's own 2026-09-27 last price/OI endpoint is 18:55 MSK; SiH7's 2026-09-06
endpoint is 17:55 MSK. They are not relabelled as other contracts' 19:00 endpoints.

## Validation and publication status

Synthetic tests use real JSON bytes, Parquet witness/archives, the actual heavy
refresh, persisted JSON, Stage9 daily/weekly, canonical reader and compact/manual
export. Only source I/O and unrelated sibling producers are substituted.
Targeted commands:

```sh
PYTHONPATH=.:src python -m pytest -q tests/unit/test_exact_comparison_sources.py
PYTHONPATH=.:src python -m pytest -q tests/unit/test_contract_price_market_oi_observed.py tests/unit/test_historical_basis_carry_context.py
python -m compileall src tests
PYTHONPATH=.:src python -m pytest -q
```

Existing Price/OI and basis/carry regression suites: **227 passed** on the
isolated server Python/Parquet environment. The initial new source/refresh suite:
**28 passed**, including saved JSON, canonical read and compact/manual export;
additional receipt and precision regressions are included in final CI.

Preliminary independent review found and corrected first-acceptance renewal,
legacy metric isolation, aggregate acquisition budget, and retry-clock defects;
regressions cover all four. Final full-head review and CI must be recorded on the
published task PR. Merge and server apply are separate decisions for #553; prior
task authority is not reused. Do not close #553 based solely on this document or
component labels. At this checkpoint production APPLIED_CODE_SHA remains
`db6333ad6c954cea5733c25c7baa4a244a23ce20`.
