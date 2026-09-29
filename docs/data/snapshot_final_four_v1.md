# PROJECT=MOEX_Bot

Task ID: `snapshot_final_four_v1`. Primary action: change.
Owner instruction: execute sequentially the four follow-up corrections to the
verified September 29 snapshot: USD_TOM/basis, CNY non-trading state, READY
semantics, and next-year currency calendar coverage. Publication, gated merge
and separate exact merged SHA server apply remain authorized in this task.

GitHub: Viktoryyyyy/moex-robot. Base: `5ffb08102344e0c062230ce7da950f825c50a4eb`.
Branch: `codex/snapshot-final-four-v1`. Mutation owner: this Codex session.
There is no competing task branch for this scope in the inspected open PRs.
Old PRs #369/#371 are not imported or modified. AGENTS.md and the two named v2
execution/parallel-lanes instructions were not found in the complete checkout.
Applicable merged management canon, amendment and Browser contexts were read.

## Result and boundaries

1. The existing live collector now explicitly selects the independent CETS USD
reference in both heavy and fast paths. The separate versioned contract names
USD000UTSTOM, CETS, RUB per USD and the ruble-settled currency-position instrument
without USD delivery. This is supported by https://www.moex.com/n97672 and a
September 29 native securities/marketdata response. Source update time cannot
renew LAST: native trade date + TIME, trading status and unchanged 60-second TTL
are checked with original response bytes on read. USD refusal does not revoke
independent Si/CR/CNY facts. Legacy default collector APIs retain their old scope.

The bounded existing exact-date source collector uses a v3 admission for USD
basis comparisons 1/5. It selects reviewed CETS dates before price quality, keeps
both own leg timestamps, hashes and first acceptance, and uses the existing 1m
CETS-to-5m normalization. USD basis/carry is explicitly a relative-price proxy
against the ruble-settled reference, not deliverable currency financing.
Historical v1/v2 and Stage3/4 pointers keep their original meaning and bytes.
The legacy Stage4 default still rejects USD; only the separately verified v3
contract can admit this supplemental reference. No backfill is performed: at
most three USD target dates are acquired, with own futures/perpetual legs.
The conservative v3 cap is 58 date-source combinations, 10 dates, the same 16MB
budget and 345600-second dated TTL. EMPTY, auth, timeout and bad selected evidence
remain exact-target refusals. No nearest-day or earlier-good-price fallback.

2. Native non-trading status N is distinguished from source failure and active
but stale data. The last observed quote retains its original clocks under an
explicit non-current role; non-trading is not proof of session completion.

3. `snapshot_status_presentation.v1` separates component processing status from
current market admission, partial-day observations and dated references. Saved
JSON, Stage9 daily/weekly and canonical/compact readers show these distinctions.
Legacy status fields remain compatible with their existing admission consumers;
the new presentation cannot grant fresh/complete/model/trading authority.
Legacy dated preparation retains its seven-instrument/22-metric admission;
the new current inventory requires eight instruments and all 30 basis metrics.
The USD v3 historical reference has its own exact-date Stage9 coverage and is
not silently added as a requirement to the old dated witness mechanism.

4. **BLOCKED_SOURCE_NOT_VERIFIED**: a reviewed 2027 calendar could not be
established from accessible official MOEX publications as of 2026-09-29. Both
explicit 2027 calendar page requests returned HTTP 403; official document 19577
exposes September/October 2026 revisions, and exact-name official-site searches
did not establish a 2027 publication. This is not proof that none exists.
The snapshot now exposes coverage through 2026-12-31 and NOT_VERIFIED for 2027.
Year rollover fails closed; no dates are invented from weekdays or the prior
year. A verified official 2027 calendar and its reviewed finite contract are
still required to complete the requested extension. This task must not be
reported as all four completed.

Unchanged: transport HTTP/MCP configuration, FUTOI balance policy, TTL values,
accepted pointers, broader macro sources, continuous Si/CR W1, weekly OI,
Stage5/PIT/model/trading authority and user position input.

## Exact implementation scope

- `contracts/intelligence/exact_comparison_sources_v3.json`
- `contracts/intelligence/snapshot_status_presentation_v1.json`
- `contracts/intelligence/usd_cets_reference_v1.json`
- `docs/data/snapshot_final_four_v1.md`
- `src/moex_data/live_basis_carry_context.py`
- `src/moex_data/rub_accepted_stage4_resolver.py`
- `src/moex_data/rub_analysis_bundle_v2.py`
- `src/moex_data/rub_cny_basis_calendar.py`
- `src/moex_data/rub_currency_market_state.py`
- `src/moex_data/rub_dated_context.py`
- `src/moex_data/rub_exact_comparison_sources.py`
- `src/moex_data/rub_exact_comparisons.py`
- `src/moex_data/rub_factual_package.py`
- `src/moex_data/rub_factual_projection.py`
- `src/moex_data/rub_factual_release.py`
- `src/moex_data/rub_factual_release_acceptance.py`
- `src/moex_data/rub_fast_market.py`
- `src/moex_data/rub_market_factual_delivery.py`
- `src/moex_data/rub_production_source_matrix.py`
- `src/moex_data/rub_snapshot_read_freshness.py`
- `src/moex_data/rub_snapshot_status_presentation.py`
- `src/moex_data/rub_usd_cets_reference.py`
- `src/moex_data/synchronized_live_market_oi_context.py`
- `src/moex_data/synchronized_live_market_oi_context_partial.py`
- `src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_live_market_oi.py`
- `tests/unit/test_currency_market_state.py`
- `tests/test_rub_readiness_isolation.py`
- `tests/test_rub_observed_range_levels.py`
- `tests/unit/test_exact_comparison_sources.py`
- `tests/unit/test_snapshot_status_presentation.py`
- `tests/unit/test_usd_cets_reference.py`

## Validation and delivery

Tests use ordinary repository imports and the existing server venv/PyArrow in
an isolated full checkout. Fixtures are synthetic/reconstructed, not production
replay. Required checks: `python -m compileall src tests`, `PYTHONPATH=.:src pytest -q`.
New cases cover USD outer identity/contract/native route and bytes, heartbeat
versus old trade, closed status, independent failure, source errors, exact 1/5,
first acceptance, v1/v2 compatibility, fast capture/disk/read and actual heavy
refresh/Stage9/compact/export, and the 2026/2027 coverage boundary.

Independent read-only review must cover the full final diff and exact published
head. Final test counts, CI, review, merge SHA, applied SHA and actual new runtime
snapshot coverage are recorded in the PR after the respective gates occur.
No runtime success or future calendar extension is claimed by this document.

Pre-publication results: 46 new/Stage9 checks passed; 67 compact-package
regressions passed; after correcting readiness scope, 118 readiness/observed
range/path-contract checks passed. Preliminary independent read-only review
found and then cleared two P2 findings (malformed USD evidence isolation and
final delivery TTL). The full diagnostic run was stopped at 4408 passed and
20 failures: 18 exposed the corrected readiness inventory issue; two depended
on a checkout outside /tmp and pass there without any test/code relaxation.
The earlier Windows archive also changed immutable grant line endings; all
subsequent checks use a direct GitHub clone with the original grant SHA.
Complete compileall/pytest CI of the published head remains mandatory before
merge, followed by independent review of that exact head and server acceptance.
