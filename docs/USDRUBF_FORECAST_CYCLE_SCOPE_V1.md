# Forecast cycle implementation manifest

PROJECT=MOEX_Bot; Task ID `usdrubf_forecast_journal_v1`; PR #561.
Owner assignment 2026-09-30 transfers mutation ownership from Browser to one
Codex execution. Independent reviewer is read-only. No second journal.

Initial head: `5053af9adf392163e5cb93bbaf30f8c92236483a`.
Base: `8a7a21e322cd6085ea8d2fc58f3dfe845bd5118e`.

## Requirement / reuse / gap / file / verification

| Requirement | Reuse and gap | Exact mutation scope | Verification |
|---|---|---|---|
| B durable journal | Existing immutable store; parent fsync and retry acknowledgement missing | src/moex_research/intelligence/usdrubf_forecast_journal.py | syscall order, failure/retry, concurrency, existing journal tests |
| B causal scoring | Existing evaluator; start target already passed | src/moex_research/intelligence/usdrubf_forecast_evaluation.py | bullish/bearish, equality, gap, multiple targets, cancellation |
| B canonical integration | Existing consumer, serialization decoder, factual package | src/moex_research/consumers/usdrubf_forecast_cycle.py (new) | carriers, metadata mismatch, baseline binding, CLI |
| C risk | Existing Stage 8 supplied aggregates remain unchanged; add explicit scenario API | src/moex_data/step8_position_risk_state.py; contracts/datasets/position_risk_scenarios.v1.json (new) | Decimal long/short, tranches, gaps, missing costs, limits |
| D evidence | Existing Phase 06/06A/07 and S7.2 runners unchanged | src/moex_research/intelligence/usdrubf_forecast_research.py (new) | immutable artifact hashes, missing evidence, sample overlap |
| E observation | Existing journal/evaluator; missing bounded resumable runner | src/moex_research/runners/usdrubf_forecast_observation.py (new) | restart/idempotency, frozen source versions, distinct root counts |
| Interface/config/docs/tests | Existing CLI extended via downstream cycle | configs/research/usdrubf_forecast_cycle.v1.json (new); docs/USDRUBF_FORECAST_JOURNAL_V1.md; tests/unit/test_usdrubf_forecast_journal.py; tests/unit/test_usdrubf_forecast_cycle.py (new); this manifest | focused, compileall, full pytest, exact-head CI and independent full diff review |

Existing open PRs inspected at intake: #457 management profile; #403 snapshot
MCP adapter; #371 ingestion contracts; #369 FUTOI EOD; older registry and
unrelated strategy PRs. No planned file overlap with those scopes. The shared
consumer, serialization, collectors, registries, management sources and CI are
read-only reuse. Amend this manifest before any additional file mutation.

Verification is not power-loss hardware testing. Synthetic mechanics, real
research evidence, retrospective acceptance, forward outcomes, merge and exact
server apply are separate gates. Do not close #539 from this downstream work.
