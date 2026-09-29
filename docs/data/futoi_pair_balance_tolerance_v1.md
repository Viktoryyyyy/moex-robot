# FUTOI pair balance tolerance v1

PROJECT=MOEX_Bot

- Task: futoi_pair_balance_tolerance_v1.
- Owner request, 2026-09-29: replace zero aggregate FUTOI net with an inclusive 1% contract-count tolerance.
- Repository base: 4fbf5bf81f9912652b152fdc25bf0536ae263b5c.
- Branch: codex/futoi-balance-tolerance-v1.

## Policy

For newly evaluated Si/CR root-v2 pairs, accept only when
`100 * abs(FIZ.net + YUR.net) <= max(total_long, total_absolute_short)`.
The denominator counts one side of contracts, avoiding double counting.
Comparison uses exact integers, including counts and source identifiers above
`2**53`; exactly 1% passes. Per-row signs and long-minus-short identity remain
exact. Original values are never rounded or adjusted. Existing
`total_open_interest = total_long` semantics remain unchanged.

The versioned contract is
`contracts/intelligence/futoi_pair_balance_tolerance_v1.json`.
Factual `balance_check` retains the policy, both side totals, denominator,
signed/absolute discrepancy and calculated percentage. Frozen provenance names
the same policy, and publication audit identifies its matching policy.
Readers verify these fields against the same frozen source values.

This owner decision supersedes the exact-zero requirement for new root-v2 pair
evaluation in the earlier live-date repair task. The reconstructed 2026-09-24
Si +6 example now passes this criterion without changing either net position.
It is synthetic regression evidence, not a production replay.

Legacy v1/EOD and old frozen v2 evidence without policy metadata retain exact-zero
replay. No old bytes, hashes, accepted pointers, Stage5 authority or trading
authority are rewritten. Latest-pair selection still precedes validation;
over-limit latest pairs retain refusal evidence without an older-pair fallback.
Current/previous and Si/CR remain independent. Clocks, TTL and the separate CR
current-only, dated and statistics admissions remain unchanged.

## Implementation scope

- New arithmetic helper and versioned policy contract.
- Existing source-native v2 producer, publication audit and delta/statistics reader.
- Existing Si/CR dated serializers and retained evidence checks.
- Existing snapshot FUTOI reader and factual/market-delivery projections.
- Two existing v2 dataset declarations.
- Existing live-date/identity regressions and new boundary/arithmetic tests.
- This task record.

There is no new collector, source, history backfill, HTTP/MCP transport change,
scheduler change or widening of governance grants.

## Validation and delivery

Targeted native refresh, frozen Parquet replay, boundary and compatibility tests:
`PYTHONPATH=.:src pytest -q tests/unit/test_futoi_pair_balance_tolerance.py tests/test_futoi_live_date_identity_repair_v1.py tests/test_futoi_live_identity_compatibility_v1.py tests/unit/test_futoi_publication_audit.py`.

The end-to-end tests enter real regular/fast refresh, preserve both role results,
write JSON, use the canonical reader and final market projection, and exercise
dated/statistics capture/replay and unchanged first economic acceptance.
Only external I/O, clocks and unrelated market acquisition are replaced.

Full CI commands: `python -m compileall -q src tests`;
`PYTHONPATH=.:src pytest -q`. Server-side validation uses an isolated full Git
checkout and the existing Python environment with a real Parquet engine; it
does not change production.

Exact published head, independent review, CI and merge/apply evidence are
recorded on the PR. Until those gates and any required authority are satisfied,
this document does not claim production activation.
