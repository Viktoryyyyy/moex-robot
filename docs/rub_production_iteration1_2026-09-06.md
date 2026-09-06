# Production preparation: source acceptance, iteration 1

Training and new model evaluation remain paused by user instruction. Volume discrepancy investigation is excluded. No source collected in this iteration is automatically admitted to forecasting.

## Delivered scope

- A snapshot-bound source matrix distinguishes collection from acceptance and identifies mandatory, conditional and enrichment blocks. It is an inventory for the current production schema, not a universal readiness evaluator. Missing acceptance decisions remain false.
- Public factual snapshots expose RSS events as `UNKNOWN` / `NOT_ANALYZED`, with nullable confidence and relevance. Collector success does not imply neutral RUB impact. Original placeholder fields are retained under `upstream_placeholder`; event provenance is preserved. Shadow execution classification and authority are unchanged.
- Independent Brent and external CNY pilots are retained outside accepted data pointers.

The news snapshot mode changes from `LIVE_RSS_DETERMINISTIC_NEUTRAL` to `LIVE_RSS_UNANALYZED`. Consumers must tolerate null confidence/relevance and must not translate UNKNOWN to NEUTRAL.

## Evidence captured on 2026-09-06

Inventory input: `/home/trader/moex_bot/deploy_backups/production_iteration1_snapshot_20260906.json`, generated at 18:46:22 UTC, read at 18:49:32 UTC. SHA-256: `4e2584f006d62cfcb3fa86f6f3fb948a5ad456b28d3d218af2b2ba4c9da18a03`.

Matrix: `/home/trader/moex_bot/deploy_backups/production_source_matrix_20260906_v1.json`. The 17 required blocks without acceptance include freshness and governance restrictions; this count is not a count of technical outages. Market closure alone does not authorize upgrading stale data.

Pilot root: `/home/trader/moex_bot/data/runs/rub_external_pilot/iteration1_20260906`.

| Pilot | Coverage | Validation | Still required |
| --- | --- | --- | --- |
| MOEX BRV6 daily | 133 candles, 2026-03-02 through 2026-09-04 | Registry identity, pagination ending in empty response, unique dates, positive finite consistent OHLC, hashes | Quote units, calendar, contract roll policy, publication/availability semantics and accepted source policy |
| FRED DEXCHUS | 416 observations, 2025-01-02 through 2026-08-28 | Unique dates, positive finite observations, raw CSV and hash | Release availability and revisions, daily alignment, source acceptance |

DEXCHUS is a New York noon buying reference expressed in CNY per USD, not a live CNH quote. Source: https://fred.stlouisfed.org/series/DEXCHUS. Historical observation dates do not prove historical system availability. Backfill receipt times are retained; no point-in-time training eligibility is granted.

Pilot hashes:

- Brent pages: `b366cbb186414f02951b10ceccd7913984201b271878a2db12799e0f3cd3f79b`.
- Brent identity: `a9eaafa3a03f6f8db67090663a9f74331b0cb07efb75dd278827bd2c8da0f3d6`.
- DEXCHUS CSV: `082e848d474f0506193756087925ac89d288647cdcb87ce5f732c6b6ca163159`.

## Remaining gates in order

1. Prove freshness semantics for open and closed sessions; preserve the distinction between the last completed session and an actionable live quote. Accept CNY spot and FUTOI at consumer boundaries; resolve basis only with comparable synchronized inputs.
2. Admit Brent and external CNY only after units, calendars, availability and operational checks. WTI, Urals, DXY and UST remain enrichment candidates, not assumed present.
3. Accept official news and macro: resolve unproven publication timestamps, review deduplication, validate a sample of 100 events, then separate actual impact analysis from acquisition. Add Ministry of Finance operations, Rosstat and the event calendar.
4. Assemble a causal dataset with contract rolls and explicit as-of joins. Reconcile unpublished historical capture utilities before reproducible production use. Request separate authorization before resuming training or model evaluation.
5. Validate the forecasting model, deliver honest daily/weekly products, and observe production operation before declaring readiness.

This change does not complete iteration 1 or establish production forecast readiness.
