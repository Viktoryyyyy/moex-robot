# RUB history readiness: physical inventory, 2026-09-06

The forecast history is not ready. This audit inspects both accepted dataset
pointers and the separate legacy continuous storage; Stage9 pointers alone
do not describe all physical data. File presence does not establish acceptance,
session completeness, point-in-time suitability or five-year coverage.

## Reproducible inventory

Run without changing data or accepted pointers:

```sh
PYTHONPATH=src python -m moex_data.rub_history_inventory --data-root "$MOEX_DATA_ROOT" > history_inventory.json
```

The report includes SHA-256 checks for the partition, manifest and quality
report of single-partition accepted pointers, physical row/date ranges, and
content hashes for each Si/CR continuous file. Roll and adjustment policies
are kept separate. Concurrent partition/pointer replacement fails the affected
entry. The command exits nonzero for inventory errors, not for missing history.
An exit code of zero does not mean forecast readiness.

Server audit at 2026-09-06 16:14:16 UTC: 24 single-partition accepted pointers
verified, 5 other pointer formats explicitly not inspected, zero inventory
errors. Full source/calendar completeness was not assessed.

## Observed coverage

| Dataset | Instrument | Physical observations | Observed dates | Limitation |
|---|---|---:|---|---|
| Continuous 5m | Si | 25,529 rows / 278 files | 2025-04-29–2026-06-08 | Stale; per-session completeness unverified |
| Continuous 5m | CR | 61 rows / 1 file | 2026-05-19 | Single sample day |
| Continuous D1 | Si | 278 rows | 2025-04-29–2026-06-08 | Less than five years; acceptance unverified |
| Continuous D1 | CR | 1 row | 2026-05-19 | Single sample day |
| Continuous W1 | Si | 4 rows | Week starts 2026-05-18–2026-06-08 | Partial legacy inventory |
| Continuous W1 | CR | 0 files | — | Missing in the declared continuous storage path |
| Accepted native D1 | USDRUBF and CNYRUBF | 1,119 rows each | 2022-04-26–2026-09-05 | Different instruments from Si/CR |
| Accepted native W1 | USDRUBF and CNYRUBF | 226 rows each | Week starts 2022-05-02–2026-08-24 | Does not close Si/CR continuous weekly gap |
| Accepted FUTOI EOD | Si | 1,756 rows | 2020-01-03–2026-08-17 | Historical tail not updated by factual live refresh |
| Accepted FUTOI EOD | CR | 1,176 rows | 2022-04-21–2026-08-17 | Less than five years; historical tail remains |
| Accepted spot 5m | USD TOM | 26 rows | 2026-09-04 | Historical single-date pointer; absent from live schema |
| Accepted spot 5m | CNY TOM | 109 rows | 2026-09-04 | Historical single-date pointer; current live quote stale |

The USD TOM artifact declares SECID USD000UTSTOM and source
moex_iss_cets_tom_1m. Its presence is not a claim of current tradability or
continuous historical coverage. The H1 target remains unverified: no accepted
H1 pointer was found, and the inspected generic derived storage only held a
10m test sample. Raw family directories are likewise not the whole inventory.

## Next implementation boundary

An authenticated explicit-SECID source probe for SiU6 and CRU6, bounded to
2026-08-01–2026-09-05, found first available 2026-08-03 and last available
2026-09-05 for both. This verifies endpoints, not every intervening date.

1. Enumerate authoritative source dates and backfill immutable contract-level
   raw Si/CR bars with publication/ingestion evidence; compare each observed
   session against source rows before accepting a history manifest.
2. Extend and validate roll mappings across actual contracts; rebuild raw and
   continuous series separately. Current SiU6/CRU6 cannot stand in for older
   contracts or create five years of history.
3. Build H1/D1/W1 from accepted history with explicit completed-period and
   availability policies, then add the seven declared weekly feature blocks.
4. Accept historical FUTOI base+delta lineage separately from factual live
   acceptance. Do not enable Stage5 by changing a readiness flag.

The seven weekly gaps remain open. External context, position sizing and
predictive validation are separate requirements and are not granted by this
inventory.
