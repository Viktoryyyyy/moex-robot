# Bounded Si/CR contract history capture

The registry's Si/CR family entries describe current explicit expiring
contracts. They must not be used to backfill five years under one SECID.
This capture path is separate from automatic loading and model acceptance.
It allows a maximum of 60 completed calendar dates for a matching registered
Si/CR SECID with passed source pilot evidence, no later than the recorded last
trade date. It does not claim the contract was front throughout that window.

The canonical full-session AlgoPack producer writes into a newly reserved
`MOEX_DATA_ROOT/runs/rub_contract_history/run_id=...` directory. Reusing a run
ID is rejected. The run archives the registry binding, per-date results,
partitions, quality reports and manifests with SHA-256. No accepted pointer
is written and global loading/research flags are not changed. Run separate
processes for different instruments: the underlying producer temporarily
changes process-level configuration and is not thread safe.

```sh
PYTHONPATH=src python -m moex_data.rub_contract_history_capture \
  --data-root "$MOEX_DATA_ROOT" \
  --registry configs/instruments/forts_instrument_registry.v1.yaml \
  --env-file "$MOEX_ENV_FILE" --instrument-id si_futures_family --secid SiU6 \
  --start 2026-07-20 --end 2026-09-05 --run-id example_si_capture

PYTHONPATH=src python -m moex_data.rub_contract_history_verify \
  --run "$MOEX_DATA_ROOT/runs/rub_contract_history/run_id=example_si_capture"
```

The verifier requires at least 30 captured dates, validates every requested
calendar date is represented, verifies hashes and producer lineage, checks
row counts, contract identities, interval timestamps, duplicate bars, finite
positive OHLC and nonnegative volume. It reports intervals longer than five
minutes without treating session breaks as missing bars or fabricating prices.
Explicit empty source responses remain separately visible. Transport/schema
failures cannot be reclassified as an empty day.

`VERIFIED_CAPTURE_NOT_MODEL_ACCEPTANCE` confirms this bounded raw capture.
It does not grant intraday gap reconciliation, continuous roll readiness,
H1/weekly completeness, five-year history, point-in-time research validity
or trading authority. Later acceptance must resolve those independent gates.
