# Futures Raw History Acceptance

## Purpose

This gate validates already materialized canonical Stage 2 raw histories before any accepted dataset pointer is created.

It does not fetch MOEX data, rerun historical backfills, discover partitions dynamically, select latest files, or write `current_accepted_manifest.json`.

## Canonical implementation

Contract: `contracts/datasets/futures_raw_history_acceptance.v1.yaml`

Runner: `src/moex_data/futures/stage2_raw_history_acceptance.py`

Repository evidence and expected coverage: `configs/datasets/futures_data_lake.v1.yaml`

Storage paths are expanded only from the active target dataset contracts and `MOEX_DATA_ROOT`.

## Acceptance scope

Quotes historical core:

- `futures_raw_5m / usdrubf_futures_family`
- `futures_raw_5m / cnyrubf_futures_family`

FUTOI historical priority:

- `futures_futoi_raw / si_futures_family`
- `futures_futoi_raw / cr_futures_family`

Fixed current-expiry `Si` and `CR` quote contracts are reference-only and are not valid multi-year historical quote acceptance targets.

## Hard checks

Every existing canonical partition in the bounded GitHub-declared coverage range is read from its exact contract-expanded path.

Quotes reuse the canonical raw 5-minute materializer validation semantics and additionally reject negative `volume`, `value`, or `num_trades` when present.

FUTOI requires:

- required canonical columns;
- explicit `instrument_id`, `source_id`, trade date, and source ticker identity;
- `clgroup` in `FIZ/YUR`;
- `ts = moment`;
- `systime >= moment` while `systime` remains publication/archive metadata only;
- valid `sess_id` and `seqnum`;
- zero duplicates under `trade_date + sess_id + seqnum + secid + clgroup`;
- zero null-required and invalid-position counts.

Final acceptance also requires exact GitHub-declared partition and row totals. FUTOI missing calendar dates must equal the recorded source-empty count.

## Output

The runner writes one immutable acceptance report under:

`${MOEX_DATA_ROOT}/state/acceptance/target_dataset_id={TARGET_DATASET_ID}/instrument_id={INSTRUMENT_ID}/run_id={RUN_ID}/acceptance_report.json`

`acceptance_status=pass` is evidence for the separate accepted-pointer promotion step. This runner never writes that pointer itself.

## CLI

Use the implemented module interface only:

`python -m moex_data.futures.stage2_raw_history_acceptance --target-dataset-id <futures_raw_5m|futures_futoi_raw> --instrument-id <explicit_instrument_id> --run-id <explicit_run_id>`

Optional `--env-file` may be supplied when the caller has an explicit environment-file path. Do not guess an environment-file path.

Run the four scoped identities separately so each acceptance report is independently auditable.

## Promotion boundary

After all required acceptance reports PASS, record the evidence in GitHub through a separate isolated change. Only that later step may authorize creation of per-instrument accepted manifests/pointers.

Scheduler, observed-source refresh, D1/W1 derivation, continuous Si/CR history, and research remain blocked during this gate.
