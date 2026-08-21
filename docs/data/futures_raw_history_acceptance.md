# Futures Raw History Acceptance

## Purpose

This gate validates already materialized canonical Stage 2 raw histories before any accepted dataset pointer is created.

It does not fetch MOEX data, rerun historical backfills, discover partitions dynamically, select latest files, or write `current_accepted_manifest.json`.

A pre-existing canonical accepted pointer is a hard failure. Pointer promotion is a separate later step.

## Canonical implementation

Contract: `contracts/datasets/futures_raw_history_acceptance.v1.yaml`

Canonical gate runner: `src/moex_data/futures/stage2_raw_history_acceptance_gate.py`

Physical-history validator: `src/moex_data/futures/stage2_raw_history_acceptance.py`

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

Before reading history, the canonical gate expands the target dataset's explicit `accepted_pointer_path_contract` and requires that pointer to be absent.

Every existing canonical partition in the bounded GitHub-declared coverage range is read from its exact contract-expanded path.

Acceptance requires both declared totals and exact date-set identity. `configs/datasets/futures_data_lake.v1.yaml` pins SHA-256 for the complete present partition-date set and the complete missing-date set for each scoped history. Digests are computed from sorted ISO dates, one date per line with a trailing newline. Count-only acceptance is forbidden.

Quotes reuse the canonical raw 5-minute materializer validation semantics and additionally require:

- stored source identities to match the registry binding;
- trade/session/timestamp dates to match the partition date;
- timestamps to lie on the five-minute grid;
- finite OHLC/volume and finite nonnegative optional activity when present;
- nonnumeric stored optional activity to fail closed;
- `ingest_ts` to parse and not precede the source bar timestamp.

FUTOI requires:

- required canonical columns;
- explicit `instrument_id`, `source_id`, trade date, `secid`, and source ticker identity;
- canonical stored `clgroup` exactly `FIZ` or `YUR`;
- `ts = moment`;
- `systime >= moment` while `systime` remains publication/archive metadata only;
- `systime <= availability_ts_utc <= ingest_ts`;
- valid `sess_id` and `seqnum`;
- zero duplicates under `trade_date + sess_id + seqnum + secid + clgroup`;
- finite position fields and `pos = pos_long + pos_short`;
- zero null-required and invalid-position counts.

Final acceptance requires exact GitHub-declared partition count, row count, partition-date digest, and missing-date digest. FUTOI missing calendar counts must also equal the recorded source-empty count.

## Output

The runner writes one immutable acceptance report under:

`${MOEX_DATA_ROOT}/state/acceptance/target_dataset_id={TARGET_DATASET_ID}/instrument_id={INSTRUMENT_ID}/run_id={RUN_ID}/acceptance_report.json`

The report records expected and actual SHA-256 values for both present and missing date sets.

`acceptance_status=pass` is evidence for the separate accepted-pointer promotion step. This runner never writes that pointer itself.

## CLI

Use the implemented canonical gate interface only:

`python -m moex_data.futures.stage2_raw_history_acceptance_gate --target-dataset-id <futures_raw_5m|futures_futoi_raw> --instrument-id <explicit_instrument_id> --run-id <explicit_run_id>`

Optional `--env-file` may be supplied when the caller has an explicit environment-file path. Do not guess an environment-file path.

Run the four scoped identities separately so each acceptance report is independently auditable.

## Promotion boundary

After all required acceptance reports PASS, record the evidence in GitHub through a separate isolated change. Only that later step may authorize creation of per-instrument accepted manifests/pointers.

Scheduler, observed-source refresh, D1/W1 derivation, continuous Si/CR history, and research remain blocked during this gate.
