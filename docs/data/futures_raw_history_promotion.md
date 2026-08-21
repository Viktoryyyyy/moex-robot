# Stage 2 Raw History Promotion

## Purpose

This step promotes an explicit immutable `PASS` raw-history acceptance report into the canonical per-dataset/per-instrument accepted-pointer model.

It is separate from historical validation. It does not fetch market data, rerun backfills, discover partitions, or select files implicitly.

## Inputs

The operator must provide all three identities explicitly:

- target dataset: `futures_raw_5m` or `futures_futoi_raw`;
- instrument id;
- acceptance run id.

The promotion runner expands the acceptance-report path only from the repository acceptance contract and `MOEX_DATA_ROOT`.

## Hard preconditions

Promotion fails closed unless the acceptance report:

- is a regular JSON file at the exact contract-expanded path;
- belongs to the explicit target dataset, instrument, and acceptance run;
- identifies the canonical Stage 2 acceptance producer and contract;
- records the same canonical pointer path that promotion will write;
- has `acceptance_status=pass`;
- records `evidence_written=true`;
- records no pre-existing or written accepted pointer;
- records no network access, historical backfill, implicit partition discovery, or automatic date selection;
- has empty failed-partition and hard-failure lists;
- has exact expected/actual equality for partition count, row count, present-date digest, missing-date digest, and calendar-missing count;
- contains a sorted, duplicate-free missing-partition-date list matching the declared missing count;
- exactly matches the GitHub Source-of-Truth Stage 2 expectation for target dataset, instrument, source, secid scope, first/last date, partition count, row count, calendar-missing count, and both exact date-set SHA-256 values.

The report is parsed and SHA-256 hashed from the same bytes. The exact date-set digests are independently reconstructed from the declared range and missing-date list, then compared with the GitHub Source-of-Truth digests. Immediately before an immutable accepted manifest is published, the report file is rehashed and promotion fails without publishing the manifest if its bytes changed.

## Outputs

Immutable accepted manifest:

`${MOEX_DATA_ROOT}/state/accepted_manifests/target_dataset_id={TARGET_DATASET_ID}/instrument_id={INSTRUMENT_ID}/acceptance_run_id={ACCEPTANCE_RUN_ID}/accepted_manifest.json`

The manifest pins the target dataset contract, exact acceptance-report reference and SHA-256, range, counts, present-date digest, missing-date list, and missing-date digest. This allows later readers to resolve a pinned history without filesystem discovery.

Canonical pointer:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id={DATASET_ID}/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

The pointer keeps the established envelope fields `dataset_id`, `instrument_id`, `run_id`, `manifest_ref`, and `quality_report_ref`, and also records `acceptance_report_ref`. For history promotion, both evidence references point to the immutable raw-history acceptance report, `quality_status=pass`, and `promotion_basis=raw_history_acceptance`.

The pointer is create-only. A pre-existing pointer or a pointer that appears concurrently blocks promotion. The accepted manifest is immutable; an identical pre-created manifest may be reused only to recover from an interruption before pointer creation.

## CLI

Use the repository module path with the repository `src` directory on `PYTHONPATH` unless the package has been installed into the active environment:

`PYTHONPATH=src python -m moex_data.futures.stage2_raw_history_promotion --target-dataset-id <futures_raw_5m|futures_futoi_raw> --instrument-id <instrument_id> --acceptance-run-id <explicit_acceptance_run_id>`

Optional `--env-file` is allowed only when the environment-file path is explicit. Do not guess an environment-file path.

## Stage 2 promotion set

The four Step 1 acceptance reports are promoted independently:

- `futures_raw_5m / usdrubf_futures_family`
- `futures_raw_5m / cnyrubf_futures_family`
- `futures_futoi_raw / si_futures_family`
- `futures_futoi_raw / cr_futures_family`

All four canonical pointers must be successfully created before the Stage 2 raw-history promotion step is complete.
