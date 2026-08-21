from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import pandas as pd

from . import accepted_manifest
from . import backfill_stage2_forts_raw_5m_instrument as quote_stage2_backfill
from . import materialize_forts_raw_5m_instrument as quote_materializer
from . import stage2_raw_history_acceptance as acceptance


QUOTE_TS_TIMEZONE = "Europe/Moscow"


def _accepted_pointer_ref(repo_root: Path, contract_path: str, instrument_id: str) -> str:
    path = repo_root / contract_path
    if not path.exists() or not path.is_file():
        raise acceptance.RawHistoryAcceptanceError("target dataset contract does not exist")
    values: list[str] = []
    prefix = "accepted_pointer_path_contract:"
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith(prefix):
            value = line[len(prefix) :].strip().strip('"').strip("'")
            if value:
                values.append(value)
    if len(values) != 1:
        raise acceptance.RawHistoryAcceptanceError(
            "target dataset contract must declare exactly one accepted_pointer_path_contract"
        )
    pointer_ref = values[0].replace("{INSTRUMENT_ID}", instrument_id)
    unresolved = pointer_ref.removeprefix("${MOEX_DATA_ROOT}/")
    if "{" in unresolved or "}" in unresolved:
        raise acceptance.RawHistoryAcceptanceError(
            "accepted pointer contract contains unresolved placeholders"
        )
    return pointer_ref


def _pointer_path(repo_root: Path, target_dataset_id: str, instrument_id: str) -> Path:
    if target_dataset_id == acceptance.QUOTE_DATASET_ID:
        contract_path = acceptance.QUOTE_CONTRACT_PATH
    elif target_dataset_id == acceptance.FUTOI_DATASET_ID:
        contract_path = acceptance.FUTOI_CONTRACT_PATH
    else:
        raise acceptance.RawHistoryAcceptanceError(
            "target_dataset_id is not part of Stage 2 raw history acceptance scope"
        )
    pointer_ref = _accepted_pointer_ref(repo_root, contract_path, instrument_id)
    try:
        return accepted_manifest._pointer_path(
            None, pointer_ref, target_dataset_id, instrument_id
        )
    except accepted_manifest.FuturesAcceptedManifestError as exc:
        raise acceptance.RawHistoryAcceptanceError(str(exc)) from exc


def _require_quote_registry_binding(
    repo_root: Path, instrument_id: str
) -> acceptance.HistoryExpectation:
    expectation = acceptance._expectation(
        repo_root, acceptance.QUOTE_DATASET_ID, instrument_id
    )
    if expectation.expected_secid is None:
        raise acceptance.RawHistoryAcceptanceError("quote expected secid is missing")
    if not quote_stage2_backfill._stage2_registry_allows(
        repo_root / quote_stage2_backfill.REGISTRY_PATH,
        instrument_id,
        expectation.expected_secid,
        expectation.source_id,
    ):
        raise acceptance.RawHistoryAcceptanceError(
            "repository quote secid evidence does not match registry binding"
        )
    return expectation


def _quote_grid_failures(
    repo_root: Path, expectation: acceptance.HistoryExpectation
) -> tuple[dict[str, str], ...]:
    pattern = acceptance._contract_path(repo_root, acceptance.QUOTE_DATASET_ID)
    failures: list[dict[str, str]] = []
    for trade_date in acceptance._date_range(expectation.date_start, expectation.date_end):
        path = acceptance._partition_path(
            repo_root=repo_root,
            pattern=pattern,
            expectation=expectation,
            trade_date=trade_date,
        )
        if not path.is_file():
            continue
        try:
            frame = pd.read_parquet(
                path, columns=["ts", "ingest_ts", "value", "num_trades"]
            )
            timestamps = pd.to_datetime(frame["ts"], errors="coerce")
            ingest_utc = pd.to_datetime(frame["ingest_ts"], errors="coerce", utc=True)
            if bool(timestamps.isna().any()) or bool(ingest_utc.isna().any()):
                failures.append(
                    {
                        "trade_date": trade_date,
                        "error": "quote partition contains invalid ts or ingest_ts",
                    }
                )
                continue
            try:
                timestamps_utc = timestamps.dt.tz_localize(
                    QUOTE_TS_TIMEZONE, ambiguous="raise", nonexistent="raise"
                ).dt.tz_convert("UTC")
            except Exception as exc:
                failures.append(
                    {
                        "trade_date": trade_date,
                        "error": "quote timestamp timezone normalization failed: " + str(exc),
                    }
                )
                continue
            if bool((ingest_utc < timestamps_utc).any()):
                failures.append(
                    {
                        "trade_date": trade_date,
                        "error": "quote partition ingest_ts precedes ts",
                    }
                )
                continue
            aligned = bool(timestamps.eq(timestamps.dt.floor("5min")).all())
            corrupt_optional = None
            for column in ("value", "num_trades"):
                raw = frame[column]
                numeric = pd.to_numeric(raw, errors="coerce")
                if bool((raw.notna() & numeric.isna()).any()):
                    corrupt_optional = column
                    break
        except Exception as exc:
            failures.append(
                {
                    "trade_date": trade_date,
                    "error": "quote 5-minute grid/activity validation failed: " + str(exc),
                }
            )
            continue
        if corrupt_optional is not None:
            failures.append(
                {
                    "trade_date": trade_date,
                    "error": "quote partition contains nonnumeric optional activity: "
                    + corrupt_optional,
                }
            )
            continue
        if not aligned:
            failures.append(
                {
                    "trade_date": trade_date,
                    "error": "quote partition ts is not aligned to 5-minute grid",
                }
            )
    return tuple(failures)


def _futoi_clgroup_failures(
    repo_root: Path, expectation: acceptance.HistoryExpectation
) -> tuple[dict[str, str], ...]:
    pattern = acceptance._contract_path(repo_root, acceptance.FUTOI_DATASET_ID)
    failures: list[dict[str, str]] = []
    for trade_date in acceptance._date_range(expectation.date_start, expectation.date_end):
        path = acceptance._partition_path(
            repo_root=repo_root,
            pattern=pattern,
            expectation=expectation,
            trade_date=trade_date,
        )
        if not path.is_file():
            continue
        try:
            frame = pd.read_parquet(path, columns=["clgroup"])
            groups = frame["clgroup"].astype("string")
            canonical = (
                not bool(groups.isna().any())
                and bool(groups.isin(("FIZ", "YUR")).all())
            )
        except Exception as exc:
            failures.append(
                {
                    "trade_date": trade_date,
                    "error": "FUTOI clgroup canonicalization validation failed: " + str(exc),
                }
            )
            continue
        if not canonical:
            failures.append(
                {
                    "trade_date": trade_date,
                    "error": "FUTOI partition clgroup must use canonical stored values FIZ or YUR",
                }
            )
    return tuple(failures)


def _apply_partition_failures(
    result: dict[str, object], failures: Sequence[dict[str, str]]
) -> None:
    if not failures:
        return
    failed_dates = list(result.get("failed_partition_dates") or [])
    failed_dates.extend(dict(item) for item in failures)
    hard_failures = list(result.get("hard_check_failures") or [])
    if "failed_partition_dates_nonempty" not in hard_failures:
        hard_failures.append("failed_partition_dates_nonempty")
    result["failed_partition_dates"] = failed_dates
    result["hard_check_failures"] = hard_failures
    result["acceptance_status"] = "fail"


def run_gate(
    *,
    repo_root: str | Path,
    target_dataset_id: str,
    instrument_id: str,
    run_id: str,
) -> dict[str, object]:
    root = Path(repo_root)
    checked_dataset = acceptance._require_token(target_dataset_id, "target_dataset_id")
    checked_instrument = acceptance._require_token(instrument_id, "instrument_id")
    checked_run_id = acceptance._require_token(run_id, "run_id")

    pointer = _pointer_path(root, checked_dataset, checked_instrument)
    if pointer.exists():
        raise acceptance.RawHistoryAcceptanceError(
            "preexisting canonical accepted pointer must be absent before raw history acceptance"
        )

    report_path = acceptance.acceptance_report_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        run_id=checked_run_id,
    )
    if report_path.exists():
        raise acceptance.RawHistoryAcceptanceError(
            "acceptance report already exists for explicit run_id"
        )

    expectation = None
    if checked_dataset == acceptance.QUOTE_DATASET_ID:
        expectation = _require_quote_registry_binding(root, checked_instrument)
    elif checked_dataset == acceptance.FUTOI_DATASET_ID:
        expectation = acceptance._expectation(root, checked_dataset, checked_instrument)

    result = acceptance.audit_history(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        run_id=checked_run_id,
    )
    if Path(str(result["acceptance_report_reference"])) != report_path:
        raise acceptance.RawHistoryAcceptanceError(
            "acceptance report path changed during deterministic gate evaluation"
        )

    if expectation is not None and result.get("acceptance_status") == "pass":
        if checked_dataset == acceptance.QUOTE_DATASET_ID:
            _apply_partition_failures(result, _quote_grid_failures(root, expectation))
        else:
            _apply_partition_failures(result, _futoi_clgroup_failures(root, expectation))

    if pointer.exists():
        raise acceptance.RawHistoryAcceptanceError(
            "canonical accepted pointer appeared during raw history acceptance"
        )

    result["preexisting_accepted_pointer_present"] = False
    result["accepted_pointer_path_checked"] = pointer.as_posix()
    result["evidence_written"] = True
    acceptance._write_json_immutable(report_path, result)
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Canonical fail-closed Stage 2 raw history acceptance gate."
    )
    parser.add_argument(
        "--target-dataset-id",
        required=True,
        choices=(acceptance.QUOTE_DATASET_ID, acceptance.FUTOI_DATASET_ID),
    )
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--env-file", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        quote_materializer.load_env_file(args.env_file)
        payload = run_gate(
            repo_root=args.repo_root,
            target_dataset_id=args.target_dataset_id,
            instrument_id=args.instrument_id,
            run_id=args.run_id,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "dataset_id": acceptance.ACCEPTANCE_DATASET_ID,
                    "error": str(exc),
                    "network_access_used": False,
                    "historical_backfill_used": False,
                    "accepted_pointer_written": False,
                    "evidence_written": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if payload["acceptance_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
