from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from moex_data.futures.freeze_accepted_futoi_history import freeze_accepted_history
from moex_data.futures.materialize_futoi_eod import materialize_eod_history
from moex_data.futures.materialize_futoi_positioning_features_d1 import materialize_features
from moex_data.futures.step5_futoi_source_quality import expected_derived_rows, omission_records

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
RUN_SUBPATH: Final[tuple[str, ...]] = ("runs", "step5_futoi_positioning")
HISTORY: Final[dict[str, tuple[str, str, int]]] = {
    "si_futures_family": ("2020-01-03", "2026-08-17", 1757),
    "cr_futures_family": ("2022-04-21", "2026-08-17", 1177),
}


class Step5PilotError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step5PilotError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.is_file():
        _fail("env_file does not exist")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    root = Path(value)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return root.resolve()


def _atomic_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp = Path(handle.name)
    temp.replace(path)


def run_pilot(*, artifact_version: str, env_file: str | None = CANONICAL_ENV_PATH) -> dict[str, object]:
    load_env_file(env_file)
    run_id = _safe_token(artifact_version, "artifact_version")
    root = _data_root()
    run_root = root.joinpath(*RUN_SUBPATH, "run_id=" + run_id)
    evidence_dir = root / "state" / "acceptance" / "step5_futoi_positioning" / ("run_id=" + run_id)
    if run_root.exists() or evidence_dir.exists():
        _fail("immutable Stage 5 run_id already exists")

    frozen_inputs: list[dict[str, object]] = []
    eod_outputs: list[dict[str, object]] = []
    feature_outputs: list[dict[str, object]] = []
    for instrument_id, (start_date, end_date, expected_partitions) in HISTORY.items():
        expected_eod_rows = expected_derived_rows(
            instrument_id,
            expected_partitions,
            start_date=start_date,
            end_date=end_date,
        )
        expected_omissions = omission_records(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
        )

        freeze_run = run_id + "_" + instrument_id + "_raw_freeze"
        frozen = freeze_accepted_history(
            data_root=root,
            output_root=run_root,
            repo_root=Path.cwd(),
            instrument_id=instrument_id,
            start_date=start_date,
            end_date=end_date,
            run_id=freeze_run,
        )
        if int(frozen["partition_count"]) != expected_partitions or frozen["physical_validation_status"] != "pass":
            _fail("Stage 5 frozen raw partition count/quality mismatch for " + instrument_id)
        frozen_inputs.append(frozen)

        eod_run = run_id + "_" + instrument_id + "_eod"
        eod = materialize_eod_history(
            data_root=root,
            output_root=run_root,
            frozen_input_manifest=str(frozen["manifest_path"]),
            instrument_id=instrument_id,
            start_date=start_date,
            end_date=end_date,
            run_id=eod_run,
        )
        if int(eod["input_partition_count"]) != expected_partitions or int(eod["row_count"]) != expected_eod_rows:
            _fail("Stage 5 historical frozen-input/EOD count mismatch for " + instrument_id)
        if eod.get("source_quality_omissions") != expected_omissions:
            _fail("Stage 5 EOD source-quality omission evidence mismatch for " + instrument_id)
        if eod.get("canonical_raw_partition_reads_used") is not False:
            _fail("Stage 5 EOD must not read mutable canonical raw partitions")
        eod_outputs.append(eod)

        feature_run = run_id + "_" + instrument_id + "_features"
        features = materialize_features(
            eod_partition=str(eod["partition_path"]),
            output_root=run_root,
            instrument_id=instrument_id,
            run_id=feature_run,
        )
        if int(features["row_count"]) != expected_eod_rows:
            _fail("Stage 5 feature row count mismatch for " + instrument_id)
        feature_outputs.append(features)

    evidence: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 5,
        "status": "pilot_passed",
        "artifact_version": run_id,
        "run_id": run_id,
        "run_root": run_root.as_posix(),
        "source_data_root": root.as_posix(),
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "raw_ingestion_changed": False,
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "canonical_raw_partition_reads_after_freeze_used": False,
        "immutable_raw_input_freeze_used": True,
        "raw_input_freeze_mode": "create_only_hardlink_same_validated_inode",
        "root_aggregate_semantics": True,
        "front_next_split_claimed": False,
        "historical_pit_research_ready_claimed": False,
        "revision_policy": "same_analytical_key_single_sess_id_then_max_seqnum",
        "snapshot_policy": "latest_resolved_complete_balanced_FIZ_YUR_event_ts",
        "source_quality_omission_policy": "explicit_attested_date_only_fail_closed_otherwise",
        "counts": {
            "mandatory_instruments": 2,
            "frozen_raw_inputs": len(frozen_inputs),
            "eod_outputs": len(eod_outputs),
            "feature_outputs": len(feature_outputs),
            "source_quality_omission_count": sum(
                len(omission_records(instrument_id, start_date=values[0], end_date=values[1]))
                for instrument_id, values in HISTORY.items()
            ),
            "expected_accepted_pointers": 4,
        },
        "histories": {
            instrument_id: {
                "start_date": values[0],
                "end_date": values[1],
                "expected_raw_partitions": values[2],
                "expected_eod_rows": expected_derived_rows(
                    instrument_id,
                    values[2],
                    start_date=values[0],
                    end_date=values[1],
                ),
                "source_quality_omissions": omission_records(
                    instrument_id,
                    start_date=values[0],
                    end_date=values[1],
                ),
            }
            for instrument_id, values in HISTORY.items()
        },
        "frozen_inputs": frozen_inputs,
        "eod_outputs": eod_outputs,
        "feature_outputs": feature_outputs,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    evidence_path = evidence_dir / "pilot_evidence.json"
    _atomic_json(evidence_path, evidence)
    evidence["evidence_path"] = evidence_path.as_posix()
    return evidence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run immutable Stage 5 Si/CR FUTOI raw-freeze, EOD and positioning-feature pilot.")
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_pilot(artifact_version=args.artifact_version, env_file=args.env_file)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 5, "status": "pilot_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
