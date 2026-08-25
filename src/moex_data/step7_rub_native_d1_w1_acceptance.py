from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

import pandas as pd
from pandas.testing import assert_series_equal

from moex_data.futures import stage2_raw_history_acceptance as stage2
from moex_data.step7_rub_native_d1_w1_materializer import (
    OHLCV_DATASET,
    TECH_DATASET,
    _validate_frozen_manifest,
    build_d1,
    build_technical_features,
    build_w1,
)

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
CONTRACT_ID: Final[str] = "step7_rub_native_d1_w1_technical_acceptance.v1"
HISTORY: Final[dict[str, tuple[str, str, int]]] = {
    "usdrubf_futures_family": ("2022-04-26", "2026-08-17", 1100),
    "cnyrubf_futures_family": ("2022-04-26", "2026-08-17", 1100),
}
EXPECTED_KEYS: Final[frozenset[tuple[str, str, str]]] = frozenset(
    (dataset_id, timeframe, instrument_id)
    for dataset_id in (OHLCV_DATASET, TECH_DATASET)
    for timeframe in ("1D", "1W")
    for instrument_id in HISTORY
)


class Step7AcceptanceError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step7AcceptanceError(message)


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
    path = Path(value)
    if not path.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return path.resolve()


def _load_json(path: Path, field: str) -> dict[str, object]:
    if not path.is_file():
        _fail(field + " missing")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step7AcceptanceError(field + " invalid JSON: " + str(exc)) from exc
    if not isinstance(value, dict):
        _fail(field + " must be object")
    return value


def _run_root(run_id: str) -> Path:
    return _data_root() / "runs" / "step7_rub_native_d1_w1" / ("run_id=" + _safe_token(run_id, "run_id"))


def _evidence_dir(run_id: str) -> Path:
    return _data_root() / "state" / "acceptance" / "step7_rub_native_d1_w1" / ("run_id=" + _safe_token(run_id, "run_id"))


def _inside_run(path_value: object, run_root: Path, field: str) -> Path:
    path = Path(str(path_value or ""))
    if not path.is_absolute():
        _fail(field + " must be absolute")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(run_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step7AcceptanceError(field + " must exist inside run root") from exc
    if not resolved.is_file():
        _fail(field + " must be a file")
    return resolved


def _revalidate_frozen(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str, validation_run_id: str) -> dict[str, object]:
    records, content_digest = _validate_frozen_manifest(data_root, manifest_path, instrument_id, start, end)
    expectation = stage2._expectation(repo_root, "futures_raw_5m", instrument_id)
    total_rows = 0
    for record in records:
        frame = pd.read_parquet(record["path"])
        rows, secids = stage2._validate_quote_partition(repo_root, frame, expectation, str(record["trade_date"]), validation_run_id)
        if int(rows) != int(record.get("row_count") or -1):
            _fail("frozen raw physical row_count mismatch")
        if list(secids) != list(record.get("secids") or []):
            _fail("frozen raw physical secid evidence mismatch")
        total_rows += int(rows)
    return {
        "partition_count": len(records),
        "row_count": total_rows,
        "content_sha256": content_digest,
        "physical_revalidation_passed": True,
    }


def _compare_frame(actual: pd.DataFrame, expected: pd.DataFrame, name: str) -> None:
    if len(actual.index) != len(expected.index):
        _fail(name + " row count mismatch")
    missing = [c for c in expected.columns if c not in actual.columns]
    if missing:
        _fail(name + " schema missing: " + ",".join(missing))
    for column in expected.columns:
        try:
            assert_series_equal(
                actual[column].reset_index(drop=True),
                expected[column].reset_index(drop=True),
                check_dtype=False,
                check_names=False,
                rtol=1e-10,
                atol=1e-12,
            )
        except AssertionError as exc:
            raise Step7AcceptanceError(name + " physical formula/source mismatch: " + column) from exc


def _validate_manifest_quality(record: Mapping[str, object], run_root: Path) -> tuple[Path, dict[str, object], dict[str, object]]:
    partition = _inside_run(record.get("partition_path"), run_root, "partition_path")
    manifest_path = _inside_run(record.get("manifest_path"), run_root, "manifest_path")
    quality_path = _inside_run(record.get("quality_report_path"), run_root, "quality_report_path")
    manifest = _load_json(manifest_path, "output manifest")
    quality = _load_json(quality_path, "output quality")
    dataset_id = str(record.get("dataset_id") or "")
    instrument_id = str(record.get("instrument_id") or "")
    timeframe = str(record.get("timeframe") or "")
    producer_run_id = str(record.get("run_id") or "")
    for values, name in ((manifest, "manifest"), (quality, "quality")):
        if values.get("dataset_id") != dataset_id or values.get("instrument_id") != instrument_id or values.get("timeframe") != timeframe:
            _fail(name + " output identity mismatch")
        if values.get("run_id") != producer_run_id or values.get("quality_status") != "pass":
            _fail(name + " run/quality mismatch")
        if int(values.get("row_count") or -1) != int(record.get("row_count") or -2):
            _fail(name + " row count mismatch")
    if Path(str(manifest.get("partition_path") or "")).resolve() != partition:
        _fail("manifest partition path mismatch")
    if Path(str(manifest.get("quality_report_path") or "")).resolve() != quality_path:
        _fail("manifest quality path mismatch")
    if manifest.get("network_calls_used") is not False or manifest.get("latest_autodetect_used") is not False or manifest.get("continuous_series_used") is not False:
        _fail("manifest execution boundary mismatch")
    return partition, manifest, quality


def validate_pilot(values: Mapping[str, object], *, run_id: str, repo_root: str | Path = ".") -> list[dict[str, object]]:
    checked_run = _safe_token(run_id, "run_id")
    if values.get("project") != "MOEX_Bot" or values.get("step") != 7 or values.get("status") != "pilot_passed":
        _fail("pilot identity/status mismatch")
    if values.get("artifact_version") != checked_run or values.get("run_id") != checked_run:
        _fail("pilot run identity mismatch")
    for field in ("run_id_reuse_allowed", "network_calls_used", "latest_autodetect_used", "continuous_series_used", "mutable_canonical_raw_read_after_freeze_allowed", "si_cr_continuous_ready", "weekly_oi_ready", "advanced_technical_policy_ready", "research_ready"):
        if values.get(field) is not False:
            _fail(field + " must be false")
    if values.get("run_artifacts_immutable") is not True or values.get("accepted_raw_history_required") is not True:
        _fail("pilot immutable/accepted-history semantics mismatch")
    run_root = _run_root(checked_run)
    if not run_root.is_dir() or Path(str(values.get("run_root") or "")).resolve() != run_root:
        _fail("pilot run_root mismatch")
    repo = Path(repo_root).resolve()
    data_root = _data_root()

    frozen_rows = values.get("frozen_inputs")
    if not isinstance(frozen_rows, list) or len(frozen_rows) != 2:
        _fail("pilot must have two frozen input manifests")
    frozen_by_instrument: dict[str, dict[str, object]] = {}
    for row in frozen_rows:
        if not isinstance(row, dict):
            _fail("frozen input evidence must be object")
        instrument_id = _safe_token(row.get("instrument_id"), "instrument_id")
        if instrument_id not in HISTORY or instrument_id in frozen_by_instrument:
            _fail("frozen input instrument set mismatch")
        start, end, expected_rows = HISTORY[instrument_id]
        manifest_path = _inside_run(row.get("manifest_path"), run_root, "frozen manifest_path")
        physical = _revalidate_frozen(
            repo_root=repo,
            data_root=data_root,
            manifest_path=manifest_path,
            instrument_id=instrument_id,
            start=start,
            end=end,
            validation_run_id=checked_run + "_acceptance_frozen_revalidation",
        )
        if int(physical["partition_count"]) != expected_rows:
            _fail("frozen physical partition count mismatch")
        frozen_by_instrument[instrument_id] = {"manifest_path": manifest_path, "physical": physical}
    if set(frozen_by_instrument) != set(HISTORY):
        _fail("frozen instrument set incomplete")

    output_rows = values.get("outputs")
    if not isinstance(output_rows, list) or len(output_rows) != 8:
        _fail("pilot must have eight output records")
    output_by_key: dict[tuple[str, str, str], dict[str, object]] = {}
    for record in output_rows:
        if not isinstance(record, dict):
            _fail("output evidence must be object")
        key = (str(record.get("dataset_id") or ""), str(record.get("timeframe") or ""), str(record.get("instrument_id") or ""))
        if key not in EXPECTED_KEYS or key in output_by_key:
            _fail("unexpected/duplicate output key")
        partition, manifest, quality = _validate_manifest_quality(record, run_root)
        output_by_key[key] = {"record": record, "partition": partition, "manifest": manifest, "quality": quality}
    if set(output_by_key) != set(EXPECTED_KEYS):
        _fail("Stage 7 output key set mismatch")

    validated: list[dict[str, object]] = []
    for instrument_id, (start, end, expected_d1_rows) in HISTORY.items():
        frozen_manifest = frozen_by_instrument[instrument_id]["manifest_path"]
        expected_d1 = build_d1(data_root=data_root, frozen_manifest_path=frozen_manifest, instrument_id=instrument_id, history_start=start, history_end=end)
        d1_item = output_by_key[(OHLCV_DATASET, "1D", instrument_id)]
        actual_d1 = pd.read_parquet(d1_item["partition"])
        if len(actual_d1.index) != expected_d1_rows:
            _fail("D1 physical expected row count mismatch")
        _compare_frame(actual_d1, expected_d1, instrument_id + " D1")
        if Path(str(d1_item["manifest"].get("source_ref") or "")).resolve() != Path(frozen_manifest).resolve():
            _fail("D1 manifest frozen lineage mismatch")

        expected_w1 = build_w1(expected_d1, history_start=start, history_end=end)
        w1_item = output_by_key[(OHLCV_DATASET, "1W", instrument_id)]
        actual_w1 = pd.read_parquet(w1_item["partition"])
        _compare_frame(actual_w1, expected_w1, instrument_id + " W1")
        if Path(str(w1_item["manifest"].get("source_ref") or "")).resolve() != Path(d1_item["partition"]).resolve():
            _fail("W1 manifest D1 lineage mismatch")
        if bool((pd.to_datetime(actual_w1["week_end_date"]).dt.date > pd.Timestamp(end).date()).any()):
            _fail("W1 contains incomplete/future week")

        d1_tech_item = output_by_key[(TECH_DATASET, "1D", instrument_id)]
        expected_d1_tech = build_technical_features(expected_d1, source_ohlcv_run_id=str(d1_item["record"]["run_id"]))
        actual_d1_tech = pd.read_parquet(d1_tech_item["partition"])
        _compare_frame(actual_d1_tech, expected_d1_tech, instrument_id + " D1 technical")
        if Path(str(d1_tech_item["manifest"].get("source_ref") or "")).resolve() != Path(d1_item["partition"]).resolve():
            _fail("D1 technical source lineage mismatch")

        w1_tech_item = output_by_key[(TECH_DATASET, "1W", instrument_id)]
        expected_w1_tech = build_technical_features(expected_w1, source_ohlcv_run_id=str(w1_item["record"]["run_id"]))
        actual_w1_tech = pd.read_parquet(w1_tech_item["partition"])
        _compare_frame(actual_w1_tech, expected_w1_tech, instrument_id + " W1 technical")
        if Path(str(w1_tech_item["manifest"].get("source_ref") or "")).resolve() != Path(w1_item["partition"]).resolve():
            _fail("W1 technical source lineage mismatch")

        for key in ((OHLCV_DATASET, "1D", instrument_id), (OHLCV_DATASET, "1W", instrument_id), (TECH_DATASET, "1D", instrument_id), (TECH_DATASET, "1W", instrument_id)):
            item = output_by_key[key]
            validated.append({
                "dataset_id": key[0],
                "timeframe": key[1],
                "instrument_id": key[2],
                "producer_run_id": str(item["record"]["run_id"]),
                "partition": item["partition"],
                "manifest_path": _inside_run(item["record"]["manifest_path"], run_root, "manifest_path"),
                "quality_path": _inside_run(item["record"]["quality_report_path"], run_root, "quality_report_path"),
                "row_count": int(item["record"]["row_count"]),
                "physical_readback_passed": True,
            })
    if len(validated) != 8:
        _fail("validated output count mismatch")
    return validated


def _rooted_ref(path: Path) -> str:
    root = _data_root().resolve(strict=True)
    try:
        rel = path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise Step7AcceptanceError("accepted artifact must be inside MOEX_DATA_ROOT") from exc
    return "${MOEX_DATA_ROOT}/" + rel.as_posix()


def _pointer_path(dataset_id: str, timeframe: str, instrument_id: str) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + dataset_id) / ("timeframe=" + timeframe) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def _stage_json(path: Path, values: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        return Path(handle.name)


def _restore(path: Path, previous: bytes | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".rollback") as handle:
        handle.write(previous)
        staged = Path(handle.name)
    staged.replace(path)


def _transactional_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    paths = [path for path, _ in records]
    if len(paths) != len(set(paths)):
        _fail("transaction target paths must be unique")
    previous = {path: path.read_bytes() if path.exists() else None for path in paths}
    staged: list[tuple[Path, Path]] = []
    applied: list[Path] = []
    try:
        for final, values in records:
            staged.append((_stage_json(final, values), final))
        for source, final in staged:
            source.replace(final)
            applied.append(final)
    except Exception as exc:
        errors: list[str] = []
        for final in reversed(applied):
            try:
                _restore(final, previous[final])
            except Exception as rollback_exc:
                errors.append(str(rollback_exc))
        if errors:
            raise Step7AcceptanceError("promotion failed and rollback incomplete: " + ";".join(errors)) from exc
        raise Step7AcceptanceError("promotion transaction failed: " + str(exc)) from exc
    finally:
        for source, _ in staged:
            source.unlink(missing_ok=True)


def promote(*, run_id: str, repo_root: str | Path = ".") -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    evidence_path = _evidence_dir(checked_run) / "pilot_evidence.json"
    validated = validate_pilot(_load_json(evidence_path, "pilot_evidence"), run_id=checked_run, repo_root=repo_root)
    records: list[tuple[Path, Mapping[str, object]]] = []
    summaries: list[dict[str, object]] = []
    for item in validated:
        pointer_path = _pointer_path(str(item["dataset_id"]), str(item["timeframe"]), str(item["instrument_id"]))
        pointer_values = {
            "dataset_id": item["dataset_id"],
            "timeframe": item["timeframe"],
            "instrument_id": item["instrument_id"],
            "run_id": item["producer_run_id"],
            "acceptance_run_id": checked_run,
            "manifest_ref": _rooted_ref(item["manifest_path"]),
            "quality_report_ref": _rooted_ref(item["quality_path"]),
            "partition_ref": _rooted_ref(item["partition"]),
            "quality_status": "pass",
            "acceptance_contract_id": CONTRACT_ID,
            "continuous_series_used": False,
            "research_ready": False,
        }
        records.append((pointer_path, pointer_values))
        summaries.append({
            "dataset_id": item["dataset_id"],
            "timeframe": item["timeframe"],
            "instrument_id": item["instrument_id"],
            "run_id": item["producer_run_id"],
            "acceptance_run_id": checked_run,
            "row_count": item["row_count"],
            "pointer_path": pointer_path.as_posix(),
            "physical_readback_passed": True,
        })
    if len(summaries) != 8:
        _fail("accepted pointer count mismatch")
    marker = _evidence_dir(checked_run) / "accepted_pointers.json"
    result: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 7,
        "status": "accepted",
        "run_id": checked_run,
        "acceptance_contract_id": CONTRACT_ID,
        "accepted_pointer_count": 8,
        "expected_pointer_count": 8,
        "pointers": summaries,
        "promotion_semantics": "transactional_with_rollback",
        "physical_partition_readback_required": True,
        "frozen_raw_physical_revalidation_required": True,
        "d1_w1_formula_revalidation_required": True,
        "technical_formula_revalidation_required": True,
        "continuous_series_used": False,
        "si_cr_continuous_ready": False,
        "weekly_oi_ready": False,
        "advanced_technical_policy_ready": False,
        "research_ready": False,
    }
    records.append((marker, result))
    _transactional_replace(records)
    result["acceptance_evidence_path"] = marker.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Accept a passed Stage 7 native RUB D1/W1 technical pilot.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_env_file(args.env_file)
    try:
        result = promote(run_id=args.run_id, repo_root=args.repo_root)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 7, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
