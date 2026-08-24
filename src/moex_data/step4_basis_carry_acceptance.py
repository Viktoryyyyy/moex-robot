from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

EXPECTED_INSTRUMENTS: Final[set[str]] = {"usd_rub_basis_carry", "cny_rub_basis_carry"}
DATASET_ID: Final[str] = "rub_basis_carry_5m"
CONTRACT_ID: Final[str] = "step4_rub_basis_carry_acceptance.v1"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
RUNS_SUBPATH: Final[tuple[str, ...]] = ("runs", "step4_rub_basis_carry")


class Step4AcceptanceError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step4AcceptanceError(message)


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field_name + " must be an explicit safe token")
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
    return root


def _run_root(run_id: str) -> Path:
    return _data_root().joinpath(*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id"))


def _evidence_dir(run_id: str) -> Path:
    return _data_root() / "state" / "acceptance" / "step4_rub_basis_carry" / ("run_id=" + _require_token(run_id, "run_id"))


def _load_json(path: Path, field_name: str) -> Mapping[str, object]:
    if not path.is_file():
        _fail(field_name + " does not exist")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step4AcceptanceError(field_name + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, Mapping):
        _fail(field_name + " must be a JSON object")
    return values


def _require_under_run_root(value: object, run_root: Path, field_name: str) -> Path:
    path = Path(str(value or ""))
    if not path.is_absolute():
        _fail(field_name + " must be absolute")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(run_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step4AcceptanceError(field_name + " must exist inside immutable run root") from exc
    if not resolved.is_file():
        _fail(field_name + " must be a regular file")
    return resolved


def _rooted_ref(path: Path) -> str:
    try:
        relative = path.resolve(strict=True).relative_to(_data_root().resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step4AcceptanceError("artifact must be inside MOEX_DATA_ROOT") from exc
    return "${MOEX_DATA_ROOT}/" + relative.as_posix()


def _same_path(value: object, expected: Path, field_name: str) -> None:
    raw = str(value or "").strip()
    if not raw:
        _fail(field_name + " is required")
    try:
        actual = Path(raw).resolve(strict=True)
        expected_resolved = expected.resolve(strict=True)
    except OSError as exc:
        raise Step4AcceptanceError(field_name + " must identify an existing artifact") from exc
    if actual != expected_resolved:
        _fail(field_name + " mismatch")


def validate_pilot(values: Mapping[str, object], *, run_id: str) -> list[dict[str, object]]:
    if values.get("project") != "MOEX_Bot" or values.get("step") != 4 or values.get("status") != "pilot_passed":
        _fail("pilot identity/status mismatch")
    if values.get("artifact_version") != run_id:
        _fail("pilot run_id mismatch")
    if values.get("run_artifacts_immutable") is not True or values.get("run_id_reuse_allowed") is not False:
        _fail("immutable run semantics not proven")
    if values.get("alignment_policy") != "exact_timestamp_inner_join":
        _fail("exact timestamp alignment not proven")
    if values.get("timestamp_policy") != "naive_exchange_localize_europe_moscow_then_utc":
        _fail("canonical timestamp policy not proven")
    for field in ("forward_fill_used", "asof_join_used", "latest_autodetect_used", "continuous_series_used"):
        if values.get(field) is not False:
            _fail(field + " must be false")
    counts = values.get("counts")
    if not isinstance(counts, Mapping):
        _fail("counts must be an object")
    expected_counts = {
        "bindings": 4,
        "perpetual_quote_partitions": 2,
        "front_next_quote_partitions": 4,
        "tom_partitions": 2,
        "derived_partitions": 2,
    }
    for field, expected in expected_counts.items():
        if counts.get(field) != expected:
            _fail("pilot count mismatch: " + field)

    run_root = _run_root(run_id)
    if not run_root.is_dir():
        _fail("immutable run root does not exist")
    if Path(str(values.get("materialization_root") or "")).resolve() != run_root.resolve():
        _fail("materialization_root mismatch")
    rows = values.get("derived_partitions")
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence) or len(rows) != 2:
        _fail("derived_partitions must contain exactly two outputs")
    result: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            _fail("derived output must be an object")
        instrument_id = _require_token(row.get("instrument_id"), "instrument_id")
        if instrument_id not in EXPECTED_INSTRUMENTS or instrument_id in seen:
            _fail("derived output identity mismatch")
        seen.add(instrument_id)
        if row.get("dataset_id") != DATASET_ID or row.get("quality_status") != "pass":
            _fail("derived output dataset/quality mismatch")
        try:
            row_count = int(row.get("row_count") or 0)
        except (TypeError, ValueError) as exc:
            raise Step4AcceptanceError("row_count must be positive") from exc
        if row_count <= 0:
            _fail("row_count must be positive")
        manifest_run_id = _require_token(row.get("run_id"), "derived.run_id")
        if row.get("alignment_policy") != "exact_timestamp_inner_join" or row.get("timestamp_policy") != "naive_exchange_localize_europe_moscow_then_utc" or row.get("forward_fill_used") is not False or row.get("asof_join_used") is not False or row.get("continuous_series_used") is not False:
            _fail("derived output causal/timestamp flags mismatch")
        partition = _require_under_run_root(row.get("partition_path"), run_root, "partition_path")
        manifest = _require_under_run_root(row.get("manifest_path"), run_root, "manifest_path")
        quality = _require_under_run_root(row.get("quality_report_path"), run_root, "quality_report_path")
        manifest_values = _load_json(manifest, "manifest")
        quality_values = _load_json(quality, "quality")
        if manifest_values.get("instrument_id") != instrument_id or manifest_values.get("row_count") != row_count or manifest_values.get("quality_status") != "pass" or manifest_values.get("run_id") != manifest_run_id:
            _fail("manifest identity/count/quality/run mismatch")
        if quality_values.get("instrument_id") != instrument_id or quality_values.get("row_count") != row_count or quality_values.get("quality_status") != "pass" or quality_values.get("run_id") != manifest_run_id:
            _fail("quality identity/count/status/run mismatch")
        if manifest_values.get("timestamp_policy") != "naive_exchange_localize_europe_moscow_then_utc" or quality_values.get("timestamp_policy") != "naive_exchange_localize_europe_moscow_then_utc":
            _fail("manifest/quality timestamp policy mismatch")
        _same_path(manifest_values.get("partition_path"), partition, "manifest.partition_path")
        _same_path(manifest_values.get("quality_report_path"), quality, "manifest.quality_report_path")
        result.append({
            "instrument_id": instrument_id,
            "row_count": row_count,
            "manifest_run_id": manifest_run_id,
            "partition": partition,
            "manifest": manifest,
            "quality": quality,
        })
    if seen != EXPECTED_INSTRUMENTS:
        _fail("derived output set mismatch")
    return result


def _pointer_path(instrument_id: str) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def _stage_json(path: Path, values: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        return Path(handle.name)


def _restore(path: Path, previous: bytes | None) -> None:
    if previous is None:
        if path.exists():
            path.unlink()
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
        for final, record_values in records:
            staged.append((_stage_json(final, record_values), final))
        for source, final in staged:
            source.replace(final)
            applied.append(final)
    except Exception as exc:
        rollback_errors: list[str] = []
        for final in reversed(applied):
            try:
                _restore(final, previous[final])
            except Exception as rollback_exc:
                rollback_errors.append(str(rollback_exc))
        if rollback_errors:
            raise Step4AcceptanceError("pointer promotion failed and rollback was incomplete: " + "; ".join(rollback_errors)) from exc
        raise Step4AcceptanceError("pointer promotion transaction failed: " + str(exc)) from exc
    finally:
        for source, _ in staged:
            if source.exists():
                try:
                    source.unlink()
                except OSError:
                    pass


def promote(*, run_id: str) -> dict[str, object]:
    checked_run = _require_token(run_id, "run_id")
    pilot_path = _evidence_dir(checked_run) / "pilot_evidence.json"
    outputs = validate_pilot(_load_json(pilot_path, "pilot_evidence"), run_id=checked_run)
    pointers: list[dict[str, object]] = []
    records: list[tuple[Path, Mapping[str, object]]] = []
    for output in outputs:
        instrument_id = str(output["instrument_id"])
        manifest_run_id = str(output["manifest_run_id"])
        pointer = _pointer_path(instrument_id)
        pointer_values = {
            "dataset_id": DATASET_ID,
            "instrument_id": instrument_id,
            "run_id": manifest_run_id,
            "acceptance_run_id": checked_run,
            "manifest_ref": _rooted_ref(output["manifest"]),
            "quality_report_ref": _rooted_ref(output["quality"]),
            "partition_ref": _rooted_ref(output["partition"]),
            "quality_status": "pass",
            "acceptance_contract_id": CONTRACT_ID,
        }
        records.append((pointer, pointer_values))
        pointers.append({
            "instrument_id": instrument_id,
            "run_id": manifest_run_id,
            "acceptance_run_id": checked_run,
            "pointer_path": pointer.as_posix(),
            "pointer_ref": "${MOEX_DATA_ROOT}/" + pointer.relative_to(_data_root()).as_posix(),
        })
    marker = _evidence_dir(checked_run) / "accepted_pointers.json"
    result: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 4,
        "status": "accepted",
        "run_id": checked_run,
        "acceptance_contract_id": CONTRACT_ID,
        "accepted_pointer_count": len(pointers),
        "expected_pointer_count": 2,
        "pointers": pointers,
        "promotion_semantics": "transactional_with_rollback",
        "continuous_series_used": False,
    }
    if result["accepted_pointer_count"] != result["expected_pointer_count"]:
        _fail("accepted pointer count mismatch")
    records.append((marker, result))
    _transactional_replace(records)
    result["acceptance_evidence_path"] = marker.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a passed Stage 4 basis-carry pilot.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        result = promote(run_id=args.run_id)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 4, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
