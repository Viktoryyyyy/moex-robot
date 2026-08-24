from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

CONTRACT_ID: Final[str] = "step3_canonical_raw_acceptance.v1"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
EXPECTED_COUNTS: Final[dict[str, int]] = {
    "bindings": 4,
    "quote_partitions": 4,
    "open_interest_partitions": 4,
    "tom_partitions": 2,
}
EXPECTED_POINTER_COUNTS: Final[dict[str, int]] = {
    "futures_raw_5m": 4,
    "futures_open_interest_raw_5m": 4,
    "fx_spot_raw_5m": 2,
}


class Step3AcceptanceError(ValueError):
    pass


@dataclass(frozen=True)
class PointerSpec:
    dataset_id: str
    instrument_id: str
    source_id: str
    secid: str
    manifest_path: Path
    quality_path: Path
    partition_path: Path
    manifest_run_id: str


def _fail(message: str) -> None:
    raise Step3AcceptanceError(message)


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field_name + " must be an explicit safe token")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists() or not env_path.is_file():
        _fail("env_file does not exist")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
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
    return path


def _evidence_dir(run_id: str) -> Path:
    return _data_root() / "state" / "acceptance" / "step3_canonical_raw" / ("run_id=" + run_id)


def pilot_evidence_path(run_id: str) -> Path:
    return _evidence_dir(_require_token(run_id, "run_id")) / "pilot_evidence.json"


def acceptance_evidence_path(run_id: str) -> Path:
    return _evidence_dir(_require_token(run_id, "run_id")) / "accepted_pointers.json"


def _load_json(path: Path, field_name: str) -> Mapping[str, object]:
    if not path.exists() or not path.is_file():
        _fail(field_name + " does not exist")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step3AcceptanceError(field_name + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, Mapping):
        _fail(field_name + " must be a JSON object")
    return values


def _require_under_root(value: object, field_name: str) -> Path:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    candidate = Path(text)
    if not candidate.is_absolute():
        _fail(field_name + " must be an absolute producer output path")
    try:
        resolved = candidate.resolve(strict=True)
        root = _data_root().resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise Step3AcceptanceError(field_name + " must exist under MOEX_DATA_ROOT") from exc
    if not resolved.is_file():
        _fail(field_name + " must identify a regular file")
    return resolved


def _rooted_ref(path: Path) -> str:
    try:
        relative = path.resolve(strict=True).relative_to(_data_root().resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step3AcceptanceError("artifact path must be rooted at MOEX_DATA_ROOT") from exc
    return "${MOEX_DATA_ROOT}/" + relative.as_posix()


def _single_scope(value: object, field_name: str) -> str:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or len(value) != 1:
        _fail(field_name + " must contain exactly one value")
    return _require_token(value[0], field_name)


def _positive_row_count(values: Mapping[str, object], context: str) -> int:
    raw = values.get("row_count")
    if isinstance(raw, bool):
        _fail(context + " row_count must be a positive integer")
    try:
        count = int(raw)
    except (TypeError, ValueError) as exc:
        raise Step3AcceptanceError(context + " row_count must be a positive integer") from exc
    if count <= 0:
        _fail(context + " row_count must be positive")
    return count


def _manifest_run_id(path: Path) -> str:
    values = _load_json(path, "manifest")
    run_id = _require_token(values.get("run_id"), "manifest.run_id")
    status = values.get("refresh_status", values.get("status"))
    if status != "succeeded":
        _fail("manifest status must be succeeded")
    return run_id


def _quote_spec(values: Mapping[str, object]) -> PointerSpec:
    if values.get("quality_status") != "pass":
        _fail("quote quality_status must be pass")
    _positive_row_count(values, "quote")
    instrument_id = _single_scope(values.get("instrument_id_scope"), "quote.instrument_id_scope")
    secid = _single_scope(values.get("secid_scope"), "quote.secid_scope")
    source_id = _require_token(values.get("source_id"), "quote.source_id")
    manifest_path = _require_under_root(values.get("manifest_reference"), "quote.manifest_reference")
    quality_path = _require_under_root(values.get("quality_report_reference"), "quote.quality_report_reference")
    partition_path = _require_under_root(values.get("storage_partition_path"), "quote.storage_partition_path")
    return PointerSpec(
        dataset_id="futures_raw_5m",
        instrument_id=instrument_id,
        source_id=source_id,
        secid=secid,
        manifest_path=manifest_path,
        quality_path=quality_path,
        partition_path=partition_path,
        manifest_run_id=_manifest_run_id(manifest_path),
    )


def _supplementary_spec(values: Mapping[str, object], *, dataset_id: str, context: str) -> PointerSpec:
    if values.get("dataset_id") != dataset_id:
        _fail(context + " dataset_id mismatch")
    if values.get("quality_status") != "pass":
        _fail(context + " quality_status must be pass")
    _positive_row_count(values, context)
    instrument_id = _require_token(values.get("instrument_id"), context + ".instrument_id")
    secid = _require_token(values.get("secid"), context + ".secid")
    source_id = _require_token(values.get("source_id"), context + ".source_id")
    manifest_path = _require_under_root(values.get("manifest_path"), context + ".manifest_path")
    quality_path = _require_under_root(values.get("quality_report_path"), context + ".quality_report_path")
    partition_path = _require_under_root(values.get("partition_path"), context + ".partition_path")
    return PointerSpec(
        dataset_id=dataset_id,
        instrument_id=instrument_id,
        source_id=source_id,
        secid=secid,
        manifest_path=manifest_path,
        quality_path=quality_path,
        partition_path=partition_path,
        manifest_run_id=_manifest_run_id(manifest_path),
    )


def _require_list(value: object, field_name: str, expected_count: int) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        _fail(field_name + " must be a sequence")
    if len(value) != expected_count:
        _fail(field_name + " count mismatch")
    result: list[Mapping[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            _fail(field_name + " items must be objects")
        result.append(item)
    return tuple(result)


def validate_pilot_evidence(values: Mapping[str, object], *, run_id: str) -> tuple[PointerSpec, ...]:
    checked_run = _require_token(run_id, "run_id")
    if values.get("project") != "MOEX_Bot" or values.get("step") != 3:
        _fail("pilot evidence project/step mismatch")
    if values.get("status") != "pilot_passed":
        _fail("pilot evidence status must be pilot_passed")
    if values.get("artifact_version") != checked_run:
        _fail("pilot evidence artifact_version mismatch")
    if values.get("latest_autodetect_used") is not False:
        _fail("pilot evidence must prove latest_autodetect_used=false")
    if values.get("continuous_series_created") is not False:
        _fail("pilot evidence must prove continuous_series_created=false")
    counts = values.get("counts")
    if not isinstance(counts, Mapping):
        _fail("pilot evidence counts must be an object")
    for field_name, expected in EXPECTED_COUNTS.items():
        if counts.get(field_name) != expected:
            _fail("pilot evidence count mismatch: " + field_name)

    quote_rows = _require_list(values.get("quote_partitions"), "quote_partitions", EXPECTED_COUNTS["quote_partitions"])
    oi_rows = _require_list(values.get("open_interest_partitions"), "open_interest_partitions", EXPECTED_COUNTS["open_interest_partitions"])
    tom_rows = _require_list(values.get("tom_partitions"), "tom_partitions", EXPECTED_COUNTS["tom_partitions"])
    specs: list[PointerSpec] = [_quote_spec(item) for item in quote_rows]
    specs.extend(_supplementary_spec(item, dataset_id="futures_open_interest_raw_5m", context="open_interest") for item in oi_rows)
    specs.extend(_supplementary_spec(item, dataset_id="fx_spot_raw_5m", context="tom") for item in tom_rows)

    keys = {(spec.dataset_id, spec.instrument_id) for spec in specs}
    if len(keys) != sum(EXPECTED_POINTER_COUNTS.values()):
        _fail("accepted pointer dataset/instrument identities must be unique")
    for dataset_id, expected in EXPECTED_POINTER_COUNTS.items():
        if sum(1 for spec in specs if spec.dataset_id == dataset_id) != expected:
            _fail("accepted pointer count mismatch for " + dataset_id)
    return tuple(specs)


def _pointer_path(spec: PointerSpec) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + spec.dataset_id) / ("instrument_id=" + spec.instrument_id) / "current_accepted_manifest.json"


def _pointer_values(spec: PointerSpec, *, acceptance_run_id: str) -> dict[str, object]:
    return {
        "dataset_id": spec.dataset_id,
        "instrument_id": spec.instrument_id,
        "source_id": spec.source_id,
        "secid": spec.secid,
        "run_id": spec.manifest_run_id,
        "acceptance_run_id": acceptance_run_id,
        "manifest_ref": _rooted_ref(spec.manifest_path),
        "quality_report_ref": _rooted_ref(spec.quality_path),
        "partition_ref": _rooted_ref(spec.partition_path),
        "quality_status": "pass",
        "refresh_status": "succeeded",
        "acceptance_contract_id": CONTRACT_ID,
    }


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temp_name = handle.name
    Path(temp_name).replace(path)


def promote_step3_pilot(*, run_id: str) -> dict[str, object]:
    checked_run = _require_token(run_id, "run_id")
    evidence_path = pilot_evidence_path(checked_run)
    values = _load_json(evidence_path, "pilot_evidence")
    specs = validate_pilot_evidence(values, run_id=checked_run)

    pointer_records: list[dict[str, object]] = []
    for spec in specs:
        path = _pointer_path(spec)
        pointer_values = _pointer_values(spec, acceptance_run_id=checked_run)
        _write_json_atomic(path, pointer_values)
        pointer_records.append({
            "dataset_id": spec.dataset_id,
            "instrument_id": spec.instrument_id,
            "pointer_path": path.as_posix(),
            "pointer_ref": _rooted_ref(path),
            "manifest_ref": pointer_values["manifest_ref"],
            "quality_report_ref": pointer_values["quality_report_ref"],
        })

    result: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 3,
        "status": "accepted",
        "acceptance_contract_id": CONTRACT_ID,
        "run_id": checked_run,
        "pilot_evidence_ref": _rooted_ref(evidence_path),
        "accepted_pointer_count": len(pointer_records),
        "expected_pointer_count": sum(EXPECTED_POINTER_COUNTS.values()),
        "pointers": pointer_records,
        "latest_autodetect_used": False,
        "continuous_series_created": False,
    }
    if result["accepted_pointer_count"] != result["expected_pointer_count"]:
        _fail("accepted pointer total count mismatch")
    marker_path = acceptance_evidence_path(checked_run)
    _write_json_atomic(marker_path, result)
    result["acceptance_evidence_path"] = marker_path.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a fully passed Step 3 physical pilot to canonical accepted pointers.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        load_env_file(args.env_file)
        result = promote_step3_pilot(run_id=args.run_id)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 3, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
