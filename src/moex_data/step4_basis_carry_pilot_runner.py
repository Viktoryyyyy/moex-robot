from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Final

import pandas as pd

from moex_data.analytics import materialize_rub_basis_carry_5m as derived
from moex_data.currency import materialize_cets_tom_raw_5m as cets
from moex_data.futures import front_next_binding as binding
from moex_data.futures import materialize_forts_raw_5m_instrument as quotes

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
RUNS_SUBPATH: Final[tuple[str, ...]] = ("runs", "step4_rub_basis_carry")
FRONT_NEXT_MINIMUM_DAYS_TO_EXPIRY: Final[int] = 1
PERPETUALS: Final[dict[str, str]] = {
    "usdrubf_futures_family": "USDRUBF",
    "cnyrubf_futures_family": "CNYRUBF",
}


class Step4PilotError(ValueError):
    pass


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise Step4PilotError(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        raise Step4PilotError(field_name + " must be an explicit safe token")
    return text


def _require_date(value: object, field_name: str) -> str:
    try:
        return date.fromisoformat(str(value or "").strip()).isoformat()
    except ValueError as exc:
        raise Step4PilotError(field_name + " must be explicit YYYY-MM-DD") from exc


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        raise Step4PilotError("MOEX_DATA_ROOT is required")
    root = Path(value)
    if not root.is_absolute():
        raise Step4PilotError("MOEX_DATA_ROOT must be absolute")
    return root


def _run_root(canonical_root: Path, run_id: str) -> Path:
    return canonical_root.joinpath(*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id"))


def _run_root_ref(run_id: str) -> str:
    return "${MOEX_DATA_ROOT}/" + "/".join((*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id")))


def _reserve_run_root(canonical_root: Path, run_id: str) -> Path:
    root = _run_root(canonical_root, run_id)
    root.parent.mkdir(parents=True, exist_ok=True)
    try:
        root.mkdir(exist_ok=False)
    except FileExistsError as exc:
        raise Step4PilotError("Step 4 run_id is immutable and cannot be reused") from exc
    return root


@contextmanager
def _materialization_root(run_root: Path) -> Iterator[None]:
    previous = os.environ.get("MOEX_DATA_ROOT")
    os.environ["MOEX_DATA_ROOT"] = run_root.as_posix()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MOEX_DATA_ROOT", None)
        else:
            os.environ["MOEX_DATA_ROOT"] = previous


def _canonical_root_restored(canonical_root: Path) -> bool:
    restored = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not restored:
        return False
    restored_path = Path(restored)
    if not restored_path.is_absolute():
        return False
    try:
        return restored_path.resolve() == canonical_root.resolve()
    except OSError:
        return False


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        staged = Path(handle.name)
    staged.replace(path)


def _artifact(base: str, suffix: str) -> str:
    return _require_token(base + "_" + suffix, "artifact_version")


def _quote_frame(payload: Mapping[str, object]) -> pd.DataFrame:
    path = Path(str(payload.get("storage_partition_path") or ""))
    if not path.is_file():
        raise Step4PilotError("quote partition missing")
    return pd.read_parquet(path)


def _tom_frame(payload: Mapping[str, object]) -> pd.DataFrame:
    path = Path(str(payload.get("partition_path") or ""))
    if not path.is_file():
        raise Step4PilotError("TOM partition missing")
    return pd.read_parquet(path)


def _lineage(payload: Mapping[str, object]) -> dict[str, object]:
    return {
        "dataset_id": payload.get("dataset_id"),
        "instrument_id": (payload.get("instrument_id_scope") or [payload.get("instrument_id")])[0] if isinstance(payload.get("instrument_id_scope"), list) else payload.get("instrument_id"),
        "partition_path": payload.get("storage_partition_path", payload.get("partition_path")),
        "manifest_path": payload.get("manifest_reference", payload.get("manifest_path")),
        "quality_report_path": payload.get("quality_report_reference", payload.get("quality_report_path")),
        "quality_status": payload.get("quality_status"),
        "row_count": payload.get("row_count"),
    }


def run_pilot(*, trade_date: str, as_of_date: str, artifact_version: str, env_file: str = CANONICAL_ENV_PATH, timeout: float = 60.0) -> dict[str, object]:
    checked_trade_date = _require_date(trade_date, "trade_date")
    checked_as_of_date = _require_date(as_of_date, "as_of_date")
    if checked_trade_date != checked_as_of_date:
        raise Step4PilotError("trade_date must equal as_of_date for the unversioned current FORTS reference")
    base_artifact = _require_token(artifact_version, "artifact_version")
    quotes.load_env_file(env_file)
    if not str(os.environ.get("MOEX_API_KEY", "")).strip():
        raise Step4PilotError("MOEX_API_KEY is required")
    canonical_root = _data_root()

    reference_frame, reference_url, observed_at = binding.fetch_reference_frame(
        as_of_date=checked_as_of_date,
        timeout=timeout,
    )
    bindings = binding.bind_front_next(
        reference_frame,
        root="Si",
        as_of_date=checked_as_of_date,
        availability_ts_utc=observed_at,
        minimum_days_to_expiry=FRONT_NEXT_MINIMUM_DAYS_TO_EXPIRY,
    )
    bindings.extend(
        binding.bind_front_next(
            reference_frame,
            root="CR",
            as_of_date=checked_as_of_date,
            availability_ts_utc=observed_at,
            minimum_days_to_expiry=FRONT_NEXT_MINIMUM_DAYS_TO_EXPIRY,
        )
    )
    if len(bindings) != 4:
        raise Step4PilotError("front-next binding count mismatch")
    binding_by_instrument = {str(item["instrument_id"]): item for item in bindings}

    run_root = _reserve_run_root(canonical_root, base_artifact)
    quote_results: dict[str, dict[str, object]] = {}
    tom_results: dict[str, dict[str, object]] = {}
    output_results: list[dict[str, object]] = []

    with _materialization_root(run_root):
        for instrument_id, secid in PERPETUALS.items():
            payload = quotes.materialize_instrument_partition(
                trade_date=checked_trade_date,
                instrument_id=instrument_id,
                secid=secid,
                artifact_version=_artifact(base_artifact, instrument_id + "_quote"),
                timeout=timeout,
            ).payload
            if payload.get("quality_status") != "pass" or int(payload.get("row_count") or 0) <= 0:
                raise Step4PilotError("perpetual quote pilot failed for " + instrument_id)
            quote_results[instrument_id] = payload

        for item in bindings:
            instrument_id = str(item["instrument_id"])
            payload = quotes.materialize_instrument_partition(
                trade_date=checked_trade_date,
                instrument_id=instrument_id,
                secid=str(item["secid"]),
                artifact_version=_artifact(base_artifact, instrument_id + "_quote"),
                timeout=timeout,
            ).payload
            if payload.get("quality_status") != "pass" or int(payload.get("row_count") or 0) <= 0:
                raise Step4PilotError("front-next quote pilot failed for " + instrument_id)
            quote_results[instrument_id] = payload

        for instrument_id, secid in cets.INSTRUMENTS.items():
            payload = cets.materialize_cets_tom_partition(
                trade_date=checked_trade_date,
                instrument_id=instrument_id,
                secid=secid,
                artifact_version=_artifact(base_artifact, instrument_id + "_quote"),
                timeout=timeout,
            )
            if payload.get("quality_status") != "pass" or int(payload.get("row_count") or 0) <= 0:
                raise Step4PilotError("TOM pilot failed for " + instrument_id)
            tom_results[instrument_id] = payload

        for output_instrument_id, spec in derived.PAIR_SPECS.items():
            lineage = {
                "spot": _lineage(tom_results[spec.spot_instrument_id]),
                "perpetual": _lineage(quote_results[spec.perpetual_instrument_id]),
                "front": _lineage(quote_results[spec.front_instrument_id]),
                "next": _lineage(quote_results[spec.next_instrument_id]),
                "binding_reference_url": reference_url,
                "binding_reference_observed_at_utc": observed_at,
            }
            output = derived.materialize_pair_partition(
                instrument_id=output_instrument_id,
                trade_date=checked_trade_date,
                artifact_version=_artifact(base_artifact, output_instrument_id),
                spot_frame=_tom_frame(tom_results[spec.spot_instrument_id]),
                perpetual_frame=_quote_frame(quote_results[spec.perpetual_instrument_id]),
                front_frame=_quote_frame(quote_results[spec.front_instrument_id]),
                next_frame=_quote_frame(quote_results[spec.next_instrument_id]),
                front_binding=binding_by_instrument[spec.front_instrument_id],
                next_binding=binding_by_instrument[spec.next_instrument_id],
                input_lineage=lineage,
            )
            if output.get("quality_status") != "pass" or int(output.get("row_count") or 0) <= 0:
                raise Step4PilotError("derived output failed for " + output_instrument_id)
            output_results.append(output)

    if not _canonical_root_restored(canonical_root):
        raise Step4PilotError("canonical MOEX_DATA_ROOT was not restored")
    if len(quote_results) != 6 or len(tom_results) != 2 or len(output_results) != 2:
        raise Step4PilotError("Step 4 output count mismatch")

    payload: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 4,
        "status": "pilot_passed",
        "trade_date": checked_trade_date,
        "as_of_date": checked_as_of_date,
        "artifact_version": base_artifact,
        "materialization_root": run_root.as_posix(),
        "materialization_root_ref": _run_root_ref(base_artifact),
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "reference_source_url": reference_url,
        "reference_observed_at_utc": observed_at,
        "bindings": bindings,
        "front_next_minimum_days_to_expiry": FRONT_NEXT_MINIMUM_DAYS_TO_EXPIRY,
        "quote_partitions": list(quote_results.values()),
        "tom_partitions": list(tom_results.values()),
        "derived_partitions": output_results,
        "counts": {
            "bindings": len(bindings),
            "perpetual_quote_partitions": 2,
            "front_next_quote_partitions": 4,
            "tom_partitions": len(tom_results),
            "derived_partitions": len(output_results),
        },
        "alignment_policy": derived.ALIGNMENT_POLICY,
        "timestamp_policy": derived.TIMESTAMP_POLICY,
        "forward_fill_used": False,
        "asof_join_used": False,
        "latest_autodetect_used": False,
        "continuous_series_used": False,
    }
    evidence_path = canonical_root / "state" / "acceptance" / "step4_rub_basis_carry" / ("run_id=" + base_artifact) / "pilot_evidence.json"
    _write_json_atomic(evidence_path, payload)
    payload["evidence_path"] = evidence_path.as_posix()
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run controlled Stage 4 RUB basis-carry pilot.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--as-of", required=True, dest="as_of_date")
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = run_pilot(
            trade_date=args.trade_date,
            as_of_date=args.as_of_date,
            artifact_version=args.artifact_version,
            env_file=args.env_file,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 4, "status": "pilot_failed", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
