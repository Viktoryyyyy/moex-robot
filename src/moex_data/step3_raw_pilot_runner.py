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

from moex_data.currency import materialize_cets_tom_raw_5m as cets
from moex_data.futures import front_next_binding as binding
from moex_data.futures import materialize_forts_raw_5m_instrument as quotes
from moex_data.futures import materialize_open_interest_instrument as oi

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
RUNS_SUBPATH: Final[tuple[str, ...]] = ("runs", "step3_canonical_raw")
EXPECTED_BINDINGS: Final[int] = 4
EXPECTED_QUOTE_PARTITIONS: Final[int] = 4
EXPECTED_OI_PARTITIONS: Final[int] = 4
EXPECTED_TOM_PARTITIONS: Final[int] = 2


class Step3PilotError(ValueError):
    pass


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise Step3PilotError(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        raise Step3PilotError(field_name + " must be an explicit safe token")
    return text


def _require_date(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise Step3PilotError(field_name + " must be explicit YYYY-MM-DD") from exc


def _require_causal_dates(trade_date: object, as_of_date: object) -> tuple[str, str]:
    checked_trade_date = _require_date(trade_date, "trade_date")
    checked_as_of_date = _require_date(as_of_date, "as_of_date")
    if checked_trade_date != checked_as_of_date:
        raise Step3PilotError(
            "trade_date must equal as_of_date while front/next binding uses the unversioned current FORTS reference"
        )
    return checked_trade_date, checked_as_of_date


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        raise Step3PilotError("MOEX_DATA_ROOT is required")
    path = Path(value)
    if not path.is_absolute():
        raise Step3PilotError("MOEX_DATA_ROOT must be absolute")
    return path


def _run_root(canonical_root: Path, run_id: str) -> Path:
    return canonical_root.joinpath(*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id"))


def _run_root_ref(run_id: str) -> str:
    return "${MOEX_DATA_ROOT}/" + "/".join((*RUNS_SUBPATH, "run_id=" + _require_token(run_id, "run_id")))


def _reserve_run_root(canonical_root: Path, run_id: str) -> Path:
    run_root = _run_root(canonical_root, run_id)
    run_root.parent.mkdir(parents=True, exist_ok=True)
    try:
        run_root.mkdir(exist_ok=False)
    except FileExistsError as exc:
        raise Step3PilotError("Step 3 run_id is immutable and cannot be reused") from exc
    return run_root


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


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp_name = handle.name
    Path(temp_name).replace(path)


def _artifact(base: str, suffix: str) -> str:
    return _require_token(base + "_" + suffix, "artifact_version")


def run_pilot(*, trade_date: str, as_of_date: str, artifact_version: str, env_file: str = CANONICAL_ENV_PATH, timeout: float = 60.0) -> dict[str, object]:
    checked_trade_date, checked_as_of_date = _require_causal_dates(trade_date, as_of_date)
    base_artifact = _require_token(artifact_version, "artifact_version")
    quotes.load_env_file(env_file)
    if not str(os.environ.get("MOEX_API_KEY", "")).strip():
        raise Step3PilotError("MOEX_API_KEY is required")
    canonical_root = _data_root()

    reference_frame, reference_url, reference_observed_at_utc = binding.fetch_reference_frame(
        as_of_date=checked_as_of_date,
        timeout=timeout,
    )
    bindings = binding.bind_front_next(
        reference_frame,
        root="Si",
        as_of_date=checked_as_of_date,
        availability_ts_utc=reference_observed_at_utc,
    )
    bindings.extend(
        binding.bind_front_next(
            reference_frame,
            root="CR",
            as_of_date=checked_as_of_date,
            availability_ts_utc=reference_observed_at_utc,
        )
    )
    if len(bindings) != EXPECTED_BINDINGS:
        raise Step3PilotError("front-next binding count mismatch")

    run_root = _reserve_run_root(canonical_root, base_artifact)
    quote_results: list[dict[str, object]] = []
    oi_results: list[dict[str, object]] = []
    tom_results: list[dict[str, object]] = []
    with _materialization_root(run_root):
        for item in bindings:
            instrument_id = str(item["instrument_id"])
            secid = str(item["secid"])
            quote_result = quotes.materialize_instrument_partition(
                trade_date=checked_trade_date,
                instrument_id=instrument_id,
                secid=secid,
                artifact_version=_artifact(base_artifact, instrument_id + "_quote"),
                timeout=timeout,
            ).payload
            if quote_result.get("quality_status") != "pass" or int(quote_result.get("row_count") or 0) <= 0:
                raise Step3PilotError("quote pilot failed for " + instrument_id)
            quote_results.append(quote_result)

            oi_result = oi.materialize_open_interest_partition(
                trade_date=checked_trade_date,
                instrument_id=instrument_id,
                secid=secid,
                artifact_version=_artifact(base_artifact, instrument_id + "_oi"),
                timeout=timeout,
            )
            if oi_result.get("quality_status") != "pass" or int(oi_result.get("row_count") or 0) <= 0:
                raise Step3PilotError("OI pilot failed for " + instrument_id)
            oi_results.append(oi_result)

        for instrument_id, secid in cets.INSTRUMENTS.items():
            result = cets.materialize_cets_tom_partition(
                trade_date=checked_trade_date,
                instrument_id=instrument_id,
                secid=secid,
                artifact_version=_artifact(base_artifact, instrument_id + "_quote"),
                timeout=timeout,
            )
            if result.get("quality_status") != "pass" or int(result.get("row_count") or 0) <= 0:
                raise Step3PilotError("TOM pilot failed for " + instrument_id)
            tom_results.append(result)

    if os.environ.get("MOEX_DATA_ROOT") != canonical_root.as_posix():
        raise Step3PilotError("canonical MOEX_DATA_ROOT was not restored after pilot materialization")
    if len(quote_results) != EXPECTED_QUOTE_PARTITIONS or len(oi_results) != EXPECTED_OI_PARTITIONS or len(tom_results) != EXPECTED_TOM_PARTITIONS:
        raise Step3PilotError("Step3 pilot output count mismatch")

    payload: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 3,
        "status": "pilot_passed",
        "trade_date": checked_trade_date,
        "as_of_date": checked_as_of_date,
        "artifact_version": base_artifact,
        "materialization_root": run_root.as_posix(),
        "materialization_root_ref": _run_root_ref(base_artifact),
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "reference_source_url": reference_url,
        "reference_observed_at_utc": reference_observed_at_utc,
        "bindings": bindings,
        "quote_partitions": quote_results,
        "open_interest_partitions": oi_results,
        "tom_partitions": tom_results,
        "counts": {
            "bindings": len(bindings),
            "quote_partitions": len(quote_results),
            "open_interest_partitions": len(oi_results),
            "tom_partitions": len(tom_results),
        },
        "latest_autodetect_used": False,
        "historical_backdating_used": False,
        "continuous_series_created": False,
    }
    evidence_path = canonical_root / "state" / "acceptance" / "step3_canonical_raw" / ("run_id=" + base_artifact) / "pilot_evidence.json"
    _write_json_atomic(evidence_path, payload)
    payload["evidence_path"] = evidence_path.as_posix()
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the controlled comprehensive Step 3 canonical raw pilot.")
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
        print(json.dumps({"project": "MOEX_Bot", "step": 3, "status": "pilot_failed", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
