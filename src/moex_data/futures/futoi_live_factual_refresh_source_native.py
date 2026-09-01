from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as materializer
from . import observed_tradestats_dates as observed_dates

PROJECT: Final[str] = "MOEX_Bot"
SCHEMA_VERSION: Final[str] = "futoi_live_factual_refresh_source_native.v1"
DATASET_ID: Final[str] = "futoi_live_factual_context"
SOURCE_ID: Final[str] = materializer.SOURCE_ID
SI_INSTRUMENT_ID: Final[str] = "si_futures_family"
CR_INSTRUMENT_ID: Final[str] = "cr_futures_family"
LIVE_INSTRUMENT_IDS: Final[tuple[str, ...]] = (SI_INSTRUMENT_ID, CR_INSTRUMENT_ID)
# Compatibility alias for legacy Si-only importers. New refresh calls must pass instrument_id explicitly.
INSTRUMENT_ID: Final[str] = SI_INSTRUMENT_ID
MARKET_TZ: Final[str] = "Europe/Moscow"
ROOT_REF_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
SOURCE_LOOKBACK_DAYS: Final[int] = 14
EXPLICIT_EMPTY_ERROR: Final[str] = "FUTOI APIM exact source returned no rows"


class FutoiSourceNativeRefreshError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiSourceNativeRefreshError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or any(
        marker in text for marker in ("/", "\\", "*", "?", "[", "]", "{", "}", "$(", "`")
    ):
        _fail(field + " must be an explicit safe token")
    return text


def _instrument_id(value: object) -> str:
    checked = _safe_token(value, "instrument_id")
    if checked not in LIVE_INSTRUMENT_IDS:
        _fail("instrument_id is not enabled for live factual FUTOI context")
    return checked


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise FutoiSourceNativeRefreshError(field + " must be YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        _fail(field + " must be canonical YYYY-MM-DD")
    return text


def _data_root() -> Path:
    raw = str(os.environ.get("MOEX_DATA_ROOT", ""))
    if not raw or raw != raw.strip():
        _fail("MOEX_DATA_ROOT is required without surrounding whitespace")
    root = Path(raw)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        _fail("MOEX_DATA_ROOT must be an existing absolute non-symlink directory")
    return root.resolve(strict=True)


def _rooted_ref(root: Path, path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        _fail("artifact reference must be a regular non-symlink file")
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiSourceNativeRefreshError("artifact escaped MOEX_DATA_ROOT") from exc
    return ROOT_REF_PREFIX + relative.as_posix()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, field: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        _fail(field + " must be a regular non-symlink JSON file")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiSourceNativeRefreshError(field + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must contain a JSON object")
    return values


def _current_path(root: Path, instrument_id: str) -> Path:
    checked_instrument = _instrument_id(instrument_id)
    return (
        root
        / "state"
        / "datasets"
        / ("dataset_id=" + DATASET_ID)
        / ("instrument_id=" + checked_instrument)
        / "current.json"
    )


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.is_symlink():
        _fail("factual current artifact must not be a symlink")
    serialized = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"
    ).encode("utf-8")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".tmp") as handle:
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _as_int(value: object, field: str) -> int:
    if value is None or isinstance(value, bool) or pd.isna(value):
        _fail(field + " must be a finite integer")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise FutoiSourceNativeRefreshError(field + " must be numeric") from exc
    if not pd.notna(number) or not float(number).is_integer():
        _fail(field + " must be a finite integer")
    return int(number)


def _market_timestamp_to_utc(value: object, field: str) -> pd.Timestamp:
    try:
        parsed = pd.Timestamp(value)
    except Exception as exc:
        raise FutoiSourceNativeRefreshError(field + " must be a valid timestamp") from exc
    if pd.isna(parsed):
        _fail(field + " must be a valid timestamp")
    try:
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize(MARKET_TZ)
        else:
            parsed = parsed.tz_convert(MARKET_TZ)
    except Exception as exc:
        raise FutoiSourceNativeRefreshError(field + " cannot be localized to " + MARKET_TZ) from exc
    return parsed.tz_convert("UTC")


def _resolved_group(frame: pd.DataFrame, group: str, ts: pd.Timestamp) -> pd.Series:
    rows = frame.loc[
        (frame["clgroup"].astype(str).str.upper() == group) & (frame["_parsed_ts"] == ts)
    ].copy()
    if rows.empty:
        _fail("latest aligned FUTOI snapshot is missing " + group)
    sessions = set(_as_int(value, group + ".sess_id") for value in rows["sess_id"].tolist())
    if len(sessions) != 1:
        _fail("latest aligned FUTOI snapshot has multiple sess_id values for " + group)
    seqnums = [_as_int(value, group + ".seqnum") for value in rows["seqnum"].tolist()]
    max_seq = max(seqnums)
    selected = rows.loc[[seq == max_seq for seq in seqnums]]
    if len(selected) != 1:
        _fail("latest aligned FUTOI snapshot has ambiguous max seqnum for " + group)
    return selected.iloc[0]


def latest_aligned_factual(
    frame: pd.DataFrame,
    *,
    expected_trade_date: str,
    expected_instrument_id: str,
    expected_source_ticker: str,
    expected_secid: str,
) -> dict[str, object]:
    required = {
        "trade_date",
        "ts",
        "systime",
        "availability_ts_utc",
        "ingest_ts",
        "sess_id",
        "seqnum",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "source_id",
        "instrument_id",
        "source_ticker",
        "secid",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        _fail("accepted FUTOI partition missing columns: " + ",".join(missing))
    if frame.empty:
        _fail("accepted FUTOI partition is empty")
    checked_instrument = _instrument_id(expected_instrument_id)
    dates = set(str(value) for value in frame["trade_date"].tolist())
    if dates != {expected_trade_date}:
        _fail("accepted FUTOI partition trade_date mismatch")
    sources = set(str(value) for value in frame["source_id"].tolist())
    if sources != {SOURCE_ID}:
        _fail("accepted FUTOI partition source_id mismatch")
    instruments = set(str(value) for value in frame["instrument_id"].tolist())
    if instruments != {checked_instrument}:
        _fail("accepted FUTOI partition instrument_id mismatch")
    tickers = set(str(value).strip().lower() for value in frame["source_ticker"].tolist())
    if tickers != {str(expected_source_ticker).strip().lower()}:
        _fail("accepted FUTOI partition source_ticker mismatch")
    secids = set(str(value).strip() for value in frame["secid"].tolist())
    if secids != {str(expected_secid).strip()}:
        _fail("accepted FUTOI partition secid mismatch")

    work = frame.copy()
    work["_parsed_ts"] = pd.to_datetime(work["ts"], errors="coerce")
    if bool(work["_parsed_ts"].isna().any()):
        _fail("accepted FUTOI partition contains invalid ts")
    groups_by_ts = work.groupby("_parsed_ts")["clgroup"].agg(
        lambda values: set(str(value).upper() for value in values)
    )
    aligned = [ts for ts, groups in groups_by_ts.items() if groups == {"FIZ", "YUR"}]
    if not aligned:
        _fail("accepted FUTOI partition has no exact aligned FIZ/YUR snapshot")
    selected_ts = max(aligned)
    fiz = _resolved_group(work, "FIZ", selected_ts)
    yur = _resolved_group(work, "YUR", selected_ts)

    fiz_sess_id = _as_int(fiz["sess_id"], "FIZ.sess_id")
    yur_sess_id = _as_int(yur["sess_id"], "YUR.sess_id")
    if fiz_sess_id != yur_sess_id:
        _fail("latest aligned FUTOI FIZ/YUR snapshot must share sess_id")
    if str(fiz["source_ticker"]).strip().lower() != str(yur["source_ticker"]).strip().lower():
        _fail("latest aligned FUTOI FIZ/YUR snapshot source_ticker mismatch")
    if str(fiz["secid"]) != str(yur["secid"]):
        _fail("latest aligned FUTOI FIZ/YUR snapshot secid mismatch")

    def side(row: pd.Series, label: str) -> dict[str, int]:
        long_value = _as_int(row["pos_long"], label + ".pos_long")
        short_signed = _as_int(row["pos_short"], label + ".pos_short")
        net_value = _as_int(row["pos"], label + ".pos")
        long_num = _as_int(row["pos_long_num"], label + ".pos_long_num")
        short_num = _as_int(row["pos_short_num"], label + ".pos_short_num")
        if long_value < 0 or short_signed > 0 or long_num < 0 or short_num < 0:
            _fail(label + " contains invalid position signs/counts")
        if net_value != long_value + short_signed:
            _fail(label + " net position identity failed")
        return {
            "long": long_value,
            "short": abs(short_signed),
            "net": net_value,
            "long_participants": long_num,
            "short_participants": short_num,
        }

    fiz_values = side(fiz, "FIZ")
    yur_values = side(yur, "YUR")
    if fiz_values["net"] + yur_values["net"] != 0:
        _fail("FIZ/YUR net positions do not balance to zero")
    total_long = fiz_values["long"] + yur_values["long"]
    total_short = fiz_values["short"] + yur_values["short"]
    if total_long != total_short:
        _fail("FIZ/YUR total long and short open interest do not balance")

    snapshot_utc = _market_timestamp_to_utc(selected_ts, "snapshot_ts")
    publication_utc = max(
        _market_timestamp_to_utc(fiz["systime"], "FIZ.systime"),
        _market_timestamp_to_utc(yur["systime"], "YUR.systime"),
    )
    availability = max(
        pd.to_datetime(
            [fiz["availability_ts_utc"], yur["availability_ts_utc"]],
            utc=True,
            errors="raise",
        )
    )
    ingest = max(
        pd.to_datetime([fiz["ingest_ts"], yur["ingest_ts"]], utc=True, errors="raise")
    )
    if availability < publication_utc:
        _fail("FUTOI availability timestamp precedes source publication timestamp")
    if ingest < availability:
        _fail("FUTOI ingest timestamp precedes availability timestamp")

    return {
        "trade_date": expected_trade_date,
        "snapshot_ts": snapshot_utc.isoformat(),
        "source_publication_time": publication_utc.isoformat(),
        "availability_ts_utc": availability.isoformat(),
        "ingest_ts_utc": ingest.isoformat(),
        "source_ticker": str(fiz["source_ticker"]),
        "secid": str(fiz["secid"]),
        "sess_id": fiz_sess_id,
        "fiz": fiz_values,
        "yur": yur_values,
        "total_open_interest": total_long,
        "short_semantics": "absolute_contract_count",
        "timestamp_semantics": "source_event_and_publication_localized_from_Europe/Moscow_to_UTC",
        "fiz_yur_alignment": (
            "latest_exact_shared_source_event_ts_and_sess_id_after_max_seqnum_revision_resolution"
        ),
    }


def _binding(instrument_id: str) -> dict[str, object]:
    checked_instrument = _instrument_id(instrument_id)
    binding = materializer._registry_binding(materializer.REGISTRY_PATH, checked_instrument)
    if binding.get("futoi.source_id") != SOURCE_ID:
        _fail("registry FUTOI source_id mismatch")
    if str(binding.get("futoi.ticker") or "").strip() == "":
        _fail("registry FUTOI ticker is missing")
    if str(binding.get("secid") or "").strip() == "":
        _fail("registry FUTOI secid is missing")
    return binding


def source_identity(instrument_id: str) -> dict[str, str]:
    binding = _binding(instrument_id)
    return {
        "instrument_id": str(binding["instrument_id"]),
        "source_id": SOURCE_ID,
        "source_ticker": str(binding["futoi.ticker"]),
        "secid": str(binding["secid"]),
    }


def _is_explicit_empty_source(exc: Exception) -> bool:
    return isinstance(exc, materializer.FutoiMaterializationError) and str(exc) == EXPLICIT_EMPTY_ERROR


def _probe_exact_date(
    binding: Mapping[str, object], candidate: date, *, timeout: float
) -> dict[str, object]:
    trade_date = candidate.isoformat()
    ticker = str(binding["futoi.ticker"])
    try:
        frame, source_url = materializer._fetch_exact(ticker, trade_date, timeout, None)
    except Exception as exc:
        if _is_explicit_empty_source(exc):
            return {
                "trade_date": trade_date,
                "status": "EMPTY_FUTOI_ON_OBSERVED_TRADE_DATE",
            }
        raise FutoiSourceNativeRefreshError(
            "FUTOI exact-date probe failed for " + trade_date + ": " + str(exc)
        ) from exc
    validated = materializer._validate_required_source_identifiers(frame)
    validated = materializer._validate_raw_source_rows(validated, trade_date, ticker)
    groups = set(validated["clgroup"].astype(str).str.upper().str.strip())
    if groups != {"FIZ", "YUR"}:
        _fail("FUTOI exact-date probe must contain exactly FIZ and YUR on " + trade_date)
    return {
        "trade_date": trade_date,
        "status": "FUTOI_DATA",
        "row_count": int(len(validated)),
        "source_url": source_url,
    }


def discover_latest_source_trade_date(
    through_date: str, *, instrument_id: str, timeout: float
) -> tuple[str, list[dict[str, object]]]:
    checked = _iso_date(through_date, "through_date")
    checked_instrument = _instrument_id(instrument_id)
    end = date.fromisoformat(checked)
    current_moscow_date = pd.Timestamp.now(tz=MARKET_TZ).date()
    if end >= current_moscow_date:
        _fail("through_date must be a completed Europe/Moscow calendar date")
    start = end - timedelta(days=SOURCE_LOOKBACK_DAYS - 1)
    try:
        raw_observed = observed_dates.observed_dates(
            start.isoformat(),
            end.isoformat(),
            instrument_id=checked_instrument,
            timeout=timeout,
        )
        authoritative_dates = observed_dates.normalize_observed_dates(
            raw_observed,
            start.isoformat(),
            end.isoformat(),
        )
    except Exception as exc:
        raise FutoiSourceNativeRefreshError(
            "FUTOI authoritative observed TradeStats date selection failed for "
            + checked_instrument
            + ": "
            + str(exc)
        ) from exc
    if not authoritative_dates:
        _fail("FUTOI observed TradeStats date selection returned no authoritative dates")
    target_trade_date = authoritative_dates[-1]
    observations: list[dict[str, object]] = [
        {
            "trade_date": value,
            "status": "OBSERVED_TRADESTATS_DATE",
            "date_authority_source_id": observed_dates.SOURCE_ID,
        }
        for value in authoritative_dates
    ]
    binding = _binding(checked_instrument)
    futoi_observation = _probe_exact_date(
        binding,
        date.fromisoformat(target_trade_date),
        timeout=timeout,
    )
    observations.append(futoi_observation)
    if futoi_observation["status"] != "FUTOI_DATA":
        _fail(
            "FUTOI exact source is empty on authoritative observed TradeStats date "
            + target_trade_date
            + " for "
            + checked_instrument
        )
    return target_trade_date, observations


def _materialize_target(
    root: Path,
    target_trade_date: str,
    run_id: str,
    *,
    instrument_id: str,
    timeout: float,
) -> tuple[Path, dict[str, object]]:
    checked_instrument = _instrument_id(instrument_id)
    identity = source_identity(checked_instrument)
    raw_run_id = run_id + "_raw_" + target_trade_date.replace("-", "")
    result = materializer.materialize_futoi_partition(
        trade_date=target_trade_date,
        instrument_id=checked_instrument,
        run_id=raw_run_id,
        timeout=timeout,
        require_enabled=False,
    )
    if (
        result.get("status") != "succeeded"
        or result.get("quality_status") != "pass"
        or result.get("trade_date") != target_trade_date
    ):
        _fail("canonical exact-date FUTOI materialization did not pass")
    if result.get("instrument_id") != checked_instrument:
        _fail("canonical FUTOI materialization instrument_id mismatch")
    if result.get("source_id") != SOURCE_ID:
        _fail("canonical FUTOI materialization source_id mismatch")
    if str(result.get("futoi_ticker") or "").strip().lower() != identity["source_ticker"].lower():
        _fail("canonical FUTOI materialization ticker mismatch")
    if str(result.get("secid") or "").strip() != identity["secid"]:
        _fail("canonical FUTOI materialization secid mismatch")
    partition_path = Path(str(result.get("storage_partition_path") or ""))
    quality_path = Path(str(result.get("quality_report_reference") or ""))
    manifest_path = Path(str(result.get("manifest_reference") or ""))
    expected_partition_sha = str(result.get("published_partition_sha256") or "").strip().lower()
    if len(expected_partition_sha) != 64 or _sha256_file(partition_path) != expected_partition_sha:
        _fail("materialized FUTOI partition SHA mismatch")
    quality = _load_json(quality_path, "FUTOI raw quality report")
    manifest = _load_json(manifest_path, "FUTOI raw refresh manifest")
    if quality.get("quality_status") != "pass" or int(quality.get("row_count") or 0) <= 0:
        _fail("FUTOI raw quality report is not pass")
    if quality.get("instrument_id") != checked_instrument:
        _fail("FUTOI raw quality report instrument_id mismatch")
    if str(quality.get("futoi_ticker") or "").strip().lower() != identity["source_ticker"].lower():
        _fail("FUTOI raw quality report ticker mismatch")
    if str(quality.get("secid") or "").strip() != identity["secid"]:
        _fail("FUTOI raw quality report secid mismatch")
    if manifest.get("refresh_status") != "succeeded":
        _fail("FUTOI raw refresh manifest is not succeeded")
    if manifest.get("publication_run_id") != raw_run_id:
        _fail("FUTOI raw refresh manifest publication run_id mismatch")
    if manifest.get("instrument_scope") != [checked_instrument]:
        _fail("FUTOI raw refresh manifest instrument scope mismatch")
    if manifest.get("published_partition_sha256") != expected_partition_sha:
        _fail("FUTOI raw refresh manifest partition SHA mismatch")
    return partition_path, {
        "accepted_state_kind": "source_native_exact_date_raw_quality_pass",
        "raw_partition_ref": _rooted_ref(root, partition_path),
        "raw_partition_sha256": expected_partition_sha,
        "raw_quality_report_ref": _rooted_ref(root, quality_path),
        "raw_quality_report_sha256": _sha256_file(quality_path),
        "raw_refresh_manifest_ref": _rooted_ref(root, manifest_path),
        "raw_refresh_manifest_sha256": _sha256_file(manifest_path),
        "source_contract_ref": materializer.SOURCE_CONTRACT_REF,
        "raw_contract_ref": materializer.RAW_CONTRACT_REF,
        "raw_producer": materializer.PRODUCER_ID,
    }


def run_refresh(
    *,
    through_date: str,
    instrument_id: str,
    run_id: str,
    timeout: float = 60.0,
) -> dict[str, object]:
    checked_through = _iso_date(through_date, "through_date")
    checked_instrument = _instrument_id(instrument_id)
    checked_run = _safe_token(run_id, "run_id")
    identity = source_identity(checked_instrument)
    target_trade_date, observations = discover_latest_source_trade_date(
        checked_through,
        instrument_id=checked_instrument,
        timeout=timeout,
    )
    root = _data_root()
    partition_path, provenance = _materialize_target(
        root,
        target_trade_date,
        checked_run,
        instrument_id=checked_instrument,
        timeout=timeout,
    )
    frame = pd.read_parquet(partition_path)
    factual = latest_aligned_factual(
        frame,
        expected_trade_date=target_trade_date,
        expected_instrument_id=checked_instrument,
        expected_source_ticker=identity["source_ticker"],
        expected_secid=identity["secid"],
    )
    completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": "PASS",
        "source_id": SOURCE_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "through_date": checked_through,
        "expected_latest_source_trade_date": target_trade_date,
        "data_as_of": factual["snapshot_ts"],
        "last_success_at": completed_at,
        "freshness": {
            "status": "FRESH",
            "policy": "bounded_observed_tradestats_dates_then_exact_futoi",
            "source_lookback_days": SOURCE_LOOKBACK_DAYS,
            "accepted_trade_date": factual["trade_date"],
            "trading_date_authority_source_id": observed_dates.SOURCE_ID,
            "weekday_weekend_inference": False,
            "calendar_dependency": False,
        },
        "source_date_observations": observations,
        "quality_status": "PASS",
        "acceptance_status": "PASS",
        "factual": factual,
        "provenance": provenance,
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_required": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
        "historical_pit_research_ready_claimed": False,
    }
    _atomic_json(_current_path(root, checked_instrument), payload)
    return payload


def _failed_instrument_result(instrument_id: str, exc: Exception) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": "FAILED",
        "source_id": SOURCE_ID,
        "instrument_id": instrument_id,
        "error_class": exc.__class__.__name__,
        "error": str(exc),
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }


def run_refresh_all(
    *, through_date: str, run_id: str, timeout: float = 60.0
) -> dict[str, object]:
    checked_through = _iso_date(through_date, "through_date")
    checked_run = _safe_token(run_id, "run_id")
    results: dict[str, object] = {}
    failed: list[str] = []
    for instrument_id in LIVE_INSTRUMENT_IDS:
        instrument_run_id = checked_run + "_" + instrument_id
        try:
            result = run_refresh(
                through_date=checked_through,
                instrument_id=instrument_id,
                run_id=instrument_run_id,
                timeout=timeout,
            )
        except Exception as exc:
            result = _failed_instrument_result(instrument_id, exc)
        results[instrument_id] = result
        if result.get("status") != "PASS":
            failed.append(instrument_id)
    if not failed:
        aggregate_status = "PASS"
    elif len(failed) == len(LIVE_INSTRUMENT_IDS):
        aggregate_status = "FAILED"
    else:
        aggregate_status = "PARTIAL_FAILURE"
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": aggregate_status,
        "run_id": checked_run,
        "through_date": checked_through,
        "instrument_ids": list(LIVE_INSTRUMENT_IDS),
        "instrument_results": results,
        "failed_instrument_ids": failed,
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh canonical Si/CR FUTOI factual-only live context using authoritative observed "
            "AlgoPack FO TradeStats dates followed by an exact-date FUTOI read."
        )
    )
    parser.add_argument("--through-date", required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--instrument-id", choices=LIVE_INSTRUMENT_IDS)
    selection.add_argument("--all-instruments", action="store_true")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        materializer.load_env_file(args.env_file)
        if args.all_instruments:
            result = run_refresh_all(
                through_date=args.through_date,
                run_id=args.run_id,
                timeout=args.timeout,
            )
            if result["status"] != "PASS":
                print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
                return 1
        else:
            result = run_refresh(
                through_date=args.through_date,
                instrument_id=args.instrument_id,
                run_id=args.run_id,
                timeout=args.timeout,
            )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "project": PROJECT,
                    "schema_version": SCHEMA_VERSION,
                    "status": "BLOCKED",
                    "error": str(exc),
                    "factual_authority": False,
                    "directional_authority": False,
                    "action_authority": False,
                    "standalone_buy_sell_authority": False,
                    "stage5_full_mode_ready": False,
                    "stage5_pointer_promotion_performed": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
