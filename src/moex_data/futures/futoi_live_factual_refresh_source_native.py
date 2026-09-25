from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from numbers import Integral, Real
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


def _freeze_artifact(root: Path, path: Path, expected_sha: str) -> Path:
    """Keep verified bytes independent of subsequent canonical partition refreshes."""
    _rooted_ref(root, path)
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected_sha:
        _fail("FUTOI evidence changed before archival: SHA mismatch")
    directory = root / "state" / "datasets" / ("dataset_id=" + DATASET_ID) / "evidence"
    if not directory.resolve().is_relative_to(root.resolve()):
        _fail("FUTOI evidence directory escaped MOEX_DATA_ROOT")
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / (expected_sha + path.suffix)
    with tempfile.NamedTemporaryFile("wb", dir=directory, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        # Publish atomically without overwriting an existing content-addressed object.
        try:
            os.link(temporary, destination)
        except FileExistsError:
            pass
        _rooted_ref(root, destination)
        if _sha256_file(destination) != expected_sha:
            _fail("archived FUTOI evidence SHA mismatch")
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _current_path(root: Path, instrument_id: str, *, raw_schema_version: str = "v1") -> Path:
    if _raw_version(raw_schema_version) == RAW_SCHEMA_V2:
        return _root_current_path(root, instrument_id)
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
    if value is None or isinstance(value, bool) or value.__class__.__name__ == "bool_" or pd.isna(value):
        _fail(field + " must be a finite integer")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise FutoiSourceNativeRefreshError(field + " must be numeric") from exc
    if not number.is_finite() or number != number.to_integral_value():
        _fail(field + " must be a finite integer")
    # A float at this boundary may already have rounded a distinct source ID.
    # Exact integer, string and Decimal inputs do not pass through binary float.
    if isinstance(value, Real) and not isinstance(value, Integral) and abs(number) >= 2**53:
        _fail(field + " has unsafe floating-point integer precision")
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
    expected_secid: str | None = None,
    raw_schema_version: str = "v1",
) -> dict[str, object]:
    if _raw_version(raw_schema_version) == RAW_SCHEMA_V2:
        return _latest_root_factual(frame, expected_trade_date=expected_trade_date,
            expected_instrument_id=expected_instrument_id, expected_source_ticker=expected_source_ticker,
            expected_secid=expected_secid)
    if "raw_schema_version" in frame and not frame["raw_schema_version"].eq("v1").all():
        _fail("v1 reader cannot admit another raw schema version")
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
    # Select the source frontier before checking its quality. Searching backwards
    # for a complete pair would silently replace an incomplete new publication.
    selected_ts = work["_parsed_ts"].max()
    if groups_by_ts.loc[selected_ts] != {"FIZ", "YUR"}:
        _fail("latest source FUTOI timestamp must contain exactly FIZ and YUR; fallback forbidden")
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


def source_identity(instrument_id: str, *, raw_schema_version: str = "v1") -> dict[str, str]:
    if _raw_version(raw_schema_version) == RAW_SCHEMA_V2:
        return _root_identity(instrument_id)
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
    raw_schema_version: str = "v1",
) -> tuple[Path, dict[str, object]]:
    if _raw_version(raw_schema_version) == RAW_SCHEMA_V2:
        return _materialize_root_target(root, target_trade_date, run_id,
            instrument_id=instrument_id, timeout=timeout)
    checked_instrument = _instrument_id(instrument_id)
    identity = source_identity(checked_instrument)
    raw_run_id = (
        run_id
        + "_"
        + checked_instrument
        + "_raw_"
        + target_trade_date.replace("-", "")
    )
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
    partition_path = _freeze_artifact(root, partition_path, expected_partition_sha)
    quality_path = _freeze_artifact(root, quality_path, _sha256_file(quality_path))
    manifest_path = _freeze_artifact(root, manifest_path, _sha256_file(manifest_path))
    quality = _load_json(quality_path, "FUTOI raw quality report")
    manifest = _load_json(manifest_path, "FUTOI raw refresh manifest")
    if quality.get("quality_status") != "pass" or int(quality.get("row_count") or 0) <= 0:
        _fail("FUTOI raw quality report is not pass")
    if quality.get("instrument_id") != checked_instrument:
        _fail("FUTOI raw quality report instrument_id mismatch")
    if quality.get("run_id") != raw_run_id or quality.get("trade_date") != target_trade_date:
        _fail("FUTOI raw quality report run/date mismatch")
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
    provenance = {
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
    if checked_instrument == CR_INSTRUMENT_ID:
        from .futoi_publication_audit import audited_latest
        audited_latest(root, pd.read_parquet(partition_path), provenance,
            expected_trade_date=target_trade_date, expected_instrument_id=checked_instrument,
            expected_source_ticker=identity["source_ticker"], expected_secid=identity["secid"])
    return partition_path, provenance


def run_refresh(
    *,
    through_date: str,
    instrument_id: str,
    run_id: str,
    timeout: float = 60.0,
    raw_schema_version: str = "v1",
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    if _raw_version(raw_schema_version) == RAW_SCHEMA_V2:
        return _run_root_refresh(through_date=through_date, instrument_id=instrument_id,
            run_id=run_id, timeout=timeout, now_fn=now_fn)
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
    current_path = _current_path(root, checked_instrument)
    # Preserve the complete run result as well as its source artifacts.
    serialized = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    current_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=current_path.parent, suffix=".json", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(serialized)
    try:
        archived = _freeze_artifact(root, temporary, hashlib.sha256(serialized).hexdigest())
    finally:
        temporary.unlink(missing_ok=True)
    payload["run_evidence_ref"] = _rooted_ref(root, archived)
    payload["run_evidence_sha256"] = _sha256_file(archived)
    _atomic_json(current_path, payload)
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
    *, through_date: str, run_id: str, timeout: float = 60.0,
    raw_schema_version: str = "v1",
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    version = _raw_version(raw_schema_version)
    version_args = {"raw_schema_version": version, "now_fn": now_fn} if version == RAW_SCHEMA_V2 else {}
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
                **version_args,
            )
        except Exception as exc:
            result = _failed_instrument_result(instrument_id, exc)
            if version == RAW_SCHEMA_V2:
                result = getattr(exc, "failure_payload", result)
                result.update(schema_version=SCHEMA_VERSION_V2, raw_schema_version=RAW_SCHEMA_V2,
                    source_identity_scope=ROOT_IDENTITY_SCOPE, source_ticker=ROOT_TICKERS[instrument_id])
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
        "schema_version": SCHEMA_VERSION_V2 if version == RAW_SCHEMA_V2 else SCHEMA_VERSION,
        **({"raw_schema_version": RAW_SCHEMA_V2, "source_identity_scope": ROOT_IDENTITY_SCOPE}
           if version == RAW_SCHEMA_V2 else {}),
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
    parser.add_argument("--raw-schema-version", choices=("v1", "v2"), default="v1")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    version_args = {"raw_schema_version": RAW_SCHEMA_V2} if args.raw_schema_version == RAW_SCHEMA_V2 else {}
    try:
        materializer.load_env_file(args.env_file)
        if args.all_instruments:
            result = run_refresh_all(
                through_date=args.through_date,
                run_id=args.run_id,
                timeout=args.timeout,
                **version_args,
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
                **version_args,
            )
    except Exception as exc:
        if args.raw_schema_version == RAW_SCHEMA_V2:
            failed = getattr(exc, "failure_payload", {
                "project": PROJECT, "schema_version": SCHEMA_VERSION_V2, "status": "BLOCKED",
                "raw_schema_version": RAW_SCHEMA_V2, "error_class": type(exc).__name__, "error": str(exc),
                "factual_authority": False, "directional_authority": False, "action_authority": False,
                "standalone_buy_sell_authority": False, "stage5_full_mode_ready": False,
                "stage5_pointer_promotion_performed": False,
            })
            print(json.dumps(failed, ensure_ascii=False, sort_keys=True))
            return 1
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


# V2 is opt-in. Legacy helpers and their historical return shape remain v1.
RAW_SCHEMA_V2: Final[str] = "v2"
SCHEMA_VERSION_V2: Final[str] = "futoi_live_factual_refresh_source_native.v2"
ROOT_IDENTITY_SCOPE: Final[str] = "source_ticker_root"
DATE_WITNESS_INSTRUMENT_ID: Final[str] = "usdrubf_futures_family"
DATE_WITNESS_SECID: Final[str] = "USDRUBF"
ROOT_TICKERS: Final[dict[str, str]] = {SI_INSTRUMENT_ID: "si", CR_INSTRUMENT_ID: "cr"}
ROOT_KEY_FIELDS: Final[tuple[str, ...]] = (
    "trade_date", "sess_id", "seqnum", "source_ticker", "clgroup",
)
ROOT_CONTRACT_REFS: Final[dict[str, str]] = {
    "source_contract_ref": "contracts/sources/futures/moex_algopack_futoi.v2.yaml",
    "raw_contract_ref": "contracts/datasets/futures_futoi_raw.v2.yaml",
    "quality_contract_ref": "contracts/datasets/futures_futoi_quality_report.v2.yaml",
    "manifest_contract_ref": "contracts/datasets/futures_futoi_refresh_manifest.v2.yaml",
}
ROOT_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "instrument_id", "source_id", "raw_schema_version", "source_identity_scope",
    "source_ticker", "ticker", "trade_date", "tradedate", "tradetime", "ts", "moment",
    "systime", "sess_id", "seqnum", "clgroup", "pos", "pos_long", "pos_short",
    "pos_long_num", "pos_short_num", "availability_ts_utc", "ingest_ts",
)


def _raw_version(value: object) -> str:
    if not isinstance(value, str) or value not in ("v1", RAW_SCHEMA_V2):
        _fail("unsupported FUTOI raw_schema_version")
    return value


def _aware_utc(value: object, field: str) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise FutoiSourceNativeRefreshError(field + " must be a timezone-aware timestamp") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        _fail(field + " must be a timezone-aware timestamp")
    return stamp.tz_convert("UTC")


def _root_identity(instrument_id: str) -> dict[str, str]:
    checked = _instrument_id(instrument_id)
    binding = _binding(checked)
    ticker = str(binding["futoi.ticker"]).strip().lower()
    if binding.get("instrument_id") != checked or ROOT_TICKERS[checked] != ticker:
        _fail("v2 FUTOI registry instrument/ticker root mismatch")
    return {
        "instrument_id": checked, "source_id": SOURCE_ID, "source_ticker": ticker,
        "raw_schema_version": RAW_SCHEMA_V2, "source_identity_scope": ROOT_IDENTITY_SCOPE,
    }


def _checked_root_path(root: Path, relative: Path) -> Path:
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        _fail("v2 root must be an existing absolute non-symlink directory")
    if relative.is_absolute() or any(p in (".", "..") for p in relative.parts):
        _fail("v2 artifact reference must be a canonical relative path")
    path = root
    for part in relative.parts:
        path /= part
        if path.is_symlink():
            _fail("v2 artifact path contains a symlink")
    if not path.resolve().is_relative_to(root.resolve()):
        _fail("v2 artifact escaped MOEX_DATA_ROOT")
    return path


def _root_current_path(root: Path, instrument_id: str) -> Path:
    return _checked_root_path(
        root, Path("state") / "datasets" / ("dataset_id=" + DATASET_ID)
        / "schema_version=v2" / ("instrument_id=" + _instrument_id(instrument_id)) / "current.json",
    )


def _validated_root_frame(
    frame: pd.DataFrame, *, instrument_id: str, trade_date: str, ticker: str,
) -> pd.DataFrame:
    """Validate the declared raw-v2 representation without admitting a pair."""
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        _fail("v2 FUTOI partition is empty")
    if frame.columns.has_duplicates or not set(ROOT_REQUIRED_COLUMNS) <= set(frame.columns):
        _fail("v2 FUTOI partition has duplicate or missing required columns")
    if "secid" in frame.columns:
        _fail("v2 root-scoped FUTOI must not contain a contract secid")
    checked = _instrument_id(instrument_id)
    day = _iso_date(trade_date, "expected_trade_date")
    if ticker != ROOT_TICKERS[checked]:
        _fail("v2 FUTOI expected ticker does not match instrument root")
    if frame[list(ROOT_REQUIRED_COLUMNS)].isna().any().any():
        _fail("v2 FUTOI partition contains null required values")
    expected = {
        "instrument_id": checked, "source_id": SOURCE_ID, "trade_date": day,
        "raw_schema_version": RAW_SCHEMA_V2, "source_identity_scope": ROOT_IDENTITY_SCOPE,
        "source_ticker": ticker,
    }
    for field, value in expected.items():
        if not frame[field].eq(value).all():
            _fail("v2 FUTOI partition " + field + " mismatch")
    if not frame["ticker"].astype(str).str.strip().str.lower().eq(ticker).all():
        _fail("v2 FUTOI original ticker mismatch")
    if not frame["clgroup"].isin(["FIZ", "YUR"]).all():
        _fail("v2 FUTOI contains unsupported clgroup")
    work = frame.copy()
    for field in ("sess_id", "seqnum", *materializer.POSITION_FIELDS):
        values = [_as_int(value, field) for value in work[field].tolist()]
        if any(not -(2**63) <= value < 2**63 for value in values):
            _fail("v2 FUTOI " + field + " exceeds exact int64 storage range")
        work[field] = pd.Series(values, index=work.index, dtype="int64")
    if work.duplicated(list(ROOT_KEY_FIELDS)).any():
        _fail("v2 FUTOI partition contains duplicate source-record keys")
    events = []
    for row in work.to_dict("records"):
        event = _market_timestamp_to_utc(row["ts"], "ts")
        moment = _market_timestamp_to_utc(row["moment"], "moment")
        original = _market_timestamp_to_utc(
            str(row["tradedate"]) + " " + str(row["tradetime"]), "tradedate/tradetime",
        )
        publication = _market_timestamp_to_utc(row["systime"], "systime")
        receipt = _aware_utc(row["availability_ts_utc"], "availability_ts_utc")
        ingest = _aware_utc(row["ingest_ts"], "ingest_ts")
        if event != moment or event != original or event.tz_convert(MARKET_TZ).date().isoformat() != day:
            _fail("v2 FUTOI event/moment/source date mismatch")
        if not event <= publication <= receipt <= ingest:
            _fail("v2 FUTOI source/publication/receipt/ingest clocks are inconsistent")
        if (row["pos_long"] < 0 or row["pos_short"] > 0
                or row["pos_long_num"] < 0 or row["pos_short_num"] < 0):
            _fail("v2 FUTOI contains invalid position signs/counts")
        if row["pos"] != row["pos_long"] + row["pos_short"]:
            _fail("v2 FUTOI per-row net position identity failed")
        events.append(event)
    work["_parsed_ts"] = pd.Series(events, index=work.index)
    return work


def _latest_root_factual(
    frame: pd.DataFrame, *, expected_trade_date: str, expected_instrument_id: str,
    expected_source_ticker: str, expected_secid: str | None,
) -> dict[str, object]:
    if expected_secid is not None:
        _fail("v2 root identity cannot be selected with a contract secid")
    work = _validated_root_frame(
        frame, instrument_id=expected_instrument_id, trade_date=expected_trade_date,
        ticker=expected_source_ticker,
    )
    latest = work["_parsed_ts"].max()
    if set(work.loc[work["_parsed_ts"].eq(latest), "clgroup"]) != {"FIZ", "YUR"}:
        _fail("latest source FUTOI timestamp must contain exactly FIZ and YUR; fallback forbidden")
    selected = {group: _resolved_group(work, group, latest) for group in ("FIZ", "YUR")}
    sessions = {_as_int(row["sess_id"], "sess_id") for row in selected.values()}
    if len(sessions) != 1:
        _fail("latest aligned FUTOI FIZ/YUR snapshot must share sess_id")
    sides = {}
    records = {}
    for group, row in selected.items():
        sides[group] = {
            "long": _as_int(row["pos_long"], "pos_long"),
            "short": -_as_int(row["pos_short"], "pos_short"),
            "net": _as_int(row["pos"], "pos"),
            "long_participants": _as_int(row["pos_long_num"], "pos_long_num"),
            "short_participants": _as_int(row["pos_short_num"], "pos_short_num"),
        }
        records[group] = {
            "trade_date": expected_trade_date, "source_ticker": expected_source_ticker,
            "clgroup": group, "sess_id": _as_int(row["sess_id"], "sess_id"),
            "seqnum": _as_int(row["seqnum"], "seqnum"),
            "snapshot_ts": latest.isoformat(),
            "source_publication_time": _market_timestamp_to_utc(row["systime"], "systime").isoformat(),
            "availability_ts_utc": _aware_utc(row["availability_ts_utc"], "availability_ts_utc").isoformat(),
            "ingest_ts_utc": _aware_utc(row["ingest_ts"], "ingest_ts").isoformat(),
        }
    fiz, yur = sides["FIZ"], sides["YUR"]
    if fiz["net"] + yur["net"] != 0:
        _fail("FIZ/YUR net positions do not balance to zero")
    total = fiz["long"] + yur["long"]
    if total != fiz["short"] + yur["short"]:
        _fail("FIZ/YUR total long and short open interest do not balance")
    return {
        "trade_date": expected_trade_date, "snapshot_ts": latest.isoformat(),
        "source_publication_time": max(_aware_utc(r["source_publication_time"], "source_publication_time") for r in records.values()).isoformat(),
        "availability_ts_utc": max(_aware_utc(r["availability_ts_utc"], "availability_ts_utc") for r in records.values()).isoformat(),
        "ingest_ts_utc": max(_aware_utc(r["ingest_ts_utc"], "ingest_ts_utc") for r in records.values()).isoformat(),
        "source_ticker": expected_source_ticker, "raw_schema_version": RAW_SCHEMA_V2,
        "source_identity_scope": ROOT_IDENTITY_SCOPE,
        "sess_id": next(iter(sessions)), "selected_source_records": records,
        "fiz": fiz, "yur": yur, "total_open_interest": total,
        "short_semantics": "absolute_contract_count",
        "timestamp_semantics": "source_event_and_publication_localized_from_Europe/Moscow_to_UTC",
        "fiz_yur_alignment": "latest_exact_shared_source_event_ts_and_sess_id_after_max_seqnum_revision_resolution",
    }


def _root_proof_bytes(root: Path, ref: object, digest: object, suffix: str) -> bytes:
    if (not isinstance(digest, str) or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)):
        _fail("v2 evidence SHA-256 is invalid")
    relative = (Path("state") / "datasets" / ("dataset_id=" + DATASET_ID)
                / "evidence" / (digest + suffix))
    if ref != ROOT_REF_PREFIX + relative.as_posix():
        _fail("v2 evidence reference is not the exact content-addressed artifact")
    path = _checked_root_path(root, relative)
    _rooted_ref(root, path)
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != digest:
        _fail("v2 evidence SHA mismatch")
    return content


def _root_original_paths(root: Path, instrument_id: str, trade_date: str, raw_run_id: str) -> dict[str, Path]:
    instrument_id = _instrument_id(instrument_id)
    trade_date = _iso_date(trade_date, "trade_date")
    raw_run_id = _safe_token(raw_run_id, "raw_run_id")
    return {
        "raw_partition": root / "market" / "supplementary" / "dataset_id=futures_futoi_raw"
        / "schema_version=v2" / ("instrument_id=" + instrument_id)
        / ("trade_date=" + trade_date) / ("source=" + SOURCE_ID) / "part.parquet",
        "raw_quality_report": root / "state" / "quality" / "dataset_id=futures_futoi_raw"
        / "schema_version=v2" / ("run_date=" + trade_date)
        / ("run_id=" + raw_run_id) / "quality_report.json",
        "raw_refresh_manifest": root / "state" / "refresh" / "dataset_id=futures_futoi_raw"
        / "schema_version=v2" / ("run_date=" + trade_date)
        / ("run_id=" + raw_run_id) / "manifest.json",
    }


def _verified_root_frame(
    root: Path, provenance: Mapping[str, object], *, instrument_id: str, trade_date: str,
) -> pd.DataFrame:
    """Read each SHA-verified buffer once; never fall back to v1 or EOD."""
    import io

    if not isinstance(provenance, Mapping):
        _fail("v2 provenance must be an object")
    checked = _instrument_id(instrument_id)
    expected = {
        "instrument_id": checked, "source_id": SOURCE_ID, "source_ticker": ROOT_TICKERS[checked],
        "raw_schema_version": RAW_SCHEMA_V2, "source_identity_scope": ROOT_IDENTITY_SCOPE,
        "source_record_key_fields": list(ROOT_KEY_FIELDS), **ROOT_CONTRACT_REFS,
        "raw_producer": "moex_data.futures.materialize_futoi_instrument.v2",
        "accepted_state_kind": "source_native_exact_date_raw_quality_pass",
    }
    if "secid" in provenance or any(provenance.get(k) != v for k, v in expected.items()):
        _fail("v2 provenance version/source/contract mismatch")
    raw_run_id = _safe_token(provenance.get("publication_run_id"), "publication_run_id")
    paths = _root_original_paths(root, checked, trade_date, raw_run_id)
    for key, path in paths.items():
        expected_ref = ROOT_REF_PREFIX + path.relative_to(root).as_posix()
        if provenance.get("original_" + key + "_ref") != expected_ref:
            _fail("v2 provenance original version/path mismatch")
    buffers = {
        key: _root_proof_bytes(root, provenance.get(key + "_ref"),
                              provenance.get(key + "_sha256"), ".parquet" if key == "raw_partition" else ".json")
        for key in paths
    }
    try:
        quality = json.loads(buffers["raw_quality_report"])
        manifest = json.loads(buffers["raw_refresh_manifest"])
    except (ValueError, UnicodeError) as exc:
        raise FutoiSourceNativeRefreshError("v2 evidence JSON is malformed") from exc
    identity = {k: expected[k] for k in (
        "instrument_id", "source_id", "source_ticker", "raw_schema_version",
        "source_identity_scope", "source_record_key_fields",
    )}
    identity["dataset_id"] = materializer.DATASET_ID
    identity["futoi_ticker"] = ROOT_TICKERS[checked]
    for value, schema in ((quality, "futures_futoi_quality_report.v2"),
                          (manifest, "futures_futoi_refresh_manifest.v2")):
        if (not isinstance(value, Mapping) or value.get("schema_version") != schema
                or "secid" in value or value.get("run_id") != raw_run_id
                or any(value.get(k) != v for k, v in identity.items())):
            _fail("v2 raw metadata schema/run/source mismatch")
    if (quality.get("trade_date") != trade_date or quality.get("quality_status") != "pass"
            or quality.get("failure_reasons") != []
            or quality.get("quality_scope") != "raw_structure_only_not_latest_pair_admission"
            or quality.get("quality_contract_ref") != ROOT_CONTRACT_REFS["quality_contract_ref"]):
        _fail("v2 raw quality metadata mismatch")
    for key in ("duplicate_key_count", "null_required_count", "invalid_position_count"):
        if _as_int(quality.get(key), key) != 0:
            _fail("v2 raw quality counter is nonzero")
    if _as_int(quality.get("exact_duplicate_rows_dropped"), "exact_duplicate_rows_dropped") < 0:
        _fail("v2 raw duplicate-removal counter is negative")
    source_contract = manifest.get("source_contract")
    if (not isinstance(source_contract, Mapping) or "secid" in source_contract
            or any(source_contract.get(k) != v for k, v in {**identity, **ROOT_CONTRACT_REFS}.items())
            or source_contract.get("transport") != "authenticated_apim"):
        _fail("v2 manifest source contract mismatch")
    endpoint = source_contract.get("source_endpoint_url")
    if not isinstance(endpoint, str) or not endpoint.endswith(
            "/iss/analyticalproducts/futoi/securities/" + ROOT_TICKERS[checked] + ".json"):
        _fail("v2 manifest FUTOI endpoint identity mismatch")
    if (manifest.get("run_date") != trade_date or manifest.get("requested_from") != trade_date
            or manifest.get("requested_till") != trade_date or manifest.get("refresh_status") != "succeeded"
            or manifest.get("publication_run_id") != raw_run_id
            or manifest.get("published_partition_sha256") != provenance["raw_partition_sha256"]
            or manifest.get("instrument_scope") != [checked] or manifest.get("source_scope") != [SOURCE_ID]
            or manifest.get("partitions_written") != [paths["raw_partition"].as_posix()]
            or manifest.get("partitions_skipped") != []
            or manifest.get("quality_report_ref") != paths["raw_quality_report"].as_posix()
            or manifest.get("producer") != expected["raw_producer"]
            or manifest.get("accepted_manifest_ref") is not None
            or manifest.get("accepted_manifest_pointer_reference") is not None
            or manifest.get("factual_authority") is not False
            or manifest.get("stage5_pointer_promotion_performed") is not False):
        _fail("v2 raw manifest date/path/status/provenance mismatch")
    try:
        frame = pd.read_parquet(io.BytesIO(buffers["raw_partition"]))
    except Exception as exc:
        raise FutoiSourceNativeRefreshError("v2 verified Parquet buffer is unreadable") from exc
    if _as_int(quality.get("row_count"), "row_count") != len(frame) or frame.empty:
        _fail("v2 raw quality row_count mismatch")
    return _validated_root_frame(frame, instrument_id=checked, trade_date=trade_date,
                                 ticker=ROOT_TICKERS[checked]).drop(columns="_parsed_ts")


def replay_root_factual(
    root: Path, provenance: Mapping[str, object], *, instrument_id: str, trade_date: str,
) -> dict[str, object]:
    frame = _verified_root_frame(root, provenance, instrument_id=instrument_id, trade_date=trade_date)
    return latest_aligned_factual(
        frame, expected_trade_date=trade_date, expected_instrument_id=instrument_id,
        expected_source_ticker=ROOT_TICKERS[instrument_id], raw_schema_version=RAW_SCHEMA_V2,
    )


def _materialize_root_target(
    root: Path, target_trade_date: str, run_id: str, *, instrument_id: str, timeout: float,
) -> tuple[Path, dict[str, object]]:
    from .futoi_publication_audit import audited_latest

    identity = _root_identity(instrument_id)
    _checked_root_path(root, Path("state") / "datasets" / ("dataset_id=" + DATASET_ID) / "evidence")
    target_trade_date = _iso_date(target_trade_date, "target_trade_date")
    run_id = _safe_token(run_id, "run_id")
    if root.resolve() != _data_root():
        _fail("v2 materializer and reader data roots differ")
    raw_run_id = run_id + "_" + instrument_id + "_raw_" + target_trade_date.replace("-", "")
    paths = _root_original_paths(root, instrument_id, target_trade_date, raw_run_id)
    for path in paths.values():
        _checked_root_path(root, path.relative_to(root))
    if any(paths[key].exists() for key in ("raw_quality_report", "raw_refresh_manifest")):
        _fail("v2 factual raw run metadata already exists; reuse is forbidden")
    try:
        result = materializer.materialize_futoi_partition(
            trade_date=target_trade_date, instrument_id=instrument_id, run_id=raw_run_id,
            timeout=timeout, require_enabled=False, raw_schema_version=RAW_SCHEMA_V2,
        )
    except Exception as exc:
        # Only this previously empty run slot can supply the failed-attempt metadata.
        evidence = {
            **identity, "trade_date": target_trade_date, "publication_run_id": raw_run_id,
            "status": "FAILED_RAW_MATERIALIZATION", "error_class": type(exc).__name__, "error": str(exc),
        }
        for key in ("raw_quality_report", "raw_refresh_manifest"):
            path = paths[key]
            if not path.exists():
                continue
            try:
                _checked_root_path(root, path.relative_to(root))
                meta = _load_json(path, key)
                if (meta.get("run_id") != raw_run_id or meta.get("instrument_id") != instrument_id
                        or meta.get("raw_schema_version") != RAW_SCHEMA_V2
                        or meta.get("source_identity_scope") != ROOT_IDENTITY_SCOPE):
                    _fail("failed v2 raw metadata identity mismatch")
                if key == "raw_quality_report" and (
                        meta.get("quality_status") != "fail" or meta.get("error_class") != type(exc).__name__
                        or meta.get("failure_reasons") != [str(exc)]):
                    _fail("failed v2 raw quality metadata mismatch")
                if key == "raw_refresh_manifest" and (
                        meta.get("refresh_status") != "failed" or meta.get("partitions_written") != []):
                    _fail("failed v2 raw manifest mismatch")
                digest = _sha256_file(path)
                frozen = _freeze_artifact(root, path, digest)
                evidence.update({key + "_ref": _rooted_ref(root, frozen), key + "_sha256": digest})
            except Exception as evidence_error:
                evidence[key + "_error"] = type(evidence_error).__name__ + ": " + str(evidence_error)
        exc.attempt_provenance = evidence
        raise
    if (not isinstance(result, Mapping) or "secid" in result
            or any(result.get(k) != v for k, v in {**identity, **ROOT_CONTRACT_REFS}.items())
            or result.get("status") != "succeeded" or result.get("quality_status") != "pass"
            or result.get("trade_date") != target_trade_date or result.get("publication_run_id") != raw_run_id):
        _fail("v2 canonical exact-date materialization did not match its declared identity")
    result_paths = {
        "raw_partition": "storage_partition_path", "raw_quality_report": "quality_report_reference",
        "raw_refresh_manifest": "manifest_reference",
    }
    provenance = {
        **identity, **ROOT_CONTRACT_REFS, "source_record_key_fields": list(ROOT_KEY_FIELDS),
        "publication_run_id": raw_run_id,
        "accepted_state_kind": "source_native_exact_date_raw_quality_pass",
        "raw_producer": "moex_data.futures.materialize_futoi_instrument.v2",
    }
    for key, path in paths.items():
        if result.get(result_paths[key]) != path.as_posix():
            _fail("v2 materialization result version/path mismatch")
        digest = (result.get("published_partition_sha256") if key == "raw_partition"
                  else _sha256_file(path))
        if not isinstance(digest, str) or len(digest) != 64:
            _fail("v2 materialization result has invalid SHA")
        frozen = _freeze_artifact(root, path, digest)
        provenance.update({
            key + "_ref": _rooted_ref(root, frozen), key + "_sha256": digest,
            "original_" + key + "_ref": ROOT_REF_PREFIX + path.relative_to(root).as_posix(),
        })
    # The existing audit archives rejected newest pairs for BOTH roots.
    frame = _verified_root_frame(root, provenance, instrument_id=instrument_id, trade_date=target_trade_date)
    audited_latest(
        root, frame, provenance, expected_trade_date=target_trade_date,
        expected_instrument_id=instrument_id, expected_source_ticker=identity["source_ticker"],
        raw_schema_version=RAW_SCHEMA_V2,
    )
    frozen_path = root / str(provenance["raw_partition_ref"]).removeprefix(ROOT_REF_PREFIX)
    return frozen_path, provenance


def _root_date_candidate(through_date: str, *, timeout: float, started: pd.Timestamp) -> tuple[str, list[dict]]:
    """Dates are USDRUBF witnesses only; own FUTOI is queried by the materializer."""
    end = date.fromisoformat(_iso_date(through_date, "through_date"))
    if end >= started.tz_convert(MARKET_TZ).date():
        _fail("through_date must be a completed Europe/Moscow calendar date")
    start = end - timedelta(days=SOURCE_LOOKBACK_DAYS - 1)
    if observed_dates.reference_secid(DATE_WITNESS_INSTRUMENT_ID) != DATE_WITNESS_SECID:
        _fail("v2 USDRUBF date-witness registry binding mismatch")
    raw = observed_dates.observed_dates(
        start.isoformat(), end.isoformat(), instrument_id=DATE_WITNESS_INSTRUMENT_ID, timeout=timeout,
    )
    dates = observed_dates.normalize_observed_dates(raw, start.isoformat(), end.isoformat())
    if not dates:
        _fail("no observed USDRUBF date witness in the bounded interval")
    observations = [{
        "trade_date": day, "status": "OBSERVED_TRADESTATS_DATE",
        "date_authority_source_id": observed_dates.SOURCE_ID,
        "witness_instrument_id": DATE_WITNESS_INSTRUMENT_ID, "witness_secid": DATE_WITNESS_SECID,
        "futoi_availability_proven": False,
    } for day in dates]
    return dates[-1], observations


def _archive_root_context(root: Path, path: Path, payload: dict[str, object]) -> None:
    # Check every path component, including the reused content-addressed namespace.
    _checked_root_path(root, path.relative_to(root))
    _checked_root_path(root, Path("state") / "datasets" / ("dataset_id=" + DATASET_ID) / "evidence")
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, suffix=".json", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
    try:
        digest = hashlib.sha256(content).hexdigest()
        archived = _freeze_artifact(root, temporary, digest)
    finally:
        temporary.unlink(missing_ok=True)
    payload["run_evidence_ref"] = _rooted_ref(root, archived)
    payload["run_evidence_sha256"] = digest
    _atomic_json(path, payload)


def _run_root_refresh(
    *, through_date: str, instrument_id: str, run_id: str, timeout: float,
    now_fn: Callable[[], datetime],
) -> dict[str, object]:
    checked = _instrument_id(instrument_id)
    day = _iso_date(through_date, "through_date")
    run_id = _safe_token(run_id, "run_id")
    root = _data_root()
    path = _root_current_path(root, checked)
    started = _aware_utc(now_fn(), "refresh_started_at")
    target = None
    observations = []
    try:
        identity = _root_identity(checked)
        target, observations = _root_date_candidate(day, timeout=timeout, started=started)
        _, provenance = _materialize_target(
            root, target, run_id, instrument_id=checked, timeout=timeout, raw_schema_version=RAW_SCHEMA_V2,
        )
        factual = replay_root_factual(root, provenance, instrument_id=checked, trade_date=target)
        completed = _aware_utc(now_fn(), "last_success_at")
        if not started <= _aware_utc(factual["availability_ts_utc"], "receipt") <= _aware_utc(
                factual["ingest_ts_utc"], "ingest") <= completed:
            _fail("v2 refresh/receipt/ingest/validation clocks are inconsistent")
        payload = {
            **identity, "schema_version": SCHEMA_VERSION_V2, "project": PROJECT, "status": "PASS",
            "run_id": run_id, "through_date": day, "expected_latest_source_trade_date": target,
            "refresh_started_at": started.isoformat(), "data_as_of": factual["snapshot_ts"],
            "last_success_at": completed.isoformat(), "source_date_observations": observations,
            "freshness": {
                "status": "FRESH", "policy": "bounded_usdrubf_witness_then_own_exact_date_futoi_v2",
                "source_lookback_days": SOURCE_LOOKBACK_DAYS, "accepted_trade_date": target,
                "trading_date_authority_source_id": observed_dates.SOURCE_ID,
                "witness_instrument_id": DATE_WITNESS_INSTRUMENT_ID, "witness_secid": DATE_WITNESS_SECID,
                "scope": "latest_completed_observed_date_not_current_intraday",
                "weekday_weekend_inference": False, "calendar_dependency": False,
            },
            "quality_status": "PASS", "acceptance_status": "PASS", "factual": factual, "provenance": provenance,
            "factual_authority": False, "directional_authority": False, "action_authority": False,
            "standalone_buy_sell_authority": False, "stage5_full_mode_required": False,
            "stage5_full_mode_ready": False, "stage5_pointer_promotion_performed": False,
            "historical_pit_research_ready_claimed": False, "session_completion_proven": False,
            "model_usable": False,
        }
    except Exception as exc:
        failed = _failed_instrument_result(checked, exc)
        failed.update(
            schema_version=SCHEMA_VERSION_V2, raw_schema_version=RAW_SCHEMA_V2,
            source_identity_scope=ROOT_IDENTITY_SCOPE, source_ticker=ROOT_TICKERS[checked],
            run_id=run_id, through_date=day, expected_latest_source_trade_date=target,
            refresh_started_at=started.isoformat(), source_date_observations=observations,
            quality_status="FAILED", acceptance_status="FAILED",
            freshness={"status": "UNAVAILABLE"}, factual=None,
            failed_attempt_evidence=getattr(exc, "attempt_provenance", None),
        )
        try:
            failed_at = _aware_utc(now_fn(), "failed_attempt_at")
            if failed_at < started:
                _fail("v2 failure clock precedes refresh start")
            failed["failed_attempt_at"] = failed_at.isoformat()
        except Exception as clock_error:
            failed["failure_clock_error"] = type(clock_error).__name__ + ": " + str(clock_error)
        # A failed current attempt must not leave the old successful envelope current.
        try:
            _archive_root_context(root, path, failed)
        except Exception as persistence_error:
            failed["failure_persistence_error"] = type(persistence_error).__name__ + ": " + str(persistence_error)
        exc.failure_payload = failed
        raise
    _archive_root_context(root, path, payload)
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
