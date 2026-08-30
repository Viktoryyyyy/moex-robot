from __future__ import annotations

from pathlib import Path
from typing import Final, Sequence

from . import refresh_forts_raw_5m_incremental as tradestats

SOURCE_ARTIFACT_ID: Final[str] = tradestats.SOURCE_ARTIFACT_ID
SOURCE_ID: Final[str] = tradestats.OBSERVED_DATE_SOURCE_ID
SOURCE_ENDPOINT: Final[str] = tradestats.OBSERVED_DATE_SOURCE_ENDPOINT
REGISTRY_PATH: Final[str] = tradestats.REGISTRY_PATH
SELECTION_RULE: Final[str] = "observed_trade_dates_only"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_registry_path(registry_path: str | Path | None = None) -> Path:
    path = Path(registry_path) if registry_path is not None else _repo_root() / REGISTRY_PATH
    if not path.is_file():
        raise ValueError("observed TradeStats registry does not exist: " + path.as_posix())
    return path


def reference_secid(instrument_id: str, registry_path: str | Path | None = None) -> str:
    checked_instrument = tradestats._require_token(instrument_id, "instrument_id")
    path = resolve_registry_path(registry_path)
    matches: list[str] = []
    for entry in tradestats._registry_entries(path.read_text(encoding="utf-8")):
        if entry.get("instrument_id") != checked_instrument:
            continue
        if entry.get("source_artifact_id") != SOURCE_ARTIFACT_ID:
            continue
        if entry.get("source_id") != SOURCE_ID:
            continue
        secid = str(entry.get("secid") or "").strip()
        if secid:
            matches.append(tradestats._require_token(secid, "registry secid"))
    unique = sorted(set(matches))
    if len(unique) != 1:
        raise ValueError(
            "observed TradeStats registry binding must resolve exactly one secid for instrument_id="
            + checked_instrument
            + "; matches="
            + repr(unique)
        )
    return unique[0]


def observed_dates(
    date_start: str,
    date_end: str,
    *,
    instrument_id: str,
    registry_path: str | Path | None = None,
    timeout: float = 30.0,
    apim_base_url: str | None = None,
) -> list[str]:
    secid = reference_secid(instrument_id, registry_path)
    return tradestats.fetch_observed_tradestats_dates(
        date_start,
        date_end,
        secid=secid,
        timeout=timeout,
        apim_base_url=apim_base_url,
    )


def normalize_observed_dates(values: Sequence[str], date_start: str, date_end: str) -> list[str]:
    start = tradestats._coerce_date(date_start, "date_start")
    end = tradestats._coerce_date(date_end, "date_end")
    return tradestats._normalize_observed_dates(values, date_start=start, date_end=end)
