from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
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


def _exact_date_has_secid(
    trade_date: date,
    *,
    secid: str,
    timeout: float,
    apim_base_url: str | None,
) -> bool:
    checked_secid = tradestats._require_token(secid, "secid")
    trade_date_text = trade_date.isoformat()
    base_url = tradestats.materializer.core._apim_base_url(apim_base_url, None)
    endpoint = tradestats.materializer.core._source_url(base_url, SOURCE_ENDPOINT)
    headers = tradestats.materializer._auth_headers_with_bearer(None)
    seen_signatures: set[tuple[object, ...]] = set()
    start = 0
    matched = False

    for _ in range(tradestats.MAX_APIM_PAGES):
        params = {
            "date": trade_date_text,
            "from": trade_date_text,
            "till": trade_date_text,
            "secid": checked_secid,
            "start": start,
            "iss.meta": "off",
            "iss.only": "tradestats",
        }
        response = tradestats.requests.get(endpoint, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("response JSON root is not an object")
        frame = tradestats.materializer.core._block_to_frame(payload)
        if frame.empty:
            break
        signature = tradestats._page_signature(frame)
        if signature in seen_signatures:
            raise ValueError("pagination did not advance")
        seen_signatures.add(signature)
        secid_col = tradestats.materializer.core._canonical_column(frame, ("secid",))
        date_col = tradestats.materializer.core._canonical_column(frame, ("tradedate", "date"))
        if secid_col is None or date_col is None:
            raise ValueError("tradestats response missing secid/tradedate columns")
        scoped = frame.loc[frame[secid_col].astype(str).str.strip().str.upper() == checked_secid.upper()]
        for raw_value in scoped[date_col].tolist():
            parsed = tradestats.materializer.core._parse_trade_date(raw_value)
            if parsed is None:
                raise ValueError("tradestats response contains invalid trade date")
            if parsed != trade_date_text:
                raise ValueError(
                    "exact-date TradeStats request returned mismatched trade date: requested="
                    + trade_date_text
                    + " returned="
                    + parsed
                )
            matched = True
        start += int(len(frame.index))
    else:
        raise ValueError("pagination exceeded max_pages guard")

    return matched


def observed_dates(
    date_start: str,
    date_end: str,
    *,
    instrument_id: str,
    registry_path: str | Path | None = None,
    timeout: float = 30.0,
    apim_base_url: str | None = None,
) -> list[str]:
    start = tradestats._coerce_date(date_start, "date_start")
    end = tradestats._coerce_date(date_end, "date_end")
    if start > end:
        raise ValueError("date_start must be <= date_end")
    secid = reference_secid(instrument_id, registry_path)
    observed: list[str] = []
    current = start
    try:
        while current <= end:
            if _exact_date_has_secid(
                current,
                secid=secid,
                timeout=timeout,
                apim_base_url=apim_base_url,
            ):
                observed.append(current.isoformat())
            current += timedelta(days=1)
    except Exception as exc:
        raise tradestats._source_error(
            secid=secid,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            detail=str(exc),
        ) from exc
    if not observed:
        raise tradestats._source_error(
            secid=secid,
            date_start=start.isoformat(),
            date_end=end.isoformat(),
            detail="authoritative AlgoPack TradeStats source returned no observed trade dates",
        )
    return observed


def normalize_observed_dates(values: Sequence[str], date_start: str, date_end: str) -> list[str]:
    start = tradestats._coerce_date(date_start, "date_start")
    end = tradestats._coerce_date(date_end, "date_end")
    return tradestats._normalize_observed_dates(values, date_start=start, date_end=end)
