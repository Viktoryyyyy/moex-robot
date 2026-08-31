from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Final

import pandas as pd
import requests

from . import materialize_cets_tom_raw_5m as cets


SOURCE_ID: Final[str] = cets.SOURCE_ID
SOURCE_CONTRACT_REF: Final[str] = cets.SOURCE_CONTRACT_REF
SOURCE_ENDPOINT_PATTERN: Final[str] = (
    "/iss/engines/currency/markets/selt/boards/CETS/securities/{SECID}/candles.json"
)
REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "begin",
    "end",
)


class CetsObservedDateError(ValueError):
    pass


def _fail(message: str) -> None:
    raise CetsObservedDateError(message)


def _require_date(value: object) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise CetsObservedDateError("candidate trade_date must be explicit YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        _fail("candidate trade_date must be explicit YYYY-MM-DD")
    return text


def _require_secid(value: object) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text:
        _fail("CETS secid must be an explicit safe token")
    return text


def _frame(payload: Mapping[str, object]) -> pd.DataFrame:
    block = payload.get("candles")
    if not isinstance(block, Mapping):
        _fail("CETS observed-date response missing candles block")
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not isinstance(rows, list):
        _fail("CETS observed-date candles block has invalid shape")
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        return frame
    by_lower = {str(column).lower(): column for column in frame.columns}
    missing = [name for name in REQUIRED_COLUMNS if name not in by_lower]
    if missing:
        _fail("CETS observed-date candles schema missing: " + ",".join(missing))
    return frame


def _has_observed_date(
    trade_date: str,
    secid: str,
    *,
    timeout: float,
    base_url: str | None,
) -> bool:
    checked_date = _require_date(trade_date)
    checked_secid = _require_secid(secid)
    base = str(base_url or os.environ.get("MOEX_ISS_URL", cets.DEFAULT_BASE_URL)).strip().rstrip("/")
    if not base:
        _fail("MOEX ISS base URL is required")
    endpoint = SOURCE_ENDPOINT_PATTERN.replace("{SECID}", checked_secid)
    url = base + endpoint
    response = requests.get(
        url,
        params={
            "from": checked_date,
            "till": checked_date,
            "interval": 1,
            "start": 0,
            "iss.meta": "off",
            "iss.only": "candles",
        },
        timeout=timeout,
        headers={"User-Agent": "moex_bot_stage10_cets_date_probe/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, Mapping):
        _fail("CETS observed-date response root is not an object")
    frame = _frame(payload)
    if frame.empty:
        return False
    by_lower = {str(column).lower(): column for column in frame.columns}
    for field in ("begin", "end"):
        parsed = pd.to_datetime(frame[by_lower[field]], errors="coerce")
        if bool(parsed.isna().any()):
            _fail("CETS observed-date candles contain invalid " + field + " timestamps")
        observed = set(parsed.dt.date.astype(str))
        if observed != {checked_date}:
            _fail(
                "CETS observed-date response returned rows outside requested date "
                + checked_date
                + " for secid="
                + checked_secid
            )
    return True


def observed_common_dates(
    candidate_dates: Sequence[str],
    *,
    timeout: float = 30.0,
    base_url: str | None = None,
) -> list[str]:
    checked_dates = sorted({_require_date(value) for value in candidate_dates})
    if not checked_dates:
        return []
    secids = tuple(_require_secid(value) for value in cets.INSTRUMENTS.values())
    if not secids:
        _fail("canonical CETS TOM registry is empty")
    observed: list[str] = []
    for trade_date in checked_dates:
        if all(
            _has_observed_date(
                trade_date,
                secid,
                timeout=timeout,
                base_url=base_url,
            )
            for secid in secids
        ):
            observed.append(trade_date)
    return observed
