from __future__ import annotations

from datetime import datetime, timezone

import pytest

from moex_data import synchronized_live_market_oi_context as core
from moex_data import synchronized_live_market_oi_context_apim as apim


class _Response:
    def __init__(self, payload: dict[str, object], url: str) -> None:
        self._payload = payload
        self.url = url
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _payload(secids: list[str]) -> dict[str, object]:
    return {
        "securities": {
            "columns": list(core.FUTURES_SECURITY_COLUMNS),
            "data": [[secid, "RFUD", "2026-12-17", 1.0, 1.0] for secid in secids],
        },
        "marketdata": {
            "columns": list(core.FUTURES_MARKETDATA_COLUMNS),
            "data": [
                [secid, 90.0, 92.0, 89.0, 91.0, 100, 9050, 10, 1000, 90.9, 91.1, "2026-09-02 13:00:00"]
                for secid in secids
            ],
        },
    }


def _fetch(first: dict[str, object], probe: dict[str, object]):
    calls: list[dict[str, object]] = []
    responses = iter((first, probe))
    times = iter((
        datetime(2026, 9, 2, 10, 0, 1, tzinfo=timezone.utc),
        datetime(2026, 9, 2, 10, 0, 2, tzinfo=timezone.utc),
    ))

    def http_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        headers: dict[str, str],
        allow_redirects: bool,
    ) -> _Response:
        assert allow_redirects is False
        calls.append(dict(params))
        return _Response(next(responses), url)

    result = apim._fetch_forts_verified(
        url="https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities.json",
        params={"iss.only": "securities,marketdata,securities.cursor"},
        headers={"Authorization": "Bearer test"},
        timeout=1.0,
        http_get=http_get,
        now_fn=lambda: next(times),
    )
    return result, calls


def test_cursorless_apim_full_response_requires_start_invariant_probe() -> None:
    source = _payload(["USDRUBF", "SIU6", "SIZ6", "CNYRUBF", "CRU6", "CRZ6"])

    (payload, _url, completion, completeness), calls = _fetch(source, _payload(["USDRUBF", "SIU6", "SIZ6", "CNYRUBF", "CRU6", "CRZ6"]))

    assert calls[0].get("start") is None
    assert calls[1]["start"] == apim.APIM_FULL_RESPONSE_PROBE_START
    assert completion == datetime(2026, 9, 2, 10, 0, 2, tzinfo=timezone.utc)
    assert completeness == {
        "mode": apim.COMPLETENESS_MODE,
        "cursor_present": False,
        "start_invariant_probe": True,
        "probe_start": apim.APIM_FULL_RESPONSE_PROBE_START,
        "securities_rows": 6,
        "marketdata_rows": 6,
    }
    assert payload[core.FORTS_ROW_RECEIPTS_KEY]["USDRUBF"] == "2026-09-02T10:00:01+00:00"


def test_cursorless_apim_full_response_rejects_start_sensitive_universe() -> None:
    first = _payload(["USDRUBF", "SIU6", "SIZ6"])
    probe = _payload(["USDRUBF", "SIZ6", "SIU6"])

    with pytest.raises(core.SynchronizedLiveMarketOIError, match="start-invariance proof failed"):
        _fetch(first, probe)


def test_cursorless_apim_full_response_rejects_security_marketdata_universe_mismatch() -> None:
    malformed = _payload(["USDRUBF", "SIU6"])
    malformed["marketdata"]["data"] = malformed["marketdata"]["data"][:1]

    with pytest.raises(core.SynchronizedLiveMarketOIError, match="SECID universe mismatch"):
        _fetch(malformed, malformed)


def test_cursorless_apim_full_response_rejects_duplicate_secid() -> None:
    malformed = _payload(["USDRUBF", "USDRUBF"])

    with pytest.raises(core.SynchronizedLiveMarketOIError, match="duplicate SECID"):
        _fetch(malformed, malformed)
