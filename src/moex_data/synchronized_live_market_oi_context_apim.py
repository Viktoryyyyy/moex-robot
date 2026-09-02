from __future__ import annotations

import os
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Final

import requests

from moex_data import synchronized_live_market_oi_context as core


APIM_FULL_RESPONSE_PROBE_START: Final[int] = 1_000_000_000
COMPLETENESS_MODE: Final[str] = "apim_full_response_start_invariant_probe"


def _secid_sequence(payload: Mapping[str, object], block_name: str) -> tuple[str, ...]:
    columns, rows = core._table_parts(payload, block_name, allow_empty=False)
    by_upper = {str(column).upper(): index for index, column in enumerate(columns)}
    secid_index = by_upper.get("SECID")
    if secid_index is None:
        raise core.SynchronizedLiveMarketOIError(f"{block_name} SECID column is missing")
    result: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if secid_index >= len(row):
            raise core.SynchronizedLiveMarketOIError(f"{block_name} row is invalid")
        secid = str(row[secid_index]).strip().upper()
        if not secid:
            raise core.SynchronizedLiveMarketOIError(f"{block_name} SECID value is missing")
        if secid in seen:
            raise core.SynchronizedLiveMarketOIError(
                f"{block_name} contains duplicate SECID in APIM full response: {secid}"
            )
        seen.add(secid)
        result.append(secid)
    return tuple(result)


def _validate_apim_full_response(payload: Mapping[str, object]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    securities = _secid_sequence(payload, "securities")
    marketdata = _secid_sequence(payload, "marketdata")
    if set(securities) != set(marketdata):
        missing_marketdata = sorted(set(securities) - set(marketdata))[:10]
        extra_marketdata = sorted(set(marketdata) - set(securities))[:10]
        raise core.SynchronizedLiveMarketOIError(
            "APIM RFUD securities/marketdata SECID universe mismatch: "
            f"securities={len(securities)} marketdata={len(marketdata)} "
            f"missing_marketdata={missing_marketdata} extra_marketdata={extra_marketdata}"
        )
    return securities, marketdata


def _receipt_map(payload: Mapping[str, object], received_at_utc: datetime) -> dict[str, str]:
    marketdata = _secid_sequence(payload, "marketdata")
    return {secid: core._iso(received_at_utc) for secid in marketdata}


def _fetch_forts_verified(
    *,
    url: str,
    params: Mapping[str, object],
    headers: Mapping[str, str],
    timeout: float,
    http_get: core.HTTPGet,
    now_fn: core.NowFn,
) -> tuple[dict[str, object], str, datetime, dict[str, object]]:
    first_params = dict(params)
    first_params.pop("start", None)
    first, source_url, first_received = core._fetch_json(
        url=url,
        params=first_params,
        headers=headers,
        timeout=timeout,
        http_get=http_get,
        now_fn=now_fn,
    )

    if isinstance(first.get("securities.cursor"), Mapping):
        payload, paged_url, received_at = core._fetch_forts_all_pages(
            url=url,
            params=params,
            headers=headers,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        return payload, paged_url, received_at, {
            "mode": "iss_cursor_pagination",
            "cursor_present": True,
            "start_invariant_probe": False,
        }

    first_securities, first_marketdata = _validate_apim_full_response(first)

    probe_params = dict(params)
    probe_params["start"] = APIM_FULL_RESPONSE_PROBE_START
    probe, probe_url, probe_received = core._fetch_json(
        url=url,
        params=probe_params,
        headers=headers,
        timeout=timeout,
        http_get=http_get,
        now_fn=now_fn,
    )
    if isinstance(probe.get("securities.cursor"), Mapping):
        raise core.SynchronizedLiveMarketOIError(
            "APIM RFUD pagination semantics changed between full-response verification requests"
        )
    probe_securities, probe_marketdata = _validate_apim_full_response(probe)

    if first_securities != probe_securities or first_marketdata != probe_marketdata:
        raise core.SynchronizedLiveMarketOIError(
            "APIM RFUD start-invariance proof failed: SECID universe/order changed when start was supplied"
        )
    if source_url != probe_url:
        raise core.SynchronizedLiveMarketOIError(
            "APIM RFUD source route changed during start-invariance verification"
        )

    first[core.FORTS_ROW_RECEIPTS_KEY] = _receipt_map(first, first_received)
    return first, source_url, probe_received, {
        "mode": COMPLETENESS_MODE,
        "cursor_present": False,
        "start_invariant_probe": True,
        "probe_start": APIM_FULL_RESPONSE_PROBE_START,
        "securities_rows": len(first_securities),
        "marketdata_rows": len(first_marketdata),
    }


def fetch_live_snapshot(
    *,
    timeout: float = 12.0,
    base_url: str | None = None,
    http_get: core.HTTPGet = requests.get,
    now_fn: core.NowFn = lambda: datetime.now(timezone.utc),
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    active_env = os.environ if env is None else env
    base = core._api_base_url(base_url, active_env)
    headers = core._auth_headers(active_env)
    forts_url = base + core.FORTS_ENDPOINT
    cets_url = base + core.CETS_ENDPOINT
    forts_params = {
        "iss.meta": "off",
        "iss.only": "securities,marketdata,securities.cursor",
        "securities.columns": ",".join(core.FUTURES_SECURITY_COLUMNS),
        "marketdata.columns": ",".join(core.FUTURES_MARKETDATA_COLUMNS),
    }
    cets_params = {
        "iss.meta": "off",
        "iss.only": "marketdata",
        "marketdata.columns": ",".join(core.CETS_MARKETDATA_COLUMNS),
    }

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="moex-live-snapshot") as executor:
        forts_future = executor.submit(
            _fetch_forts_verified,
            url=forts_url,
            params=forts_params,
            headers=headers,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        cets_future = executor.submit(
            core._fetch_json,
            url=cets_url,
            params=cets_params,
            headers=headers,
            timeout=timeout,
            http_get=http_get,
            now_fn=now_fn,
        )
        forts_payload, forts_source_url, forts_received, completeness = forts_future.result()
        cets_payload, cets_source_url, cets_received = cets_future.result()

    snapshot = core.build_snapshot_from_payloads(
        forts_payload=forts_payload,
        cets_payload=cets_payload,
        forts_received_at_utc=forts_received,
        cets_received_at_utc=cets_received,
        forts_source_url=forts_source_url,
        cets_source_url=cets_source_url,
    )
    provenance = snapshot.get("provenance")
    if not isinstance(provenance, dict) or not isinstance(provenance.get("forts"), dict):
        raise core.SynchronizedLiveMarketOIError("snapshot FORTS provenance is missing")
    provenance["forts"]["pagination_complete"] = True
    provenance["forts"]["completeness"] = completeness
    return snapshot


SCHEMA_VERSION = core.SCHEMA_VERSION
SynchronizedLiveMarketOIError = core.SynchronizedLiveMarketOIError
