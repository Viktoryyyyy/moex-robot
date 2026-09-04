from __future__ import annotations

import os
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Final

import requests

from moex_data import synchronized_live_market_oi_context as core


APIM_FULL_RESPONSE_PROBE_START: Final[int] = 1_000_000_000
COMPLETENESS_MODE: Final[str] = "apim_full_response_start_invariant_probe"
FORTS_WAP_STATUS: Final[str] = "unavailable_source_native"
CONTRACT_METADATA_SOURCE_ID: Final[str] = "moex_apim_forts_rfud_live_securities"
EXPIRING_LOGICAL_IDS: Final[tuple[str, ...]] = (
    "si_front",
    "si_next",
    "cr_front",
    "cr_next",
)


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


def _without_unproven_forts_wap(payload: Mapping[str, object]) -> dict[str, object]:
    copied = deepcopy(dict(payload))
    block = copied.get("marketdata")
    if not isinstance(block, dict):
        raise core.SynchronizedLiveMarketOIError("FORTS marketdata block is missing")
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise core.SynchronizedLiveMarketOIError("FORTS marketdata block is invalid")
    by_upper = {str(column).upper(): index for index, column in enumerate(columns)}
    value_index = by_upper.get("VALTODAY")
    if value_index is None:
        raise core.SynchronizedLiveMarketOIError("FORTS marketdata VALTODAY column is missing")
    for row in rows:
        if not isinstance(row, list) or value_index >= len(row):
            raise core.SynchronizedLiveMarketOIError("FORTS marketdata row is invalid")
        row[value_index] = None
    return copied


def _mark_wap_semantics(snapshot: dict[str, object]) -> None:
    instruments = snapshot.get("instruments")
    provenance = snapshot.get("provenance")
    if not isinstance(instruments, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot instruments are missing")
    for logical_id in core.FUTURES_LOGICAL_ORDER:
        item = instruments.get(logical_id)
        if not isinstance(item, dict):
            raise core.SynchronizedLiveMarketOIError(f"snapshot instrument {logical_id} is missing")
        item["wap"] = None
        item["wap_method"] = None
        item["wap_status"] = FORTS_WAP_STATUS
    spot = instruments.get("cnyrub_tom")
    if not isinstance(spot, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot instrument cnyrub_tom is missing")
    spot["wap_status"] = "available_source_native" if spot.get("wap") is not None else "missing_source_native"
    if not isinstance(provenance, dict) or not isinstance(provenance.get("forts"), dict):
        raise core.SynchronizedLiveMarketOIError("snapshot FORTS provenance is missing")
    provenance["forts"]["wap_method"] = None
    provenance["forts"]["wap_status"] = FORTS_WAP_STATUS


def _attach_expiry_metadata(
    snapshot: dict[str, object],
    forts_payload: Mapping[str, object],
) -> None:
    instruments = snapshot.get("instruments")
    provenance = snapshot.get("provenance")
    if not isinstance(instruments, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot instruments are missing")
    if not isinstance(provenance, dict) or not isinstance(provenance.get("forts"), dict):
        raise core.SynchronizedLiveMarketOIError("snapshot FORTS provenance is missing")
    securities = core._table_frame(forts_payload, "securities")
    core._require_columns(securities, core.FUTURES_SECURITY_COLUMNS, "FORTS securities")
    source_url = provenance["forts"].get("source_url")

    for logical_id in EXPIRING_LOGICAL_IDS:
        item = instruments.get(logical_id)
        if not isinstance(item, dict):
            raise core.SynchronizedLiveMarketOIError(f"snapshot instrument {logical_id} is missing")
        secid = str(item.get("secid") or "").strip()
        if not secid:
            raise core.SynchronizedLiveMarketOIError(f"snapshot instrument {logical_id} SECID is missing")
        row = core._row_by_secid(securities, secid, block_name="FORTS securities")
        raw_expiry = str(row.get("LASTTRADEDATE") or "").strip()
        try:
            expiry = date.fromisoformat(raw_expiry).isoformat()
        except ValueError as exc:
            raise core.SynchronizedLiveMarketOIError(
                f"{secid}.LASTTRADEDATE is invalid"
            ) from exc
        item["expiry_date"] = expiry
        item["expiry_metadata"] = {
            "source_id": CONTRACT_METADATA_SOURCE_ID,
            "source_url": source_url,
            "source_field": "LASTTRADEDATE",
            "same_rfud_response_as_live_binding": True,
            "front_next_minimum_days_to_expiry": 1,
            "expiry_day_contract_allowed": False,
        }


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
    probe, _probe_url, probe_received = core._fetch_json(
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

    normalized_forts_payload = _without_unproven_forts_wap(forts_payload)
    snapshot = core.build_snapshot_from_payloads(
        forts_payload=normalized_forts_payload,
        cets_payload=cets_payload,
        forts_received_at_utc=forts_received,
        cets_received_at_utc=cets_received,
        forts_source_url=forts_source_url,
        cets_source_url=cets_source_url,
    )
    _mark_wap_semantics(snapshot)
    _attach_expiry_metadata(snapshot, normalized_forts_payload)
    provenance = snapshot.get("provenance")
    if not isinstance(provenance, dict) or not isinstance(provenance.get("forts"), dict):
        raise core.SynchronizedLiveMarketOIError("snapshot FORTS provenance is missing")
    provenance["forts"]["pagination_complete"] = True
    provenance["forts"]["completeness"] = completeness
    provenance["forts"]["contract_metadata_source_id"] = CONTRACT_METADATA_SOURCE_ID
    provenance["forts"]["contract_metadata_reused_from_live_response"] = True
    return snapshot


SCHEMA_VERSION = core.SCHEMA_VERSION
SynchronizedLiveMarketOIError = core.SynchronizedLiveMarketOIError
