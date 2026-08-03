from __future__ import annotations

import json
from datetime import date, datetime, timezone
from email.message import Message
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

import pandas as pd
import pytest

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_source_validation as source,
)


DAY = date(2026, 7, 29)
NOW = datetime(2026, 7, 30, 3, 0, tzinfo=timezone.utc)
TOKEN = "secret-token"
COLUMNS = list(source.RAW_REQUIRED_FIELDS)


def _row(
    group: str,
    *,
    moment: str = "2026-07-29 23:50:00",
    systime: str = "2026-07-29 23:51:00",
    sess_id: str = "1",
    seqnum: int | None = None,
    ticker: str = "Si",
    pos: float | None = None,
    pos_long: float | None = None,
    pos_short: float | None = None,
) -> list[object]:
    if group == "FIZ":
        values = {
            "pos": 10.0,
            "pos_long": 100.0,
            "pos_short": -90.0,
            "pos_long_num": 10,
            "pos_short_num": 9,
            "seqnum": 101,
        }
    else:
        values = {
            "pos": -10.0,
            "pos_long": 80.0,
            "pos_short": -90.0,
            "pos_long_num": 8,
            "pos_short_num": 9,
            "seqnum": 102,
        }
    values.update(
        {
            "sess_id": sess_id,
            "ticker": ticker,
            "clgroup": group,
            "moment": moment,
            "systime": systime,
        }
    )
    if seqnum is not None:
        values["seqnum"] = seqnum
    if pos is not None:
        values["pos"] = pos
    if pos_long is not None:
        values["pos_long"] = pos_long
    if pos_short is not None:
        values["pos_short"] = pos_short
    return [values[column] for column in COLUMNS]


def _payload(
    rows: list[list[object]],
    columns: list[str] | None = None,
    *,
    block_name: str = "data",
) -> bytes:
    return json.dumps(
        {block_name: {"columns": columns or COLUMNS, "data": rows}},
        separators=(",", ":"),
    ).encode("utf-8")


class Response:
    def __init__(self, payload: bytes = b'{"ok":true}') -> None:
        self.payload = payload

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def _pair(
    *,
    systime: str = "2026-07-29 23:51:00",
) -> source.FutoiDailyPair:
    pair, _ = source.parse_futoi_daily_response(
        _payload([_row("FIZ", systime=systime), _row("YUR", systime=systime)]),
        trade_date=DAY,
        route=source.build_futoi_url(DAY),
        retrieved_at_utc=NOW,
    )
    return pair


def test_exact_si_route_and_one_day_query() -> None:
    route = source.build_futoi_url(DAY)
    parsed = urlsplit(route)
    assert parsed.scheme == "https"
    assert parsed.hostname == "apim.moex.com"
    assert parsed.path == "/iss/analyticalproducts/futoi/securities/si.json"
    assert parse_qs(parsed.query) == {
        "from": ["2026-07-29"],
        "till": ["2026-07-29"],
        "latest": ["1"],
    }


def test_route_rejected_before_network() -> None:
    calls = 0

    def opener(*_args: object, **_kwargs: object) -> Response:
        nonlocal calls
        calls += 1
        return Response()

    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.fetch_futoi_bytes(
            "https://apim.moex.com/iss/analyticalproducts/futoi/securities/usdrubf.json?from=2026-07-29&till=2026-07-29&latest=1",
            TOKEN,
            opener=opener,
        )
    assert raised.value.blocker == "provenance_not_sufficient"
    assert calls == 0


def test_named_iss_futoi_block_is_parsed_like_canonical_loader() -> None:
    pair, columns = source.parse_futoi_daily_response(
        _payload([_row("FIZ"), _row("YUR")], block_name="futoi"),
        trade_date=DAY,
        route=source.build_futoi_url(DAY),
        retrieved_at_utc=NOW,
    )
    assert pair.trade_date == DAY
    assert pair.source_ticker == "Si"
    assert tuple(columns) == tuple(COLUMNS)


def test_payload_without_iss_tabular_block_fails_closed() -> None:
    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.parse_futoi_daily_response(
            json.dumps({"metadata": {"version": 1}}).encode("utf-8"),
            trade_date=DAY,
            route=source.build_futoi_url(DAY),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "official_schema_not_stable"


def test_futoi_schema_block_is_selected_when_cursor_precedes_it() -> None:
    cursor = {
        "columns": ["INDEX", "TOTAL", "PAGESIZE"],
        "data": [[0, 2, 100]],
    }
    futoi = {
        "columns": COLUMNS,
        "data": [_row("FIZ"), _row("YUR")],
    }
    payload = json.dumps(
        {"futoi.cursor": cursor, "futoi": futoi}
    ).encode("utf-8")
    pair, columns = source.parse_futoi_daily_response(
        payload,
        trade_date=DAY,
        route=source.build_futoi_url(DAY),
        retrieved_at_utc=NOW,
    )
    assert pair.trade_date == DAY
    assert pair.source_ticker == "Si"
    assert tuple(columns) == tuple(COLUMNS)


def test_malformed_cursor_columns_do_not_abort_valid_futoi_selection() -> None:
    malformed_cursor = {
        "columns": [{}],
        "data": [[0]],
    }
    futoi = {
        "columns": COLUMNS,
        "data": [_row("FIZ"), _row("YUR")],
    }
    payload = json.dumps(
        {"futoi.cursor": malformed_cursor, "futoi": futoi}
    ).encode("utf-8")
    pair, columns = source.parse_futoi_daily_response(
        payload,
        trade_date=DAY,
        route=source.build_futoi_url(DAY),
        retrieved_at_utc=NOW,
    )
    assert pair.trade_date == DAY
    assert pair.source_ticker == "Si"
    assert tuple(columns) == tuple(COLUMNS)


def test_uppercase_mixed_case_futoi_schema_is_normalized() -> None:
    provider_columns = [
        column.upper() if index % 2 == 0 else column.title()
        for index, column in enumerate(COLUMNS)
    ]
    cursor = {
        "columns": ["INDEX", "TOTAL", "PAGESIZE"],
        "data": [[0, 2, 100]],
    }
    futoi = {
        "columns": provider_columns,
        "data": [_row("FIZ"), _row("YUR")],
    }
    payload = json.dumps(
        {"futoi.cursor": cursor, "futoi": futoi}
    ).encode("utf-8")
    pair, columns = source.parse_futoi_daily_response(
        payload,
        trade_date=DAY,
        route=source.build_futoi_url(DAY),
        retrieved_at_utc=NOW,
    )
    assert pair.trade_date == DAY
    assert pair.source_ticker == "Si"
    assert tuple(columns) == tuple(COLUMNS)


def test_duplicate_columns_after_case_normalization_fail_closed() -> None:
    columns = COLUMNS + ["TICKER"]
    payload = json.dumps(
        {
            "futoi": {
                "columns": columns,
                "data": [
                    _row("FIZ") + ["Si"],
                    _row("YUR") + ["Si"],
                ],
            }
        }
    ).encode("utf-8")
    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.parse_futoi_daily_response(
            payload,
            trade_date=DAY,
            route=source.build_futoi_url(DAY),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "official_schema_not_stable"


def test_multiple_unknown_iss_tabular_blocks_fail_closed() -> None:
    table = {"columns": COLUMNS, "data": [_row("FIZ"), _row("YUR")]}
    payload = json.dumps({"futoi": table, "alternate": table}).encode("utf-8")
    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.parse_futoi_daily_response(
            payload,
            trade_date=DAY,
            route=source.build_futoi_url(DAY),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "official_schema_not_stable"


def test_aligned_pair_uses_canonical_normalizer_and_allows_seqnum_difference() -> None:
    pair = _pair()
    assert pair.source_ticker == "Si"
    assert pair.target_security_id == "USDRUBF"
    assert pair.storage_family_code == "USDRUBF"
    assert pair.sess_id == "1"
    assert pair.fiz_seqnum == 101
    assert pair.yur_seqnum == 102
    assert pair.fiz_pos == 10.0
    assert pair.yur_pos == -10.0
    assert pair.source_available_at.isoformat() == "2026-07-29T23:51:00+03:00"


def test_latest_common_pair_does_not_use_independent_latest_group_row() -> None:
    rows = [
        _row("FIZ"),
        _row("YUR"),
        _row(
            "FIZ",
            moment="2026-07-29 23:55:00",
            systime="2026-07-29 23:56:00",
            seqnum=103,
        ),
    ]
    pair, _ = source.parse_futoi_daily_response(
        _payload(rows),
        trade_date=DAY,
        route=source.build_futoi_url(DAY),
        retrieved_at_utc=NOW,
    )
    assert pair.moment.isoformat() == "2026-07-29T23:50:00+03:00"
    assert pair.fiz_seqnum == 101
    assert pair.yur_seqnum == 102


@pytest.mark.parametrize(
    "rows,blocker",
    [
        ([_row("FIZ")], "incomplete_identity_coverage"),
        (
            [_row("FIZ", ticker="USDRUBF"), _row("YUR")],
            "provenance_not_sufficient",
        ),
        (
            [_row("FIZ", ticker="CNY"), _row("YUR", ticker="CNY")],
            "provenance_not_sufficient",
        ),
        (
            [_row("FIZ", ticker=""), _row("YUR", ticker="")],
            "provenance_not_sufficient",
        ),
        (
            [_row("FIZ", sess_id=""), _row("YUR", sess_id="")],
            "official_schema_not_stable",
        ),
        (
            [_row("FIZ", pos=11.0), _row("YUR")],
            "numerical_or_chronology_integrity_failure",
        ),
        (
            [_row("FIZ"), _row("YUR", pos=-9.0, pos_long=81.0, pos_short=-90.0)],
            "numerical_or_chronology_integrity_failure",
        ),
    ],
)
def test_schema_identity_and_numerical_fail_closed(
    rows: list[list[object]],
    blocker: str,
) -> None:
    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.parse_futoi_daily_response(
            _payload(rows),
            trade_date=DAY,
            route=source.build_futoi_url(DAY),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == blocker


def test_empty_provider_payload_fails_closed_before_normalization() -> None:
    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.parse_futoi_daily_response(
            _payload([]),
            trade_date=DAY,
            route=source.build_futoi_url(DAY),
            retrieved_at_utc=NOW,
        )
    assert raised.value.blocker == "incomplete_identity_coverage"


def test_systime_is_exact_pit_availability_and_cutoff_is_inclusive() -> None:
    source.validate_prior_session_pair(
        _pair(),
        target_trade_date=date(2026, 7, 30),
        prior_trade_date=DAY,
    )
    source.validate_prior_session_pair(
        _pair(systime="2026-07-30 06:00:00"),
        target_trade_date=date(2026, 7, 30),
        prior_trade_date=DAY,
    )
    late = _pair(systime="2026-07-30 06:00:01")
    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.validate_prior_session_pair(
            late,
            target_trade_date=date(2026, 7, 30),
            prior_trade_date=DAY,
        )
    assert raised.value.blocker == "point_in_time_cutoff_not_provable"


def test_acceptance_matrix_is_exact_and_has_no_target_fields() -> None:
    eligible = pd.DataFrame(
        [
            {
                "target_trade_date": "2026-07-30",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2026-07-29",
            },
            {
                "target_trade_date": "2026-07-31",
                "target_instrument_id": "forts.usdrubf",
                "prior_trade_date": "2026-07-30",
            },
        ]
    )
    matrix, diagnostics = source.build_futoi_pit_acceptance_matrix(
        eligible,
        [_pair()],
    )
    assert list(matrix.columns) == list(source.ACCEPTANCE_MATRIX_COLUMNS)
    assert not set(matrix.columns) & source.FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
    assert matrix.futoi_trade_date.notna().tolist() == [True, False]
    assert diagnostics.accepted.tolist() == [True, False]
    assert diagnostics.forward_fill_used.eq(False).all()
    assert diagnostics.nearest_date_substitution_used.eq(False).all()


def test_license_gate_is_bound_to_exact_moex_futoi_identity() -> None:
    passed, evidence = source.validate_license_access_evidence({})
    assert passed is False
    assert evidence["blocker"] == "provider_license_and_access_terms_not_documented"

    common = {
        "product": "AlgoPack FUTOI",
        "account_entitlement": True,
        "permitted_research_use": True,
        "permitted_local_raw_storage": True,
        "permitted_derived_feature_use": True,
        "redistribution_policy": "raw redistribution prohibited",
        "evidence_source": "provider terms",
        "verified_at": "2026-07-30T12:00:00Z",
    }
    passed, evidence = source.validate_license_access_evidence(
        {"provider": "unrelated vendor", **common}
    )
    assert passed is False
    assert evidence["provider_identity_verified"] is False

    passed, evidence = source.validate_license_access_evidence(
        {"provider": "MOEX AlgoPack FUTOI", **common}
    )
    assert passed is True
    assert evidence["status"] == "passed"


def test_futoi_adapter_restores_source_specific_404_mapping() -> None:
    headers = Message()
    headers["X-MOEX-Error-Code"] = "ticker-not-found"
    error = HTTPError(
        source.build_futoi_url(DAY),
        404,
        "sanitized",
        headers,
        None,
    )

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise error

    with pytest.raises(source.FutoiSiSourceValidationError) as raised:
        source.fetch_futoi_bytes(
            source.build_futoi_url(DAY),
            TOKEN,
            opener=opener,
        )
    assert raised.value.blocker == "futoi_si_not_available"
    assert TOKEN not in str(raised.value)
