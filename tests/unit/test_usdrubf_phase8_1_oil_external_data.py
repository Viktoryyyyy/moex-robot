from __future__ import annotations

import json
from datetime import date, datetime, time, timezone

import pytest

from src.moex_research.external_data.models import ExternalDataError
from src.moex_research.external_data.oil_markets import (
    MOEX_CANDLE_COLUMNS,
    MOEX_CONTRACT_COLUMNS,
    assert_cme_market_open_at_cutoff,
    build_pre_moex_observation,
    contract_selection_uses_no_volume,
    load_cme_wti_current_quotes,
    parse_cme_wti_contract_calendar,
    parse_cme_wti_current_quotes,
    parse_cme_wti_historical_intraday,
    parse_moex_brent_contracts,
    parse_moex_brent_daily_candles,
    select_contract_for_day,
)
from src.moex_research.external_data.registry import require_phase8_2_ready


RETRIEVED = datetime(2026, 7, 15, 5, 45, tzinfo=timezone.utc)


def _iss(block: str, columns: tuple[str, ...], rows: list[list[object]]) -> bytes:
    return json.dumps({block: {"columns": list(columns), "data": rows}}).encode()


def _contract_payload() -> bytes:
    return _iss(
        "securities",
        MOEX_CONTRACT_COLUMNS,
        [
            ["BRN6", "BR-7.26", "BR", "RFUD", "2026-08-01", "2026-08-01"],
            ["SiU6", "Si-9.26", "Si", "RFUD", "2026-09-17", "2026-09-17"],
        ],
    )


def _candle_payload(rows: list[list[object]] | None = None) -> bytes:
    return _iss(
        "candles",
        MOEX_CANDLE_COLUMNS,
        rows
        if rows is not None
        else [["2026-07-14 00:00:00", "2026-07-14 23:59:59", 69, 71, 68, 70, 1000, 70000]],
    )


def _calendar_payload() -> bytes:
    return json.dumps(
        [
            {
                "contractMonth": "Aug 2026",
                "productCode": "CLQ26",
                "lastTrade": "21 Jul 2026",
                "settlement": "21 Jul 2026",
                "expirationDate": {"dateOnlyLongFormat": "01 Aug 2026"},
            },
            {
                "contractMonth": "Sep 2026",
                "productCode": "CLU26",
                "lastTrade": "20 Aug 2026",
                "settlement": "20 Aug 2026",
                "expirationDate": {"dateOnlyLongFormat": "01 Sep 2026"},
            },
        ]
    ).encode()


def _quote_record(**overrides: object) -> dict[str, object]:
    quote: dict[str, object] = {
        "quoteCode": "CLU6",
        "productCode": "CL",
        "lastTradeDate": "2026-08-20T05:00:00Z",
        "last": "70.50",
        "open": "70.00",
        "high": "71.00",
        "low": "69.50",
        "priorSettle": "69.75",
        "volume": "12345",
        "updated": "2026-07-15T05:34:00Z",
    }
    quote.update(overrides)
    return quote


def _quotes_payload(
    quotes: list[dict[str, object]], *, quote_delay: object = "10 minutes"
) -> bytes:
    return json.dumps(
        {
            "quoteDelayed": True,
            "quoteDelay": quote_delay,
            "tradeDate": "15 Jul 2026",
            "quotes": quotes,
        }
    ).encode()


def _quote_payload(**overrides: object) -> bytes:
    return _quotes_payload([_quote_record(**overrides)])


def test_moex_contract_metadata_and_daily_candles_retain_expiration_and_provenance() -> None:
    contracts = parse_moex_brent_contracts(_contract_payload(), retrieved_at_utc=RETRIEVED)
    assert [item["contract_code"] for item in contracts] == ["BRN6"]
    assert contracts[0]["expiration_date"] == "2026-08-01"

    candles = parse_moex_brent_daily_candles(
        _candle_payload(),
        contract_code="BRN6",
        expiration_date=date(2026, 8, 1),
        retrieved_at_utc=RETRIEVED,
    )
    assert candles[0]["trade_date"] == "2026-07-14"
    assert candles[0]["contract_code"] == "BRN6"
    assert candles[0]["expiration_date"] == "2026-08-01"
    assert candles[0]["historical_model_use_status"] == "blocked_pending_source_validation"


def test_moex_missing_expiration_identity_schema_malformed_numeric_and_empty_fail() -> None:
    missing = _iss(
        "securities",
        MOEX_CONTRACT_COLUMNS,
        [["BRN6", "BR-7.26", "BR", "RFUD", None, "2026-08-01"]],
    )
    with pytest.raises(ExternalDataError, match="expiration_date"):
        parse_moex_brent_contracts(missing, retrieved_at_utc=RETRIEVED)

    bad_columns = _iss("candles", MOEX_CANDLE_COLUMNS[:-1], [])
    with pytest.raises(ExternalDataError, match="columns"):
        parse_moex_brent_daily_candles(
            bad_columns,
            contract_code="BRN6",
            expiration_date=date(2026, 8, 1),
            retrieved_at_utc=RETRIEVED,
        )
    malformed = _candle_payload(
        [["2026-07-14 00:00:00", "2026-07-14 23:59:59", "bad", 71, 68, 70, 1000, 70000]]
    )
    with pytest.raises(ExternalDataError, match="malformed"):
        parse_moex_brent_daily_candles(
            malformed,
            contract_code="BRN6",
            expiration_date=date(2026, 8, 1),
            retrieved_at_utc=RETRIEVED,
        )
    with pytest.raises(ExternalDataError, match="no rows"):
        parse_moex_brent_daily_candles(
            _candle_payload([]),
            contract_code="BRN6",
            expiration_date=date(2026, 8, 1),
            retrieved_at_utc=RETRIEVED,
        )


def test_duplicate_candle_and_mutable_alias_fail_closed() -> None:
    row = ["2026-07-14 00:00:00", "2026-07-14 23:59:59", 69, 71, 68, 70, 1000, 70000]
    with pytest.raises(ExternalDataError, match="duplicate Brent"):
        parse_moex_brent_daily_candles(
            _candle_payload([row, row]),
            contract_code="BRN6",
            expiration_date=date(2026, 8, 1),
            retrieved_at_utc=RETRIEVED,
        )
    with pytest.raises(ExternalDataError, match="mutable"):
        parse_moex_brent_daily_candles(
            _candle_payload(),
            contract_code="current",
            expiration_date=date(2026, 8, 1),
            retrieved_at_utc=RETRIEVED,
        )


def test_contract_selection_uses_expiration_only_and_never_same_day_volume() -> None:
    contracts = [
        {"contract_code": "CLQ26", "expiration_date": "2026-07-21", "volume": 999999},
        {"contract_code": "CLU26", "expiration_date": "2026-08-20", "volume": 1},
    ]
    assert select_contract_for_day(contracts, date(2026, 7, 15)) == "CLU26"
    assert contract_selection_uses_no_volume()


def test_cme_calendar_and_current_quote_parse_with_exact_timezone_conversion() -> None:
    calendar = parse_cme_wti_contract_calendar(
        _calendar_payload(), retrieved_at_utc=RETRIEVED
    )
    expirations = {
        item["contract_code"]: date.fromisoformat(str(item["expiration_date"]))
        for item in calendar
    }
    quotes = parse_cme_wti_current_quotes(
        _quote_payload(), expiration_by_contract=expirations, retrieved_at_utc=RETRIEVED
    )
    assert quotes[0]["observation_timestamp_utc"] == "2026-07-15T05:34:00Z"
    assert quotes[0]["contract_code"] == "CLU26"
    assert quotes[0]["display_quote_code"] == "CLU6"
    assert quotes[0]["observation_timestamp_moscow"] == "2026-07-15T08:34:00+03:00"
    assert quotes[0]["quote_delay_minutes"] == 10
    assert quotes[0]["has_price_observation"] is True
    assert quotes[0]["minutes_since_last_trade"] == 11
    assert quotes[0]["historical_model_use_status"] == "blocked_pending_license"


def test_delayed_quote_exactly_at_effective_availability_boundary_passes() -> None:
    quotes = parse_cme_wti_current_quotes(
        _quote_payload(updated="2026-07-15T05:35:00Z"),
        expiration_by_contract={"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=RETRIEVED,
    )

    result = build_pre_moex_observation(quotes, date(2026, 7, 15))

    assert result["observation_timestamp_moscow"] == "2026-07-15T08:35:00+03:00"


def test_delayed_quote_after_effective_availability_boundary_fails() -> None:
    quotes = parse_cme_wti_current_quotes(
        _quote_payload(updated="2026-07-15T05:35:01Z"),
        expiration_by_contract={"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=RETRIEVED,
    )

    with pytest.raises(ExternalDataError, match="visible"):
        build_pre_moex_observation(quotes, date(2026, 7, 15))


def test_delayed_quote_exactly_at_cutoff_fails() -> None:
    quotes = parse_cme_wti_current_quotes(
        _quote_payload(updated="2026-07-15T05:45:00Z"),
        expiration_by_contract={"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=RETRIEVED,
    )

    with pytest.raises(ExternalDataError, match="visible"):
        build_pre_moex_observation(quotes, date(2026, 7, 15))


def test_delayed_quote_must_also_be_visible_at_snapshot_retrieval() -> None:
    quotes = parse_cme_wti_current_quotes(
        _quote_payload(updated="2026-07-15T05:35:00Z"),
        expiration_by_contract={"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=datetime(2026, 7, 15, 5, 40, tzinfo=timezone.utc),
    )

    with pytest.raises(ExternalDataError, match="visible at retrieval"):
        build_pre_moex_observation(quotes, date(2026, 7, 15))


def test_delayed_quote_semantics_fail_closed_when_changed_or_absent() -> None:
    expirations = {"CLU26": date(2026, 8, 20)}
    with pytest.raises(ExternalDataError, match="delay declaration mismatch"):
        parse_cme_wti_current_quotes(
            _quotes_payload([_quote_record()], quote_delay="15 minutes"),
            expiration_by_contract=expirations,
            retrieved_at_utc=RETRIEVED,
        )

    quotes = parse_cme_wti_current_quotes(
        _quote_payload(), expiration_by_contract=expirations, retrieved_at_utc=RETRIEVED
    )
    del quotes[0]["quote_delay_minutes"]
    with pytest.raises(ExternalDataError, match="delay semantics"):
        build_pre_moex_observation(quotes, date(2026, 7, 15))


def test_contract_roll_does_not_fallback_to_later_available_contract() -> None:
    expirations = {
        "CLQ26": date(2026, 7, 22),
        "CLU26": date(2026, 8, 20),
    }
    quotes = parse_cme_wti_current_quotes(
        _quotes_payload(
            [
                _quote_record(
                    quoteCode="CLQ6",
                    lastTradeDate="2026-07-22T05:00:00Z",
                    updated="2026-07-15T05:36:00Z",
                ),
                _quote_record(updated="2026-07-15T05:35:00Z"),
            ]
        ),
        expiration_by_contract=expirations,
        retrieved_at_utc=datetime(2026, 7, 15, 5, 46, tzinfo=timezone.utc),
    )

    with pytest.raises(ExternalDataError, match="selected CME contract"):
        build_pre_moex_observation(quotes, date(2026, 7, 15))


def test_contract_roll_preserves_nearest_blank_quote_row_and_fails_closed() -> None:
    expirations = {
        "CLQ26": date(2026, 7, 22),
        "CLU26": date(2026, 8, 20),
    }
    quotes = parse_cme_wti_current_quotes(
        _quotes_payload(
            [
                _quote_record(
                    quoteCode="CLQ6",
                    lastTradeDate="2026-07-22T05:00:00Z",
                    last="-",
                    open="-",
                    high="-",
                    low="-",
                ),
                _quote_record(),
            ]
        ),
        expiration_by_contract=expirations,
        retrieved_at_utc=RETRIEVED,
    )

    assert quotes[0]["contract_code"] == "CLQ26"
    assert quotes[0]["has_price_observation"] is False
    with pytest.raises(ExternalDataError, match="selected CME contract"):
        build_pre_moex_observation(quotes, date(2026, 7, 15))


def test_nearest_contract_with_visible_quote_is_selected() -> None:
    expirations = {
        "CLQ26": date(2026, 7, 22),
        "CLU26": date(2026, 8, 20),
    }
    quotes = parse_cme_wti_current_quotes(
        _quotes_payload(
            [
                _quote_record(
                    quoteCode="CLQ6",
                    lastTradeDate="2026-07-22T05:00:00Z",
                    updated="2026-07-15T05:35:00Z",
                ),
                _quote_record(updated="2026-07-15T05:34:00Z"),
            ]
        ),
        expiration_by_contract=expirations,
        retrieved_at_utc=RETRIEVED,
    )

    result = build_pre_moex_observation(quotes, date(2026, 7, 15))

    assert result["contract_code"] == "CLQ26"


def test_pre_moex_contract_selection_remains_independent_of_volume() -> None:
    expirations = {
        "CLQ26": date(2026, 7, 22),
        "CLU26": date(2026, 8, 20),
    }
    quotes = parse_cme_wti_current_quotes(
        _quotes_payload(
            [
                _quote_record(
                    quoteCode="CLQ6",
                    lastTradeDate="2026-07-22T05:00:00Z",
                    volume="1",
                ),
                _quote_record(volume="999999"),
            ]
        ),
        expiration_by_contract=expirations,
        retrieved_at_utc=RETRIEVED,
    )

    result = build_pre_moex_observation(quotes, date(2026, 7, 15))

    assert result["contract_code"] == "CLQ26"


def test_cutoff_excludes_later_observation_and_rejects_full_day_close() -> None:
    expirations = {"CLU26": date(2026, 8, 20)}
    later = parse_cme_wti_current_quotes(
        _quote_payload(updated="2026-07-15T05:46:00Z"),
        expiration_by_contract=expirations,
        retrieved_at_utc=datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc),
    )
    with pytest.raises(ExternalDataError, match="at or before"):
        build_pre_moex_observation(later, date(2026, 7, 15))

    with pytest.raises(ExternalDataError, match="full-day close"):
        parse_cme_wti_current_quotes(
            _quote_payload(close="70.75"),
            expiration_by_contract=expirations,
            retrieved_at_utc=RETRIEVED,
        )


def test_pre_moex_result_has_fixed_cutoff_and_retains_stale_age() -> None:
    quotes = parse_cme_wti_current_quotes(
        _quote_payload(),
        expiration_by_contract={"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=RETRIEVED,
    )
    result = build_pre_moex_observation(quotes, date(2026, 7, 15))
    assert result["cutoff_timestamp_moscow"] == "2026-07-15T08:45:00+03:00"
    assert result["minutes_since_last_trade"] == 11
    assert result["previous_official_settlement"] == 69.75


def test_missing_expiration_duplicate_identity_malformed_numeric_and_empty_fail() -> None:
    with pytest.raises(ExternalDataError, match="missing expiration"):
        parse_cme_wti_current_quotes(
            _quote_payload(), expiration_by_contract={}, retrieved_at_utc=RETRIEVED
        )
    duplicate_root = json.loads(_quote_payload())
    duplicate_root["quotes"].append(dict(duplicate_root["quotes"][0]))
    with pytest.raises(ExternalDataError, match="duplicate CME"):
        parse_cme_wti_current_quotes(
            json.dumps(duplicate_root).encode(),
            expiration_by_contract={"CLU26": date(2026, 8, 20)},
            retrieved_at_utc=RETRIEVED,
        )
    with pytest.raises(ExternalDataError, match="malformed"):
        parse_cme_wti_current_quotes(
            _quote_payload(last="not-a-number"),
            expiration_by_contract={"CLU26": date(2026, 8, 20)},
            retrieved_at_utc=RETRIEVED,
        )
    with pytest.raises(ExternalDataError, match="empty"):
        parse_cme_wti_current_quotes(
            json.dumps(
                {
                    "quoteDelayed": True,
                    "quoteDelay": "10 minutes",
                    "tradeDate": "15 Jul 2026",
                    "quotes": [],
                }
            ).encode(),
            expiration_by_contract={"CLU26": date(2026, 8, 20)},
            retrieved_at_utc=RETRIEVED,
        )


def test_cme_current_loader_uses_official_json_route_and_injected_transport() -> None:
    requested: list[str] = []

    def transport(url: str) -> bytes:
        requested.append(url)
        return _quote_payload()

    records = load_cme_wti_current_quotes(
        {"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=RETRIEVED,
        transport=transport,
    )
    assert requested == ["https://www.cmegroup.com/CmeWS/mvc/quotes/v2/425"]
    assert records[0]["contract_code"] == "CLU26"


def test_holiday_does_not_reuse_future_data_and_cme_is_open_at_normal_cutoff() -> None:
    quote = parse_cme_wti_current_quotes(
        _quote_payload(updated="2026-07-16T05:34:00Z"),
        expiration_by_contract={"CLU26": date(2026, 8, 20)},
        retrieved_at_utc=datetime(2026, 7, 16, 5, 45, tzinfo=timezone.utc),
    )
    with pytest.raises(ExternalDataError, match="at or before"):
        build_pre_moex_observation(quote, date(2026, 7, 15))
    assert_cme_market_open_at_cutoff(date(2026, 7, 15))


def test_winter_monday_cutoff_maps_to_open_sunday_evening_chicago_session() -> None:
    assert_cme_market_open_at_cutoff(date(2026, 1, 12))


def test_sunday_chicago_before_17_is_rejected() -> None:
    with pytest.raises(ExternalDataError, match="Sunday"):
        assert_cme_market_open_at_cutoff(
            date(2026, 1, 12), cutoff_local_time=time(1, 59)
        )


def test_saturday_chicago_is_rejected() -> None:
    with pytest.raises(ExternalDataError, match="Saturday"):
        assert_cme_market_open_at_cutoff(
            date(2026, 1, 10), cutoff_local_time=time(21, 0)
        )


def test_friday_chicago_at_or_after_16_is_rejected() -> None:
    with pytest.raises(ExternalDataError, match="Friday"):
        assert_cme_market_open_at_cutoff(
            date(2026, 1, 10), cutoff_local_time=time(1, 0)
        )


def test_monday_through_thursday_maintenance_break_is_rejected() -> None:
    with pytest.raises(ExternalDataError, match="maintenance"):
        assert_cme_market_open_at_cutoff(
            date(2026, 1, 13), cutoff_local_time=time(1, 30)
        )


def test_normal_weekday_chicago_session_passes() -> None:
    assert_cme_market_open_at_cutoff(
        date(2026, 1, 14), cutoff_local_time=time(21, 0)
    )


def test_historical_parser_is_isolated_and_license_blocked_source_cannot_be_ready() -> None:
    with pytest.raises(ExternalDataError, match="blocked_pending_license"):
        parse_cme_wti_historical_intraday(
            json.dumps({"dataset": "CME_DataMine_Market_by_Order", "licensed": False}).encode()
        )
    with pytest.raises(ExternalDataError, match="blocked_pending_license"):
        require_phase8_2_ready("cme_wti_pre_moex")


def test_licensed_historical_parser_is_strict_and_diagnostic_only() -> None:
    payload = json.dumps(
        {
            "dataset": "CME_DataMine_Market_by_Order",
            "licensed": True,
            "rows": [
                {
                    "contract_code": "CLU26",
                    "event_timestamp_utc": "2026-07-15T05:34:00Z",
                    "price": "70.50",
                    "quantity": "2",
                }
            ],
        }
    ).encode()
    result = parse_cme_wti_historical_intraday(payload)
    assert result[0]["historical_model_use_status"] == "diagnostic_only"
    assert result[0]["contract_code"] == "CLU26"
