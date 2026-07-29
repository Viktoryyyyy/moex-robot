from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pandas as pd

from moex_research.external_data import moex_cnyrubf_algopack_history as source
from moex_research.runners import (
    usdrubf_phase8_6a_algopack_cnyrubf_source_validation as runner,
)
from moex_research.runners.usdrubf_phase8_6a_algopack_cnyrubf_nullable_im_policy import (
    install_nullable_initial_margin_policy,
)


NOW = datetime(2026, 7, 29, 10, 0, tzinfo=timezone.utc)
TRADE_DATE = date(2026, 6, 10)


def _payload_with_missing_initial_margin() -> bytes:
    values: dict[str, object] = {
        "tradedate": TRADE_DATE.isoformat(),
        "tradetime": "09:05:00",
        "secid": source.SECURITY_ID,
        "asset_code": source.ASSET_CODE,
        "pr_open": 12.0,
        "pr_high": 12.2,
        "pr_low": 11.9,
        "pr_close": 12.1,
        "pr_std": 0.1,
        "vol": 10.0,
        "val": 120.0,
        "trades": 5,
        "pr_vwap": 12.0,
        "pr_change": 0.0,
        "trades_b": 3,
        "trades_s": 2,
        "val_b": 72.0,
        "val_s": 48.0,
        "vol_b": 6.0,
        "vol_s": 4.0,
        "disb": 0.0,
        "pr_vwap_b": 12.0,
        "pr_vwap_s": 12.0,
        "im": None,
        "oi_open": 100.0,
        "oi_high": 110.0,
        "oi_low": 90.0,
        "oi_close": 105.0,
        "sec_pr_open": 12.0,
        "sec_pr_high": 12.2,
        "sec_pr_low": 11.9,
        "sec_pr_close": 12.1,
        "SYSTIME": "2026-06-10 09:05:03",
    }
    row = [values[column] for column in source._TRADESTAT_COLUMNS]
    return json.dumps(
        {
            "data": {
                "columns": list(source._TRADESTAT_COLUMNS),
                "data": [row],
            },
            "data.cursor": {
                "columns": ["INDEX", "TOTAL", "PAGESIZE"],
                "data": [[0, 1, 1000]],
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")


def test_missing_initial_margin_is_preserved_as_null() -> None:
    install_nullable_initial_margin_policy()
    rows, _, _, digest = source.parse_tradestats_page_response(
        _payload_with_missing_initial_margin(),
        from_date=TRADE_DATE,
        till_date=TRADE_DATE,
        start=0,
        route=source.build_tradestats_url(TRADE_DATE, TRADE_DATE),
        retrieved_at_utc=NOW,
    )
    assert len(rows) == 1
    assert rows[0].initial_margin is None

    candles = source.aggregate_daily_tradestats(
        rows,
        source_route=source.build_tradestats_url(TRADE_DATE, TRADE_DATE),
        retrieved_at_utc=NOW,
        raw_payload_sha256=digest,
    )
    assert len(candles) == 1
    assert candles[0].initial_margin_close is None


def test_coverage_does_not_require_initial_margin() -> None:
    install_nullable_initial_margin_policy()
    row = {column: 1 for column in runner.ACCEPTANCE_MATRIX_COLUMNS}
    row.update(
        {
            "target_trade_date": "2026-06-11",
            "target_instrument_id": "forts.usdrubf",
            "prior_trade_date": "2026-06-10",
            "cnyrubf_initial_margin_close": None,
        }
    )
    matrix = pd.DataFrame([row], columns=runner.ACCEPTANCE_MATRIX_COLUMNS)
    validation = matrix.loc[:, runner.IDENTITY_COLUMNS].copy()

    coverage = runner._coverage(matrix, validation).iloc[0]
    assert int(coverage.eligible_covered_count) == 1
    assert int(coverage.validation_covered_count) == 1


def test_numeric_gate_ignores_only_initial_margin() -> None:
    install_nullable_initial_margin_policy()
    frame = pd.DataFrame(
        [{"close": 12.1, "initial_margin_close": None}]
    )
    assert runner._finite(frame, ("close", "initial_margin_close")) is True

    frame.loc[0, "close"] = None
    assert runner._finite(frame, ("close", "initial_margin_close")) is False
