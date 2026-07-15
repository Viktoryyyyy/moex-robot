from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.moex_research.external_data.cbr import (
    DAILY_KEY_RATE_HEADERS,
    KEY_RATE_HEADERS,
    KEY_RATE_ROUTE,
    LIQUIDITY_NUMBER_HEADERS,
    LIQUIDITY_REQUIRED_MARKERS,
    RUONIA_HEADERS,
    load_key_rate_daily,
    load_ruonia_daily,
    parse_banking_liquidity_html,
    parse_key_rate_html,
    parse_ruonia_html,
)
from src.moex_research.external_data.models import ExternalDataError


RETRIEVED = datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc)
KEY_RATE_WIDE_HEADERS = (
    *KEY_RATE_HEADERS,
    "Refinancing rate",
    "Overnight loans",
)


def _table(headers: tuple[str, ...], rows: list[tuple[object, ...]], prefix: str = "") -> bytes:
    header = "".join(f"<th>{cell}</th>" for cell in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"<html>{prefix}<table><tr>{header}</tr>{body}</table></html>".encode()


def _ruonia_row(**overrides: object) -> tuple[object, ...]:
    values: list[object] = [
        "14.07.2026",
        "14.42",
        "483.45",
        "57",
        "23",
        "13.05",
        "14.00",
        "14.50",
        "14.75",
        "Standard",
        "15.07.2026",
    ]
    indexes = {
        "observation_date": 0,
        "ruonia_rate_pct": 1,
        "transaction_volume_rub_bn": 2,
        "publication_date": 10,
    }
    for key, value in overrides.items():
        values[indexes[key]] = value
    return tuple(values)


def _liquidity_payload(rows: list[tuple[object, ...]]) -> bytes:
    markers = "".join(
        f"<tr><th>{marker}</th></tr>" for marker in LIQUIDITY_REQUIRED_MARKERS
    )
    header = "".join(f"<th>{cell}</th>" for cell in LIQUIDITY_NUMBER_HEADERS)
    body = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"<html><table>{markers}<tr>{header}</tr>{body}</table></html>".encode()


def test_canonical_ruonia_parses_and_preserves_publication_date() -> None:
    result = parse_ruonia_html(_table(RUONIA_HEADERS, [_ruonia_row()]), retrieved_at_utc=RETRIEVED)

    assert result[0]["observation_date"] == "2026-07-14"
    assert result[0]["publication_date"] == "2026-07-15"
    assert result[0]["ruonia_rate_pct"] == 14.42
    assert result[0]["historical_model_use_status"] == "candidate_for_phase8_2"
    assert len(result[0]["raw_payload_sha256"]) == 64


def test_ruonia_publication_before_observation_fails_closed() -> None:
    payload = _table(
        RUONIA_HEADERS,
        [_ruonia_row(publication_date="13.07.2026")],
    )
    with pytest.raises(ExternalDataError, match="precedes"):
        parse_ruonia_html(payload, retrieved_at_utc=RETRIEVED)


def test_ruonia_malformed_numeric_and_missing_field_fail_closed() -> None:
    malformed = _table(RUONIA_HEADERS, [_ruonia_row(ruonia_rate_pct="secret")])
    with pytest.raises(ExternalDataError, match="malformed"):
        parse_ruonia_html(malformed, retrieved_at_utc=RETRIEVED)

    missing_headers = RUONIA_HEADERS[:-1]
    with pytest.raises(ExternalDataError, match="columns"):
        parse_ruonia_html(
            _table(missing_headers, [_ruonia_row()[:-1]]), retrieved_at_utc=RETRIEVED
        )


def test_duplicate_ruonia_identity_fails_closed() -> None:
    payload = _table(RUONIA_HEADERS, [_ruonia_row(), _ruonia_row()])
    with pytest.raises(ExternalDataError, match="duplicate RUONIA"):
        parse_ruonia_html(payload, retrieved_at_utc=RETRIEVED)


def test_official_key_rate_change_history_parses_and_sorts_effective_dates() -> None:
    payload = _table(
        KEY_RATE_WIDE_HEADERS,
        [
            ("15.07.2026", "14.25", "8.25", "15.25"),
            ("01.06.2026", "13.0", "8.25", "14.0"),
        ],
    )
    result = parse_key_rate_html(payload, retrieved_at_utc=RETRIEVED)
    assert [item["effective_date"] for item in result] == [
        "2026-06-01",
        "2026-07-15",
    ]
    assert [item["key_rate_pct"] for item in result] == [13.0, 14.25]
    assert result[0]["source_revision_status"] == "official_change_date_history"
    assert result[0]["historical_model_use_status"] == "candidate_for_phase8_2"


def test_daily_key_rate_date_rate_response_is_rejected() -> None:
    payload = _table(DAILY_KEY_RATE_HEADERS, [("15.07.2026", "14.25")])
    with pytest.raises(ExternalDataError, match="daily Date / Rate"):
        parse_key_rate_html(payload, retrieved_at_utc=RETRIEVED)


def test_key_rate_duplicate_effective_date_fails() -> None:

    duplicate = _table(
        KEY_RATE_HEADERS,
        [("15.07.2026", "14.25"), ("15.07.2026", "14.25")],
    )
    with pytest.raises(ExternalDataError, match="duplicate key-rate"):
        parse_key_rate_html(duplicate, retrieved_at_utc=RETRIEVED)


def test_key_rate_malformed_effective_date_and_rate_fail() -> None:
    with pytest.raises(ExternalDataError, match="effective_date"):
        parse_key_rate_html(
            _table(KEY_RATE_HEADERS, [("not-a-date", "14.25")]),
            retrieved_at_utc=RETRIEVED,
        )
    with pytest.raises(ExternalDataError, match="key_rate_pct"):
        parse_key_rate_html(
            _table(KEY_RATE_HEADERS, [("15.07.2026", "not-a-rate")]),
            retrieved_at_utc=RETRIEVED,
        )


def test_consecutive_identical_key_rate_change_points_fail() -> None:
    payload = _table(
        KEY_RATE_HEADERS,
        [("01.06.2026", "14.25"), ("15.07.2026", "14.25")],
    )
    with pytest.raises(ExternalDataError, match="identical rates"):
        parse_key_rate_html(payload, retrieved_at_utc=RETRIEVED)


def test_banking_liquidity_parses_required_fields_and_stays_vintage_blocked() -> None:
    row = (
        "15.07.2026",
        "2,243.2",
        "1,856.4",
        "7,339",
        "6,526.9",
        "0",
        "0.8",
        "811.3",
        "5,715",
        "0",
        "5,715",
        "0",
        "232.3",
        "4,893.6",
        "5,280.4",
    )
    result = parse_banking_liquidity_html(
        _liquidity_payload([row]), retrieved_at_utc=RETRIEVED
    )

    assert result[0]["liquidity_deficit_surplus_rub_bn"] == 2243.2
    assert result[0]["liquidity_deficit_surplus_ex_correspondent_accounts_rub_bn"] == 1856.4
    assert result[0]["bank_correspondent_accounts_rub_bn"] == 4893.6
    assert result[0]["required_reserves_averaging_rub_bn"] == 5280.4
    assert result[0]["source_revision_status"] == "latest_revised"
    assert result[0]["historical_model_use_status"] == "blocked_pending_vintage_policy"


def test_empty_requested_interval_and_schema_change_fail_closed() -> None:
    with pytest.raises(ExternalDataError, match="no rows"):
        parse_key_rate_html(_table(KEY_RATE_HEADERS, []), retrieved_at_utc=RETRIEVED)
    changed = tuple("changed" if item == "Key rate" else item for item in KEY_RATE_HEADERS)
    with pytest.raises(ExternalDataError, match="columns"):
        parse_key_rate_html(_table(changed, [("15.07.2026", "14.25")]), retrieved_at_utc=RETRIEVED)


def test_key_rate_loader_retains_exact_change_history_route_and_provenance() -> None:
    seen: list[str] = []

    def transport(url: str) -> bytes:
        seen.append(url)
        return _table(KEY_RATE_HEADERS, [("03.02.2014", "5.5")])

    result = load_key_rate_daily(
        date(2014, 2, 3),
        date(2026, 7, 15),
        retrieved_at_utc=RETRIEVED,
        transport=transport,
    )
    assert seen[0].startswith(KEY_RATE_ROUTE)
    assert "UniDbQuery.From=03.02.2014" in seen[0]
    assert result[0]["source_route"] == seen[0]
    assert result[0]["source_revision_status"] == "official_change_date_history"
    assert len(result[0]["raw_payload_sha256"]) == 64


def test_loader_is_mockable_and_builds_explicit_interval_url() -> None:
    seen: list[str] = []

    def transport(url: str) -> bytes:
        seen.append(url)
        return _table(RUONIA_HEADERS, [_ruonia_row()])

    result = load_ruonia_daily(
        date(2026, 7, 14),
        date(2026, 7, 15),
        retrieved_at_utc=RETRIEVED,
        transport=transport,
    )
    assert result[0]["observation_date"] == "2026-07-14"
    assert result[0]["source_route"] == seen[0]
    assert "UniDbQuery.From=14.07.2026" in seen[0]
    assert "UniDbQuery.To=15.07.2026" in seen[0]
