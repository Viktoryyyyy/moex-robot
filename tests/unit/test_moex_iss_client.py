from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from moex_research.external_data.models import ExternalDataError
from moex_research.external_data.moex_iss import (
    MOEX_ISS_HOST,
    MoexIssClient,
    MoexIssClientError,
    RetryPolicy,
    parse_iss_block,
    require_utc,
    validate_official_route,
)


def _payload(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def test_client_retries_only_exact_transient_error_on_same_route() -> None:
    route = "https://iss.moex.com/iss/securities/BRQ4.json"
    calls: list[str] = []
    delays: list[float] = []

    def transport(url: str) -> bytes:
        calls.append(url)
        if len(calls) < 3:
            raise ExternalDataError("external-data request failed")
        return b"ok"

    client = MoexIssClient(
        retry_policy=RetryPolicy(5, (0.5, 1.0, 2.0, 4.0)),
        transport=transport,
        sleeper=delays.append,
    )

    assert client.fetch(route) == b"ok"
    assert calls == [route, route, route]
    assert delays == [0.5, 1.0]


def test_client_preserves_exhausted_route_attempts_and_cause() -> None:
    route = "https://iss.moex.com/iss/securities/BRQ4.json"
    cause = ExternalDataError("external-data request failed")

    def transport(_url: str) -> bytes:
        raise cause

    client = MoexIssClient(
        retry_policy=RetryPolicy(2, (0.0,)),
        transport=transport,
        sleeper=lambda _delay: None,
    )

    with pytest.raises(ExternalDataError, match=r"official route=.*attempts=2") as raised:
        client.fetch(route)
    assert raised.value.__cause__ is cause


def test_client_does_not_retry_semantic_failure() -> None:
    calls = 0

    def transport(_url: str) -> bytes:
        nonlocal calls
        calls += 1
        raise ExternalDataError("response is not valid UTF-8 JSON")

    client = MoexIssClient(
        retry_policy=RetryPolicy(5, (0.5, 1.0, 2.0, 4.0)),
        transport=transport,
        sleeper=lambda _delay: pytest.fail("semantic errors must not retry"),
    )

    with pytest.raises(ExternalDataError, match="valid UTF-8 JSON"):
        client.fetch("https://iss.moex.com/iss/securities/BRQ4.json")
    assert calls == 1


def test_client_now_utc_uses_injected_clock() -> None:
    now = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
    client = MoexIssClient(
        retry_policy=RetryPolicy(1, ()),
        transport=lambda _url: b"unused",
        sleeper=lambda _delay: None,
        clock=lambda: now,
    )
    assert client.now_utc() == now


def test_route_validation_supports_exact_and_prefix_constraints() -> None:
    exact = "https://iss.moex.com/iss/securities/CNYRUB_TOM.json?iss.meta=off"
    assert validate_official_route(
        exact,
        expected_path="/iss/securities/CNYRUB_TOM.json",
    ) == {"iss.meta": "off"}

    prefixed = "https://iss.moex.com/iss/securities/BRQ4.json?iss.meta=off"
    assert validate_official_route(
        prefixed,
        allowed_path_prefix="/iss/securities/",
    ) == {"iss.meta": "off"}

    with pytest.raises(MoexIssClientError) as raised:
        validate_official_route(
            exact.replace(MOEX_ISS_HOST, "example.com"),
            expected_path="/iss/securities/CNYRUB_TOM.json",
        )
    assert raised.value.reason == "route_not_allowlisted"


def test_route_validation_requires_one_path_constraint() -> None:
    route = "https://iss.moex.com/iss/securities/BRQ4.json"
    with pytest.raises(ValueError, match="exactly one"):
        validate_official_route(route)
    with pytest.raises(ValueError, match="exactly one"):
        validate_official_route(
            route,
            expected_path="/iss/securities/BRQ4.json",
            allowed_path_prefix="/iss/securities/",
        )


@pytest.mark.parametrize(
    "url",
    [
        "http://iss.moex.com/iss/securities/BRQ4.json",
        "https://user@iss.moex.com/iss/securities/BRQ4.json",
        "https://iss.moex.com:444/iss/securities/BRQ4.json",
        "https://iss.moex.com/iss/other/BRQ4.json",
    ],
)
def test_route_validation_refuses_nonofficial_identity(url: str) -> None:
    with pytest.raises(MoexIssClientError) as raised:
        validate_official_route(url, allowed_path_prefix="/iss/securities/")
    assert raised.value.reason == "route_not_allowlisted"


def test_block_parser_preserves_schema_rows_root_and_digest() -> None:
    payload = _payload(
        {
            "candles": {
                "columns": ["close", "open", "extra"],
                "data": [[11.5, 11.0, "kept"]],
            }
        }
    )
    block = parse_iss_block(
        payload,
        block_name="candles",
        required_columns=("open", "close"),
    )

    assert block.columns == ("close", "open", "extra")
    assert block.rows == [{"close": 11.5, "open": 11.0, "extra": "kept"}]
    assert block.root["candles"]["data"] == [[11.5, 11.0, "kept"]]
    assert block.raw_payload_sha256 == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"not-json", "invalid_json"),
        (_payload({}), "missing_block"),
        (_payload({"x": {"columns": "bad", "data": []}}), "malformed_columns"),
        (_payload({"x": {"columns": ["a"], "data": []}}), "missing_required_columns"),
        (_payload({"x": {"columns": ["a", "b"], "data": "bad"}}), "malformed_data"),
        (_payload({"x": {"columns": ["a", "b"], "data": [[1]]}}), "row_width_mismatch"),
    ],
)
def test_block_parser_returns_structured_failure_reason(payload: bytes, reason: str) -> None:
    with pytest.raises(MoexIssClientError) as raised:
        parse_iss_block(payload, block_name="x", required_columns=("a", "b"))
    assert raised.value.reason == reason
    assert raised.value.block_name == "x"


def test_utc_validation_requires_timezone_aware_explicit_utc() -> None:
    value = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
    assert require_utc(value) == value

    with pytest.raises(MoexIssClientError) as naive:
        require_utc(value.replace(tzinfo=None))
    assert naive.value.reason == "timestamp_not_timezone_aware"

    with pytest.raises(MoexIssClientError) as offset:
        require_utc(value.astimezone(timezone(timedelta(hours=3))))
    assert offset.value.reason == "timestamp_not_expressed_in_utc"


def test_retry_policy_is_self_consistent() -> None:
    with pytest.raises(ValueError, match="delay count"):
        RetryPolicy(3, (0.5,))
    with pytest.raises(ValueError, match="positive"):
        RetryPolicy(0, ())
    with pytest.raises(ValueError, match="non-negative"):
        RetryPolicy(2, (-1.0,))
