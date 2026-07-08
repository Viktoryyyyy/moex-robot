from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]

FEATURE_EXPORT_PATH = REPO_ROOT / "contracts/features/usdrubf_phase2_d1_feature_export_v1.json"
UNIFIED_FEATURE_CONTRACT_PATH = REPO_ROOT / "contracts/features/usdrubf_phase2_unified_external_feature_contract_v1.json"
PIT_VALIDATION_PATH = REPO_ROOT / "contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml"
OHLC_CONTRACT_PATH = REPO_ROOT / "contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml"
EMA_CONTRACT_PATH = REPO_ROOT / "contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml"
CLASSICAL_CONTRACT_PATH = REPO_ROOT / "contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml"
ROLL_CONTRACT_PATH = REPO_ROOT / "contracts/sources/futures/roll_expiry_mapping.v1.yaml"
CBR_CONTRACT_PATH = REPO_ROOT / "contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml"
RU_TAX_CONTRACT_PATH = REPO_ROOT / "contracts/calendars/calendar/ru_tax_periods.v1.yaml"
RU_US_HOLIDAYS_CONTRACT_PATH = REPO_ROOT / "contracts/calendars/calendar/ru_us_holidays.v1.yaml"
FUTOI_POLICY_PATH = REPO_ROOT / "docs/sot/strategies/ema_3_19_ai/phase2_futoi_pit_policy_v1.md"

D1_PIT_CONTRACTS = (
    FEATURE_EXPORT_PATH,
    UNIFIED_FEATURE_CONTRACT_PATH,
    PIT_VALIDATION_PATH,
    OHLC_CONTRACT_PATH,
    EMA_CONTRACT_PATH,
    CLASSICAL_CONTRACT_PATH,
)

UNKNOWN_AVAILABILITY_CONTRACTS = (
    FEATURE_EXPORT_PATH,
    UNIFIED_FEATURE_CONTRACT_PATH,
    PIT_VALIDATION_PATH,
    OHLC_CONTRACT_PATH,
    EMA_CONTRACT_PATH,
    CLASSICAL_CONTRACT_PATH,
    ROLL_CONTRACT_PATH,
    CBR_CONTRACT_PATH,
    RU_TAX_CONTRACT_PATH,
    RU_US_HOLIDAYS_CONTRACT_PATH,
    FUTOI_POLICY_PATH,
)

CALENDAR_CONTRACTS = (
    CBR_CONTRACT_PATH,
    RU_TAX_CONTRACT_PATH,
    RU_US_HOLIDAYS_CONTRACT_PATH,
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _json(path: Path) -> dict[str, Any]:
    payload = json.loads(_read(path))
    assert isinstance(payload, dict), f"{path} must contain a JSON object"
    return payload


def _assert_repo_yaml_subset_parses(path: Path) -> str:
    text = _read(path)
    assert text.strip(), f"{path} is empty"
    stack: list[int] = [-1]
    block_scalar_parent_indent: int | None = None

    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        assert "\t" not in line, f"{path}:{line_no} contains a tab"
        indent = len(line) - len(line.lstrip(" "))
        assert indent % 2 == 0, f"{path}:{line_no} has non-2-space indentation"

        if block_scalar_parent_indent is not None:
            if indent > block_scalar_parent_indent:
                continue
            block_scalar_parent_indent = None

        stripped = line.strip()

        while stack and indent <= stack[-1]:
            stack.pop()
        assert stack, f"{path}:{line_no} invalid indentation stack"

        if stripped.startswith("- "):
            item = stripped[2:].strip()
            assert item, f"{path}:{line_no} has an empty list item"
            continue

        assert ":" in stripped, f"{path}:{line_no} is not a key/value YAML line"
        key, value = stripped.split(":", 1)
        assert re.match(r"^[A-Za-z0-9_./${}-]+$", key), f"{path}:{line_no} invalid key"
        value = value.strip()
        if not value:
            stack.append(indent)
        elif value in {">", "|"}:
            stack.append(indent)
            block_scalar_parent_indent = indent
        elif value[0] in {'"', "'"}:
            assert value[-1] == value[0], f"{path}:{line_no} has an unclosed quoted scalar"

    return text


def _yaml_contract_text(path: Path) -> str:
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError:
        return _assert_repo_yaml_subset_parses(path)

    loaded = yaml.safe_load(_read(path))
    assert isinstance(loaded, dict), f"{path} must parse as a YAML mapping"
    return _read(path)


def _combined_text(*paths: Path) -> str:
    return "\n".join(_read(path) for path in paths).lower()


def _assert_contains_all(text: str, markers: tuple[str, ...] | list[str]) -> None:
    lowered = text.lower()
    for marker in markers:
        assert marker.lower() in lowered, marker


def test_pit_time_fields_and_d1_forecast_anchor_rules_are_declared() -> None:
    _json(FEATURE_EXPORT_PATH)
    _json(UNIFIED_FEATURE_CONTRACT_PATH)
    _yaml_contract_text(PIT_VALIDATION_PATH)

    for path in D1_PIT_CONTRACTS:
        text = _read(path)
        _assert_contains_all(
            text,
            (
                "availability_ts_utc",
                "forecast_anchor_ts",
                "06:00 Europe/Moscow",
            ),
        )

    d1_text = _combined_text(*D1_PIT_CONTRACTS)
    _assert_contains_all(
        d1_text,
        (
            "availability_ts_utc <= forecast_anchor_ts",
            "D1 trade_date T",
            "T+1 06:00 Europe/Moscow",
        ),
    )


def test_unknown_availability_timestamp_is_excluded_or_shifted_by_one_trading_day() -> None:
    for path in UNKNOWN_AVAILABILITY_CONTRACTS:
        text = _read(path).lower()
        assert "unknown" in text or "unresolved" in text, path
        assert "exclude" in text or "not eligible" in text, path
        assert "shift" in text or "one trading day" in text, path
        assert "availability_ts_utc" in text, path


def test_label_leakage_denylist_blocks_labels_intervals_future_targets_and_annotations() -> None:
    export_contract = _json(FEATURE_EXPORT_PATH)
    denylist_text = json.dumps(export_contract["denylist"], sort_keys=True).lower()
    combined_exclusion_text = _combined_text(
        FEATURE_EXPORT_PATH,
        UNIFIED_FEATURE_CONTRACT_PATH,
        PIT_VALIDATION_PATH,
        OHLC_CONTRACT_PATH,
        EMA_CONTRACT_PATH,
        CLASSICAL_CONTRACT_PATH,
        ROLL_CONTRACT_PATH,
        CBR_CONTRACT_PATH,
        RU_TAX_CONTRACT_PATH,
        RU_US_HOLIDAYS_CONTRACT_PATH,
        FUTOI_POLICY_PATH,
    )

    for marker in (
        "phase_label",
        "phase_remaining_sessions",
        "current_regime_ends_within_*",
        "next_regime_if_current_ends",
        "interval_id",
        "interval_start",
        "interval_end",
        "annotation_*",
        "future_return_*",
        "future_volatility_*",
        "future_drawdown_*",
    ):
        assert marker in denylist_text

    for broader_marker in (
        "future returns",
        "future volatility",
        "drawdown",
        "llm classifications",
        "post-fact annotations",
    ):
        assert broader_marker in combined_exclusion_text


def test_ema_3_19_is_context_and_diagnostic_only_not_label_source() -> None:
    ema_text = _read(EMA_CONTRACT_PATH)
    export_text = _read(FEATURE_EXPORT_PATH)
    unified_text = _read(UNIFIED_FEATURE_CONTRACT_PATH)
    combined = "\n".join((ema_text, export_text, unified_text)).lower()

    _assert_contains_all(
        combined,
        (
            "ema 3/19 cross context",
            "diagnostic",
            "not a b/s/out label source",
            "ema as b/s/out label source",
        ),
    )

    assert "EMA as B/S/OUT label source" in ema_text
    assert "EMA as B/S/OUT label source" in export_text


def test_futoi_is_participant_positioning_only_and_not_generic_open_interest() -> None:
    policy_text = _read(FUTOI_POLICY_PATH).lower()
    unified_text = _read(UNIFIED_FEATURE_CONTRACT_PATH).lower()

    assert "futoi means participant positioning" in policy_text
    assert "does not mean generic open interest" in policy_text
    assert "no open-interest interpretation" in policy_text
    assert "futoi.participant_positioning" in unified_text
    assert "future_gate_required" in unified_text


def test_cbr_official_usdrub_is_reference_only_not_causal_market_input() -> None:
    cbr_contract = _yaml_contract_text(CBR_CONTRACT_PATH).lower()

    assert "official_usd_rub_rate_role: reference_only" in cbr_contract
    assert "causal_market_driver_allowed: false" in cbr_contract
    assert "must not be described or used as a" in cbr_contract
    assert "causal market usd/rub input" in cbr_contract
    assert "lagged/reference indicator separate" in cbr_contract
    assert "market usd/rub dynamics" in cbr_contract


def test_calendar_contracts_split_pre_anchor_schedule_from_post_fact_outcome() -> None:
    for path in CALENDAR_CONTRACTS:
        text = _yaml_contract_text(path).lower()
        assert "schedule_known_before_anchor:" in text, path
        assert "post_fact_outcome:" in text, path
        assert "status: allowed_design_only" in text, path
        assert "status: blocked" in text, path
        assert "availability_ts_utc <= forecast_anchor_ts" in text, path


def test_contracts_do_not_authorize_runtime_data_loading_feature_computation_or_modeling() -> None:
    paths = (
        FEATURE_EXPORT_PATH,
        UNIFIED_FEATURE_CONTRACT_PATH,
        PIT_VALIDATION_PATH,
        OHLC_CONTRACT_PATH,
        EMA_CONTRACT_PATH,
        CLASSICAL_CONTRACT_PATH,
        ROLL_CONTRACT_PATH,
        CBR_CONTRACT_PATH,
        RU_TAX_CONTRACT_PATH,
        RU_US_HOLIDAYS_CONTRACT_PATH,
        FUTOI_POLICY_PATH,
    )
    combined = _combined_text(*paths)

    for forbidden in (
        "runtime_authorized: true",
        '"runtime_authorized": true',
        "server_apply_authorized: true",
        '"server_apply_authorized": true',
        "ingestion_authorized: true",
        '"ingestion_authorized": true',
        "feature_computation_authorized: true",
        '"feature_computation_authorized": true',
        "modeling_authorized: true",
        '"modeling_authorized": true',
        'feature_export_generation_authorized": true',
    ):
        assert forbidden not in combined

    _assert_contains_all(
        combined,
        (
            "no data loading",
            "no ingestion",
            "no materialization",
            "no runtime",
            "no feature computation",
            "no modeling",
            "no server apply",
        ),
    )
