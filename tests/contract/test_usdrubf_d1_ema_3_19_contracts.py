from __future__ import annotations

import importlib
import json
from pathlib import Path

CONTRACT_FILES = [
    Path("contracts/features/usdrubf_d1_ohlc_from_5m.json"),
    Path("contracts/features/usdrubf_d1_ema_3_19_cross_context.json"),
    Path("contracts/labels/usdrubf_d1_ema_3_19_cross_labels.json"),
]

CONFIG_FILES = [
    Path("configs/features/usdrubf_d1_ohlc_from_5m.json"),
    Path("configs/features/usdrubf_d1_ema_3_19_cross_context.json"),
]

ALLOWED_CONTRACT_CLASSES = {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
FORBIDDEN_MARKERS = ("latest", "current", "autodetect")
FORBIDDEN_SCOPE_MARKERS = (
    "src/strategies/usdrubf_ema_3_19_d1_ai_filter/",
    "configs/strategies/usdrubf_ema_3_19_d1_ai_filter.json",
    "live_adapter",
    "broker",
    "runtime scheduler",
)


def _load(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _walk_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        out: list[str] = []
        for item in value.values():
            out.extend(_walk_strings(item))
        return out
    if isinstance(value, list):
        out = []
        for item in value:
            out.extend(_walk_strings(item))
        return out
    return []


def test_contract_json_files_validate_required_shape() -> None:
    for path in CONTRACT_FILES:
        payload = _load(path)
        assert payload["artifact_id"]
        assert payload["artifact_role"] in {"feature", "label"}
        assert payload["contract_class"] in ALLOWED_CONTRACT_CLASSES
        assert list(payload.keys()).count("contract_class") == 1
        assert payload["producer_ref"]
        assert payload["consumer_refs"]
        assert payload["format"]
        assert payload["schema_version"] == 1
        assert payload["partitioning"] == "unpartitioned"


def test_config_json_files_validate_registry_shape() -> None:
    for path in CONFIG_FILES:
        payload = _load(path)
        assert payload["feature_id"]
        assert payload["instrument_ids"] == ["usdrubf"]
        assert payload["granularity"] == "D1"
        assert payload["artifact_ref"].startswith("contracts/features/")
        assert payload["producer_ref"]
        assert payload["calendar_contract"] == "moex_futures"
        assert payload["timezone"] == "Europe/Moscow"
        assert payload["is_research_allowed"] is True
        assert payload["is_runtime_allowed"] is False
        assert payload["status"] == "active"


def test_contracts_do_not_use_forbidden_paths_or_dynamic_markers() -> None:
    for path in CONTRACT_FILES + CONFIG_FILES:
        text_values = _walk_strings(_load(path))
        for value in text_values:
            lowered = value.lower()
            assert not value.startswith("/")
            assert not value.startswith("~/")
            assert "\\" not in value
            assert ".." not in value.split("/")
            assert not any(marker in lowered for marker in FORBIDDEN_MARKERS)
            assert not any(marker in lowered for marker in FORBIDDEN_SCOPE_MARKERS)


def test_context_contract_declares_required_event_feature_columns() -> None:
    payload = _load(Path("contracts/features/usdrubf_d1_ema_3_19_cross_context.json"))
    required = set(payload["required_columns"])

    assert {
        "instrument_id",
        "end",
        "open",
        "high",
        "low",
        "close",
        "ema3",
        "ema19",
        "ema_diff",
        "ema_diff_prev",
        "cross_dir",
        "bars_since_prev_cross",
        "ret_1d",
        "ret_3d",
        "ret_5d",
        "rolling_vol_5d",
        "rolling_vol_20d",
    }.issubset(required)
    assert payload["ema_alpha_formula"] == "2/(window+1)"
    assert payload["warmup_full_d1_bars_before_first_event"] == 19


def test_label_contract_declares_research_only_future_outcome_policy() -> None:
    payload = _load(Path("contracts/labels/usdrubf_d1_ema_3_19_cross_labels.json"))
    policy = payload["label_policy"]

    assert policy["scope"] == "research_only"
    assert policy["runtime_consumption_allowed"] is False
    assert policy["earliest_outcome_anchor"] == "D+1 open"
    assert policy["future_outcomes_only"] is True
    assert policy["labels_must_not_enter_feature_rows"] is True
    assert policy["allow_trade_h5_source_label"] == "signed_ret_o2o_h5"
    assert policy["allow_trade_h5_positive_only"] is True


def test_new_modules_import_successfully() -> None:
    for module_name in (
        "src.moex_features.daily.usdrubf_d1_ohlc_from_5m",
        "src.moex_features.daily.usdrubf_d1_ema_3_19_cross_context",
        "src.moex_research.labels.usdrubf_d1_ema_3_19_cross_labels",
    ):
        assert importlib.import_module(module_name)
