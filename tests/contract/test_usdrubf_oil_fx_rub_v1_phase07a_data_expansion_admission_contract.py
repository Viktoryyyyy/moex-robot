from __future__ import annotations

import copy
import json
import re
from functools import reduce
from pathlib import Path

import pytest

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission as phase07a


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission.json")


def test_phase07a_contract_is_frozen_and_research_only() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    phase07a._validate_contract(payload)

    assert payload["contract_identity"]["task_id"] == phase07a.TASK_ID
    assert payload["usdrubf_frozen_prefix"]["prefix_start_date"] == "2022-04-26"
    assert payload["usdrubf_frozen_prefix"]["prefix_end_date"] == "2026-10-01"
    assert payload["usdrubf_frozen_prefix"]["accepted_date_count"] == 1143
    assert payload["usdrubf_frozen_prefix"]["raw_row_count"] == 188760
    assert payload["usdrubf_frozen_prefix"]["partition_content_set_sha256"] == phase07a.EXPECTED_PREFIX_CONTENT_SHA256
    assert payload["usdrubf_frozen_prefix"]["target_identity_count"] == 1142

    assert payload["brent_admission"]["minimum_days_to_expiration"] == 7
    assert payload["brent_admission"]["candle_trade_date"] == "exact prior_trade_date"
    assert payload["brent_admission"]["continuous_alias_allowed"] is False
    assert payload["brent_admission"]["cross_contract_return_allowed"] is False

    assert payload["purpose"]["signal_evaluation_allowed"] is False
    assert payload["purpose"]["backtest_allowed"] is False
    assert payload["purpose"]["parameter_optimization_allowed"] is False
    assert payload["purpose"]["strategy_promotion_allowed"] is False
    assert payload["purpose"]["trading_allowed"] is False

    assert payload["runtime_artifacts"] == list(phase07a.DECLARED_OUTPUTS)


def _contract_paths(value, prefix=()):
    """Enumerate canonical fields, independently of the runner's expectations."""
    if isinstance(value, dict):
        for key, child in value.items():
            path = (*prefix, key)
            yield path
            yield from _contract_paths(child, path)


CANONICAL = json.loads(CONTRACT.read_text(encoding="utf-8"))
CONTRACT_PATHS = list(_contract_paths(CANONICAL))


@pytest.mark.parametrize("path", CONTRACT_PATHS, ids=lambda path: ".".join(path))
@pytest.mark.parametrize("mutation", ["missing", "null", "wrong_type", "changed"])
def test_every_frozen_contract_field_fails_closed(path, mutation):
    payload = copy.deepcopy(CANONICAL)
    parent = payload
    for key in path[:-1]:
        parent = parent[key]
    value = parent[path[-1]]
    if mutation == "missing":
        del parent[path[-1]]
    elif mutation == "null":
        parent[path[-1]] = None
    elif mutation == "wrong_type":
        # Numeric equality must not bypass exact JSON types.
        parent[path[-1]] = (
            int(value) if type(value) is bool else
            float(value) if type(value) is int else
            [] if isinstance(value, dict) else {}
        )
    else:
        parent[path[-1]] = (
            not value if type(value) is bool else
            value + 1 if type(value) is int else
            value + " changed" if isinstance(value, str) else
            value[:-1] if isinstance(value, list) else
            {**value, "undeclared_permission": True}
        )
    with pytest.raises(phase07a.Phase07AError, match=re.escape(".".join(path))):
        phase07a._validate_contract(payload)


BOOLEAN_PATHS = [
    path for path in CONTRACT_PATHS
    if type(reduce(dict.__getitem__, path, CANONICAL)) is bool
]


@pytest.mark.parametrize("path", BOOLEAN_PATHS, ids=lambda path: ".".join(path))
@pytest.mark.parametrize("value", [0, 1, "false", "true", None])
def test_contract_booleans_require_literal_json_boolean(path, value):
    payload = copy.deepcopy(CANONICAL)
    parent = payload
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    with pytest.raises(phase07a.Phase07AError, match=re.escape(".".join(path))):
        phase07a._validate_contract(payload)


@pytest.mark.parametrize("payload", [None, [], "", False, 0])
def test_contract_root_requires_object(payload):
    with pytest.raises(phase07a.Phase07AError, match="contract"):
        phase07a._validate_contract(payload)
