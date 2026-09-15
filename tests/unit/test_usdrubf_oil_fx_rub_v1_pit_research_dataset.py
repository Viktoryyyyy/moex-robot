from __future__ import annotations

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from moex_research.features.oil_fx_rub_v1_features import prepare_identity_panel
from moex_research.runners import usdrubf_phase6_internal_modeling_dataset_builder as phase6_builder
from moex_research.runners.usdrubf_oil_fx_rub_v1_pit_research_dataset import (
    BRENT_READY_STATUS,
    EXPECTED_IMMUTABLE_SHA256,
    OilFxRubDatasetError,
    build_research_dataset,
    validate_brent_admission,
    validate_immutable_hashes,
    validate_phase6_source_panel_replay,
)


def _contract() -> dict:
    return {
        "contract_identity": {
            "contract_id": "usdrubf_oil_fx_rub_v1_pit_research_dataset",
            "contract_version": "1.0",
            "project": "MOEX_Bot",
            "task_id": "STRAT_OIL_FX_RUB_V1_02B_RESEARCH_DATASET",
        },
        "staged_modes": {
            "brent_only": {
                "authorized_now": True,
                "cnyrubf_input_allowed": False,
            },
            "oil_fx_full": {"authorized_now": False},
        },
        "approved_file_scope": {
            "existing_files_to_modify": [],
            "scope_widening_allowed": False,
        },
        "authority_boundary": {
            "direct_main_write_allowed": False,
            "merge_allowed": False,
            "server_apply_allowed": False,
            "real_external_data_acquisition_allowed": False,
            "accepted_upstream_artifact_mutation_allowed": False,
            "model_fit_allowed": False,
            "strategy_promotion_allowed": False,
            "broker_action_allowed": False,
            "trading_allowed": False,
        },
    }


def _gates(status: str) -> dict:
    result = {f"G{i}_test": {"passed": True} for i in range(1, 9)}
    result["G9_final_source_readiness"] = {
        "passed": True,
        "failed_gates": [],
        "status": status,
        "blocker_classification": None,
    }
    return result


def _phase6_source_panel() -> pd.DataFrame:
    dates = pd.bdate_range("2024-08-02", periods=491)
    x = np.arange(len(dates), dtype=float)
    close = 80.0 + x * 0.05
    return pd.DataFrame(
        {
            "trade_date": dates.strftime("%Y-%m-%d"),
            "instrument_id": "forts.usdrubf",
            "open": close - 0.02,
            "high": close + 0.10,
            "low": close - 0.10,
            "close": close,
            "volume": 1000.0 + x,
            "value": 80000.0 + x * 10.0,
            "num_trades": 100.0 + x,
        }
    )


def _frozen_modeling_dataset(panel: pd.DataFrame) -> pd.DataFrame:
    prepared = phase6_builder._prepare_internal_d1_panel(panel)
    diagnostic = phase6_builder._add_past_only_diagnostics(prepared)
    features = phase6_builder._build_feature_frame(diagnostic)
    targets = pd.DataFrame(
        {
            "target_trade_date": prepared["trade_date"],
            "target_instrument_id": prepared["instrument_id"],
            "target_phase_label": None,
            "target_is_labeled": False,
            "target_source": "manual_phase_labels_v1",
        }
    )
    targets.loc[1:472, "target_phase_label"] = "B"
    targets.loc[1:472, "target_is_labeled"] = True
    return pd.concat([targets, features], axis=1)


def _brent(modeling: pd.DataFrame) -> pd.DataFrame:
    frame = prepare_identity_panel(modeling)
    x = np.arange(len(frame), dtype=float)
    frame["brent_contract_code"] = "BRX4"
    frame["brent_trade_date"] = frame["prior_trade_date"]
    frame["brent_open"] = 70.0 + x * 0.01
    frame["brent_high"] = frame["brent_open"] + 1.0
    frame["brent_low"] = frame["brent_open"] - 1.0
    frame["brent_close"] = frame["brent_open"] + 0.2
    frame["brent_volume"] = 1000.0 + x
    return frame


def test_brent_only_passes_after_lineage_and_hash_admission() -> None:
    panel = _phase6_source_panel()
    modeling = _frozen_modeling_dataset(panel)
    validate_phase6_source_panel_replay(modeling, panel)
    features, labels, gates = build_research_dataset(
        contract=_contract(),
        frozen_modeling_dataset=modeling,
        phase6_source_panel=panel,
        brent_matrix=_brent(modeling),
        brent_gate_results=_gates(BRENT_READY_STATUS),
        phase6_lineage_verified=True,
        brent_artifacts_verified=True,
        mode="brent_only",
    )
    assert len(features) == 472
    assert len(labels) == 472
    assert gates["G9_final"]["passed"] is True


def test_oil_fx_full_remains_blocked_even_if_caller_requests_it() -> None:
    panel = _phase6_source_panel()
    modeling = _frozen_modeling_dataset(panel)
    with pytest.raises(OilFxRubDatasetError, match="not authorized by current contract"):
        build_research_dataset(
            contract=_contract(),
            frozen_modeling_dataset=modeling,
            phase6_source_panel=panel,
            brent_matrix=_brent(modeling),
            brent_gate_results=_gates(BRENT_READY_STATUS),
            phase6_lineage_verified=True,
            brent_artifacts_verified=True,
            mode="oil_fx_full",
        )


def test_immutable_upstream_hashes_are_exactly_pinned() -> None:
    validate_immutable_hashes(dict(EXPECTED_IMMUTABLE_SHA256))
    bad = dict(EXPECTED_IMMUTABLE_SHA256)
    bad["brent_pit_acceptance_matrix"] = "0" * 64
    with pytest.raises(OilFxRubDatasetError, match="hash mismatch"):
        validate_immutable_hashes(bad)


def test_phase6_source_panel_replay_detects_changed_price_history() -> None:
    panel = _phase6_source_panel()
    modeling = _frozen_modeling_dataset(panel)
    changed = panel.copy()
    changed.loc[100, "close"] += 1.0
    changed.loc[100, "high"] = max(changed.loc[100, "high"], changed.loc[100, "close"])
    with pytest.raises(OilFxRubDatasetError, match="replay mismatch"):
        validate_phase6_source_panel_replay(modeling, changed)


def test_gate_inventory_and_authority_widening_fail_closed() -> None:
    gates = _gates(BRENT_READY_STATUS)
    del gates["G3_test"]
    with pytest.raises(OilFxRubDatasetError, match="exactly G1-G9"):
        validate_brent_admission(gates)

    contract = deepcopy(_contract())
    contract["authority_boundary"]["model_fit_allowed"] = True
    panel = _phase6_source_panel()
    modeling = _frozen_modeling_dataset(panel)
    with pytest.raises(OilFxRubDatasetError, match="authority boundary was widened"):
        build_research_dataset(
            contract=contract,
            frozen_modeling_dataset=modeling,
            phase6_source_panel=panel,
            brent_matrix=_brent(modeling),
            brent_gate_results=_gates(BRENT_READY_STATUS),
            phase6_lineage_verified=True,
            brent_artifacts_verified=True,
            mode="brent_only",
        )
