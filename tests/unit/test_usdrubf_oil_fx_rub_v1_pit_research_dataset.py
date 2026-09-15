from __future__ import annotations

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from moex_research.runners.usdrubf_oil_fx_rub_v1_pit_research_dataset import (
    BRENT_READY_STATUS,
    CNYRUBF_READY_STATUS,
    OilFxRubDatasetError,
    build_research_dataset,
    validate_brent_admission,
)


def _contract() -> dict:
    return {
        "contract_identity": {
            "contract_id": "usdrubf_oil_fx_rub_v1_pit_research_dataset",
            "contract_version": "1.0",
            "project": "MOEX_Bot",
            "task_id": "STRAT_OIL_FX_RUB_V1_02B_RESEARCH_DATASET",
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


def _identities() -> pd.DataFrame:
    targets = pd.bdate_range("2024-08-05", periods=472)
    priors = pd.bdate_range("2024-08-02", periods=472)
    return pd.DataFrame(
        {
            "target_trade_date": targets.strftime("%Y-%m-%d"),
            "target_instrument_id": "forts.usdrubf",
            "prior_trade_date": priors.strftime("%Y-%m-%d"),
        }
    )


def _brent() -> pd.DataFrame:
    frame = _identities()
    x = np.arange(len(frame), dtype=float)
    frame["brent_contract_code"] = "BRX4"
    frame["brent_trade_date"] = frame["prior_trade_date"]
    frame["brent_open"] = 70.0 + x * 0.01
    frame["brent_high"] = frame["brent_open"] + 1.0
    frame["brent_low"] = frame["brent_open"] - 1.0
    frame["brent_close"] = frame["brent_open"] + 0.2
    frame["brent_volume"] = 1000.0 + x
    return frame


def _cnyrubf() -> pd.DataFrame:
    frame = _identities()
    x = np.arange(len(frame), dtype=float)
    frame["cnyrubf_security_id"] = "CNYRUBF"
    frame["cnyrubf_trade_date"] = frame["prior_trade_date"]
    frame["cnyrubf_open"] = 11.0 + x * 0.001
    frame["cnyrubf_high"] = frame["cnyrubf_open"] + 0.05
    frame["cnyrubf_low"] = frame["cnyrubf_open"] - 0.05
    frame["cnyrubf_close"] = frame["cnyrubf_open"] + 0.01
    frame["cnyrubf_volume"] = 5000.0 + x
    frame["cnyrubf_volume_imbalance"] = 0.1
    frame["cnyrubf_open_interest_close"] = 100000.0 + x
    return frame


def _d1() -> pd.DataFrame:
    dates = pd.bdate_range("2024-08-05", periods=490)
    return pd.DataFrame(
        {"trade_date": dates.strftime("%Y-%m-%d"), "close": 80.0 + np.arange(490)}
    )


def test_brent_only_passes_with_admitted_brent_and_no_cny() -> None:
    features, labels, gates = build_research_dataset(
        contract=_contract(),
        identity_panel=_identities(),
        brent_matrix=_brent(),
        brent_gate_results=_gates(BRENT_READY_STATUS),
        usdrubf_d1=_d1(),
        mode="brent_only",
    )
    assert len(features) == 472
    assert len(labels) == 472
    assert gates["G9_final"]["passed"] is True
    assert gates["G3_cnyrubf_admission"]["status"] is None


def test_full_mode_refuses_failed_cnyrubf_gate() -> None:
    cny_gates = _gates(CNYRUBF_READY_STATUS)
    cny_gates["G4_test"]["passed"] = False
    with pytest.raises(OilFxRubDatasetError, match="failed gate"):
        build_research_dataset(
            contract=_contract(),
            identity_panel=_identities(),
            brent_matrix=_brent(),
            brent_gate_results=_gates(BRENT_READY_STATUS),
            usdrubf_d1=_d1(),
            mode="oil_fx_full",
            cnyrubf_matrix=_cnyrubf(),
            cnyrubf_gate_results=cny_gates,
        )


def test_full_mode_requires_exact_cnyrubf_candidate_status() -> None:
    with pytest.raises(OilFxRubDatasetError, match="post-fix source status"):
        build_research_dataset(
            contract=_contract(),
            identity_panel=_identities(),
            brent_matrix=_brent(),
            brent_gate_results=_gates(BRENT_READY_STATUS),
            usdrubf_d1=_d1(),
            mode="oil_fx_full",
            cnyrubf_matrix=_cnyrubf(),
            cnyrubf_gate_results=_gates("moex_algopack_cnyrubf_source_not_ready"),
        )


def test_upstream_gate_inventory_must_be_exact_g1_to_g9() -> None:
    brent_gates = _gates(BRENT_READY_STATUS)
    del brent_gates["G3_test"]
    with pytest.raises(OilFxRubDatasetError, match="exactly G1-G9"):
        validate_brent_admission(brent_gates)


def test_authority_boundary_widening_is_rejected() -> None:
    contract = deepcopy(_contract())
    contract["authority_boundary"]["model_fit_allowed"] = True
    with pytest.raises(OilFxRubDatasetError, match="authority boundary was widened"):
        build_research_dataset(
            contract=contract,
            identity_panel=_identities(),
            brent_matrix=_brent(),
            brent_gate_results=_gates(BRENT_READY_STATUS),
            usdrubf_d1=_d1(),
            mode="brent_only",
        )
