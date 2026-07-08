from __future__ import annotations

from pathlib import Path

from src.moex_data.futures import phase2_d1_panel_build_plan as plan_module

ROOT = Path(__file__).resolve().parents[2]


def test_phase2_d1_panel_build_plan_imports_and_builds_from_repo_files() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["phase"] == "2.9"
    assert plan["plan_status"] == "blocked_pending_data_build_approval"
    assert plan["source_dataset"] == "futures_raw_5m.v1"
    assert plan["target_panel"] == "usdrubf_phase2_d1_panel.v1"
    assert plan["target_grain"] == "one row per trade_date per canonical instrument"
    assert all(plan["repository_file_presence"].values())
    assert plan["plan_checks"]["required_repository_files_present"] is True


def test_phase2_d1_panel_build_plan_binds_forecast_anchor_and_pit_rules() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["forecast_anchor"] == "06:00 Europe/Moscow"
    assert plan["pit_cutoff_rule"] == "availability_ts_utc <= forecast_anchor_ts"
    assert (
        plan["d1_availability_rule"]
        == "D1 trade_date T available no earlier than T+1 06:00 Europe/Moscow"
    )
    assert "PIT availability validation" in plan["required_gates_before_data_build"]
    assert "output_schema_validation" in plan["required_gates_before_data_build"]


def test_phase2_d1_panel_build_plan_keeps_label_ema_and_cbr_roles_non_leaking() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["label_role"] == "y_only_not_feature"
    assert plan["ema_role"] == "context_diagnostic_only_not_label_source"
    assert plan["cbr_official_usdrub_role"] == "reference_only_not_causal_market_input"


def test_phase2_d1_panel_build_plan_keeps_futoi_and_external_sources_blocked() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["futoi_status"] == "blocked_until_provider_timestamp_schema_revision_policy"
    assert plan["blocked_external_sources"] == [
        "FUTOI source contract",
        "oil",
        "DXY",
        "CNY/RUB proxy",
        "USD/RUB spot proxy",
        "news raw ingestion",
        "news LLM classification",
    ]


def test_phase2_d1_panel_build_plan_authorizes_no_execution_or_generated_paths() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["authorization_flags"] == {
        "can_run_ingestion": False,
        "can_run_backfill": False,
        "can_run_materialization": False,
        "can_compute_features": False,
        "can_train_model": False,
        "can_predict": False,
    }
    assert not any(plan["authorization_flags"].values())
    assert plan["generated_data_path_authorized"] is False
    assert plan["server_path_authorized"] is False
    assert plan["output_files_authorized"] is False
    assert plan["authorized_output_paths"] == []


def test_phase2_d1_panel_build_plan_has_no_runtime_side_effects() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["side_effects"] == {
        "reads_market_data": False,
        "writes_files": False,
        "network_calls": False,
        "subprocess": False,
        "server_access": False,
    }
    assert not any(plan["side_effects"].values())


def test_phase2_d1_panel_build_plan_uses_phase2_8_gate_without_authorizing_data_build() -> None:
    plan = plan_module.build_d1_panel_build_plan(ROOT)

    assert plan["materialization_gate"]["status"] == "blocked_pending_data_build_approval"
    assert plan["materialization_gate"]["gate_passed"] is True
    assert plan["materialization_gate"]["server_apply_authorized"] is False
    assert plan["materialization_gate"]["runtime_authorized"] is False
    assert plan["materialization_gate"]["output_files_authorized"] is False
    assert plan["plan_checks"]["phase2_materialization_gate_passed"] is True
    assert plan["plan_checks"]["explicit_pm_l2_data_build_approval_present"] is False
    assert plan["plan_checks"]["data_build_authorized"] is False


def test_phase2_d1_panel_build_plan_source_is_stdlib_only_and_not_loader_runtime() -> None:
    source = (ROOT / "src/moex_data/futures/phase2_d1_panel_build_plan.py").read_text(
        encoding="utf-8"
    )

    for forbidden in (
        "import pandas",
        "import numpy",
        "read_csv",
        "read_parquet",
        "to_csv",
        "to_parquet",
        "requests",
        "urllib",
        "subprocess.run",
        "raw_5m_loader",
        "resampler",
        "MOEX_ISS",
        "fit(",
        "predict(",
    ):
        assert forbidden not in source


def test_phase2_d1_panel_build_plan_assertion_helper_returns_blocked_plan() -> None:
    plan = plan_module.assert_d1_panel_build_plan_blocked(ROOT)

    assert plan["plan_status"] == "blocked_pending_data_build_approval"
    assert not any(plan["authorization_flags"].values())
    assert not any(plan["side_effects"].values())
