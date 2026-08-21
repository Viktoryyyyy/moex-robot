from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import stage2_raw_history_acceptance as acceptance
from moex_data.futures import stage2_raw_history_acceptance_gate as gate


def _quote_pointer_contract(repo: Path) -> None:
    contract = repo / acceptance.QUOTE_CONTRACT_PATH
    contract.parent.mkdir(parents=True, exist_ok=True)
    contract.write_text(
        "\n".join(
            [
                "dataset_id: futures_raw_5m",
                'accepted_pointer_path_contract: "${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _quote_expectation(secid: str = "USDRUBF") -> acceptance.HistoryExpectation:
    return acceptance.HistoryExpectation(
        target_dataset_id=acceptance.QUOTE_DATASET_ID,
        instrument_id="usdrubf_futures_family",
        source_id=acceptance.QUOTE_SOURCE_ID,
        date_start="2026-08-17",
        date_end="2026-08-17",
        expected_partitions=1,
        expected_rows=1,
        expected_secid=secid,
    )


def _futoi_expectation() -> acceptance.HistoryExpectation:
    return acceptance.HistoryExpectation(
        target_dataset_id=acceptance.FUTOI_DATASET_ID,
        instrument_id="si_futures_family",
        source_id=acceptance.FUTOI_SOURCE_ID,
        date_start="2026-08-17",
        date_end="2026-08-17",
        expected_partitions=1,
        expected_rows=2,
        expected_secid="SiU6",
        expected_source_ticker="si",
        expected_missing_dates=0,
    )


def test_preexisting_accepted_pointer_blocks_before_history_audit(tmp_path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    _quote_pointer_contract(repo)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    pointer = (
        data_root
        / "state/datasets/dataset_id=futures_raw_5m/instrument_id=usdrubf_futures_family/current_accepted_manifest.json"
    )
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text("{}\n", encoding="utf-8")

    def should_not_run(**kwargs):
        raise AssertionError("history audit must not run when a pointer already exists")

    monkeypatch.setattr(gate.acceptance, "audit_history", should_not_run)
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="must be absent"):
        gate.run_gate(
            repo_root=repo,
            target_dataset_id="futures_raw_5m",
            instrument_id="usdrubf_futures_family",
            run_id="blocked_pointer",
        )


def test_preexisting_acceptance_report_blocks_before_history_audit(tmp_path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    _quote_pointer_contract(repo)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    report = data_root / "state/acceptance/existing/acceptance_report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text('{"immutable": true}\n', encoding="utf-8")

    monkeypatch.setattr(gate.acceptance, "acceptance_report_path", lambda **kwargs: report)

    def should_not_run(**kwargs):
        raise AssertionError("history audit must not run when acceptance evidence already exists")

    monkeypatch.setattr(gate.acceptance, "audit_history", should_not_run)
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="already exists"):
        gate.run_gate(
            repo_root=repo,
            target_dataset_id="futures_raw_5m",
            instrument_id="usdrubf_futures_family",
            run_id="existing_report",
        )

    assert report.read_text(encoding="utf-8") == '{"immutable": true}\n'


def test_quote_registry_binding_rejects_coverage_secid_drift(tmp_path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    registry = repo / gate.quote_stage2_backfill.REGISTRY_PATH
    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(
        """registry_id: forts_instrument_registry.v1
instruments:
  - instrument_id: usdrubf_futures_family
    source_id: moex_algopack_fo_tradestats_5m
    secid: USDRUBF
    evidence_status: pilot_passed
    enabled_for_raw_5m_materialization: false
rules:
  family_partition_key_allowed: false
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        gate.acceptance,
        "_expectation",
        lambda *args, **kwargs: _quote_expectation("WRONG"),
    )

    with pytest.raises(
        acceptance.RawHistoryAcceptanceError,
        match="quote secid evidence does not match registry binding",
    ):
        gate._require_quote_registry_binding(repo, "usdrubf_futures_family")


def test_quote_grid_rejects_off_grid_timestamp(tmp_path, monkeypatch) -> None:
    path = tmp_path / "part.parquet"
    pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-08-17 10:14:00"]),
            "value": [800.0],
            "num_trades": [5],
        }
    ).to_parquet(path, index=False)
    monkeypatch.setattr(gate.acceptance, "_contract_path", lambda *args: "unused")
    monkeypatch.setattr(gate.acceptance, "_date_range", lambda *args: ("2026-08-17",))
    monkeypatch.setattr(gate.acceptance, "_partition_path", lambda **kwargs: path)

    failures = gate._quote_grid_failures(Path("."), _quote_expectation())

    assert len(failures) == 1
    assert "not aligned to 5-minute grid" in failures[0]["error"]


def test_quote_optional_activity_rejects_nonnumeric_stored_value(tmp_path, monkeypatch) -> None:
    path = tmp_path / "part.parquet"
    pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-08-17 10:15:00"]),
            "value": ["corrupt"],
            "num_trades": [5],
        }
    ).to_parquet(path, index=False)
    monkeypatch.setattr(gate.acceptance, "_contract_path", lambda *args: "unused")
    monkeypatch.setattr(gate.acceptance, "_date_range", lambda *args: ("2026-08-17",))
    monkeypatch.setattr(gate.acceptance, "_partition_path", lambda **kwargs: path)

    failures = gate._quote_grid_failures(Path("."), _quote_expectation())

    assert len(failures) == 1
    assert "nonnumeric optional activity: value" in failures[0]["error"]


def test_futoi_clgroup_rejects_noncanonical_stored_spelling(tmp_path, monkeypatch) -> None:
    path = tmp_path / "part.parquet"
    pd.DataFrame({"clgroup": ["FIZ", " fiz "]}).to_parquet(path, index=False)
    monkeypatch.setattr(gate.acceptance, "_contract_path", lambda *args: "unused")
    monkeypatch.setattr(gate.acceptance, "_date_range", lambda *args: ("2026-08-17",))
    monkeypatch.setattr(gate.acceptance, "_partition_path", lambda **kwargs: path)

    failures = gate._futoi_clgroup_failures(Path("."), _futoi_expectation())

    assert len(failures) == 1
    assert "canonical stored values FIZ or YUR" in failures[0]["error"]


def test_pointer_created_during_audit_blocks_before_evidence_write(tmp_path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    _quote_pointer_contract(repo)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    pointer = (
        data_root
        / "state/datasets/dataset_id=futures_raw_5m/instrument_id=usdrubf_futures_family/current_accepted_manifest.json"
    )
    report = data_root / "state/acceptance/race/acceptance_report.json"

    monkeypatch.setattr(gate.acceptance, "acceptance_report_path", lambda **kwargs: report)
    monkeypatch.setattr(
        gate,
        "_require_quote_registry_binding",
        lambda *args, **kwargs: _quote_expectation(),
    )
    monkeypatch.setattr(gate, "_quote_grid_failures", lambda *args, **kwargs: ())

    def audit_and_promote(**kwargs):
        pointer.parent.mkdir(parents=True, exist_ok=True)
        pointer.write_text("{}\n", encoding="utf-8")
        return {
            "acceptance_report_reference": report.as_posix(),
            "acceptance_status": "pass",
            "failed_partition_dates": [],
            "hard_check_failures": [],
        }

    monkeypatch.setattr(gate.acceptance, "audit_history", audit_and_promote)

    with pytest.raises(
        acceptance.RawHistoryAcceptanceError, match="appeared during raw history acceptance"
    ):
        gate.run_gate(
            repo_root=repo,
            target_dataset_id="futures_raw_5m",
            instrument_id="usdrubf_futures_family",
            run_id="pointer_race",
        )

    assert not report.exists()
