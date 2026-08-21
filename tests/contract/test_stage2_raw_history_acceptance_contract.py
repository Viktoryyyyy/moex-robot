from pathlib import Path

from moex_data.futures.contract_io import load_simple_yaml_mapping


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "contracts/datasets/futures_raw_history_acceptance.v1.yaml"
CONFIG = ROOT / "configs/datasets/futures_data_lake.v1.yaml"
RUNBOOK = ROOT / "docs/data/futures_raw_history_acceptance.md"
GATE = ROOT / "src/moex_data/futures/stage2_raw_history_acceptance_gate.py"
VALIDATOR = ROOT / "src/moex_data/futures/stage2_raw_history_acceptance.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_raw_history_acceptance_is_registered_and_fail_closed() -> None:
    contract = _text(CONTRACT)
    config = _text(CONFIG)
    runbook = _text(RUNBOOK)
    gate = _text(GATE)
    validator = _text(VALIDATOR)
    loaded = load_simple_yaml_mapping(
        ROOT, "contracts/datasets/futures_raw_history_acceptance.v1.yaml"
    )

    assert loaded["dataset_id"] == "futures_raw_history_acceptance"
    assert "raw_history_acceptance: contracts/datasets/futures_raw_history_acceptance.v1.yaml" in config
    assert "producer: moex_data.futures.stage2_raw_history_acceptance_gate" in contract
    assert "preexisting_accepted_pointer_absent: true" in contract
    assert "this_gate_writes_accepted_pointer: false" in contract
    assert "network_access_allowed: false" in contract
    assert "historical_backfill_allowed: false" in contract
    assert "implicit_partition_discovery_allowed: false" in contract
    assert "automatic_date_selection_allowed: false" in contract
    assert "public_iss_transport_allowed: false" in contract
    assert "systime_as_raw_identity_allowed: false" in contract
    assert "evidence_written: true" in contract

    assert "preexisting canonical accepted pointer must be absent" in gate
    assert "acceptance report already exists for explicit run_id" in gate
    assert "write_accepted_manifest_pointer" not in gate
    assert "backfill_range(" not in gate
    assert "materialize_instrument_partition(" not in validator
    assert "materialize_futoi_partition(" not in validator
    assert "requests." not in validator
    assert "def main(" not in validator
    assert "partition contains missing stored identity" in validator
    assert "partition stored identity mismatch" in validator
    assert "quote partition ts date mismatch" in validator
    assert "FUTOI availability timestamp precedes source publication timestamp" in validator

    assert "accepted_pointer_ready: false" in config
    assert "scheduler_ready: false" in config
    assert "d1_materialization_ready: false" in config
    assert "research_ready: false" in config
    assert "Fixed current-expiry `Si` and `CR` quote contracts are reference-only" in runbook
    assert "Pointer promotion is a separate later step" in runbook
