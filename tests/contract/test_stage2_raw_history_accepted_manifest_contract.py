from pathlib import Path

from moex_data.futures.contract_io import load_simple_yaml_mapping


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "contracts/datasets/futures_raw_history_accepted_manifest.v1.yaml"


def test_raw_history_accepted_manifest_contract_loads_with_production_loader() -> None:
    values = load_simple_yaml_mapping(
        ROOT, "contracts/datasets/futures_raw_history_accepted_manifest.v1.yaml"
    )
    assert values["contract_id"] == "futures_raw_history_accepted_manifest.v1"
    assert values["dataset_id"] == "futures_raw_history_accepted_manifest"
    assert values["schema_version"] == "futures_raw_history_accepted_manifest.v1"
    assert values["producer"] == "moex_data.futures.stage2_raw_history_promotion"
    assert values["promotion_policy"]["acceptance_status_required"] == "pass"
    assert values["promotion_policy"]["pointer_create_only_required"] is True
    assert values["execution_boundary"]["network_access_allowed"] is False
    assert CONTRACT.is_file()
