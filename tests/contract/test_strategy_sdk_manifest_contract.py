def test_strategy_sdk_manifest_contract_smoke():
    from moex_strategy_sdk.manifest import REQUIRED_MANIFEST_FIELDS
    assert "strategy_id" in REQUIRED_MANIFEST_FIELDS
