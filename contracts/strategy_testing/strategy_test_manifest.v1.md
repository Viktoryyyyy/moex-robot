# Strategy Test Manifest Contract v1

contract_id: strategy_test_manifest.v1
schema_version: v1
artifact_class: strategy_test_manifest
producer: moex_strategy_sdk / moex_research.runners
consumer: moex_research.registry / moex_backtest.engine / PM review

## purpose

Declares one canonical strategy testing package before any research runner, metrics engine, or backtest engine consumes it.

## required_fields

- strategy_test_id
- strategy_id
- strategy_version
- test_type
- instrument_scope
- timeframe_scope
- dataset_refs
- feature_refs
- label_refs
- signal_refs
- backtest_semantics_ref
- cost_slippage_ref
- artifact_contract_ref
- runtime_live_allowed=false

## validation_rules

- strategy_test_id must be stable and unique inside the registry scope.
- strategy_id and strategy_version must match the strategy manifest.
- test_type must explicitly distinguish research-only, canonical_backtest, or validation-only use.
- instrument_scope and timeframe_scope must be explicit non-empty lists.
- dataset_refs, feature_refs, label_refs, and signal_refs must reference declared contract documents or registry records.
- backtest_semantics_ref must point to canonical_backtest_semantics.v1 or a later approved version.
- cost_slippage_ref must point to an approved cost_slippage_contract.
- artifact_contract_ref must point to the expected result artifact contract.
- runtime_live_allowed must be exactly false in this contract skeleton.

## forbidden_patterns

- Implicit use of latest dataset, latest run, or latest artifact folder.
- Any runtime/live permission inferred from positive research metrics.
- Strategy test manifests that omit anti-leakage references.
- Strategy test manifests that combine promotion verdicts with registry metrics.
- Server absolute paths embedded as undeclared dependencies.
