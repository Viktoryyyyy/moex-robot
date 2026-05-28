# Signal Contract v1

contract_id: signal_contract.v1
schema_version: v1
artifact_class: signal_contract
producer: strategies
consumer: moex_backtest.engine / moex_runtime.orchestrator / moex_research.runners

## purpose

Declares deterministic strategy signal outputs and their time availability boundary.

## required_fields

- signal_ref_id
- signal_id
- schema_version
- artifact_class
- producer
- consumer
- strategy_id
- strategy_version
- feature_refs
- signal_timestamp_rule
- known_by_when
- signal_value_schema
- position_intent_schema
- anti_leakage_invariants

## validation_rules

- signal_ref_id must be unique within the strategy test manifest.
- strategy_id and strategy_version must match the strategy manifest.
- feature_refs must point to approved feature_contract entries.
- signal_timestamp_rule must define which bar/session the signal belongs to.
- known_by_when must define the earliest backtest/runtime decision point.
- signal_value_schema must define allowed values and null behavior.
- position_intent_schema must define whether the signal is directional, flat, or sized.
- anti_leakage_invariants must prove that labels and future bars are unavailable to signal generation.

## forbidden_patterns

- Signal generation with file IO, network calls, or artifact path discovery.
- Signals timestamped after outcome data but executed as if known earlier.
- Signal math inside CLI loops or runtime orchestration.
- Silent coercion of missing features into tradable signals.
