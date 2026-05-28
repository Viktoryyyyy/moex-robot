# Label Contract v1

contract_id: label_contract.v1
schema_version: v1
artifact_class: label_contract
producer: moex_features.labels / moex_research
consumer: moex_research.runners / moex_research.registry / PM review

## purpose

Declares outcome labels and separates statistical research labels from execution-compatible labels.

## required_fields

- label_ref_id
- label_id
- schema_version
- artifact_class
- producer
- consumer
- dataset_refs
- label_columns
- label_class
- primary_research
- secondary_execution_compatible
- event_anchor
- outcome_window
- known_by_when
- anti_leakage_invariants

## validation_rules

- label_ref_id must be unique within the strategy test manifest.
- label_class must explicitly declare primary_research or secondary_execution_compatible.
- primary_research labels may measure event-anchored post-event outcomes for statistical testing.
- secondary_execution_compatible labels must use the earliest executable point and must not be reported as the primary hypothesis result.
- event_anchor and outcome_window must be explicit and session-indexed where relevant.
- known_by_when must state that labels are not available to ex-ante feature or signal generation.
- anti_leakage_invariants must prohibit label leakage into features, signals, or execution decisions.

## forbidden_patterns

- Blending primary_research and secondary_execution_compatible labels in one ambiguous field.
- Treating delayed execution-compatible labels as the primary statistical answer.
- Using future outcome columns as model inputs.
- Omitting event/setup/outcome timing semantics.
- Inferring executable performance from research-only labels.
