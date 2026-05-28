# Dataset Reference Contract v1

contract_id: dataset_reference_contract.v1
schema_version: v1
artifact_class: dataset_reference
producer: moex_data.datasets
consumer: moex_features / moex_research.runners / moex_backtest.engine

## purpose

Declares a dataset dependency for canonical strategy testing without relying on implicit discovery.

## required_fields

- dataset_ref_id
- dataset_id
- schema_version
- artifact_class
- path_contract_class
- path_contract
- producer
- consumer
- instrument_scope
- timeframe_scope
- source_granularity
- calendar_session_rule_ref
- known_by_when_rule
- data_quality_ref

## validation_rules

- dataset_ref_id must be unique within the strategy test manifest.
- path_contract_class must be one of repo_relative, external_pattern, cli_argument, or env_contract.
- path_contract must be explicit and must not require filesystem guessing.
- instrument_scope and timeframe_scope must be non-empty.
- source_granularity must declare native_d1, derived_from_intraday, or mixed.
- calendar_session_rule_ref must define how trading sessions are resolved.
- known_by_when_rule must state when the dataset is available for ex-ante use.
- data_quality_ref must identify the quality contract or report required before consumption.

## forbidden_patterns

- Generic glob discovery.
- Silent fallback to latest file or newest directory.
- Hardcoded absolute server paths inside strategy, research, or backtest logic.
- Dataset references without known_by_when semantics.
- Using derived labels as source datasets for ex-ante features.
