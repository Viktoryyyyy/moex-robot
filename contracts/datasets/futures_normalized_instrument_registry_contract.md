# futures_normalized_instrument_registry_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_normalized_instrument_registry.v1

purpose: Normalized registry of MOEX futures instruments derived from raw ISS registry snapshots.
producer: futures_normalized_instrument_registry_builder
consumer:
- futures_family_mapping_builder
- futures_asset_class_mapping_builder
- futures_perpetual_detection_rule_evaluator
- futures_expiration_map_builder
- futures_slice1_universe_selector

path_pattern: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/futures_normalized_instrument_registry.parquet
primary_key:
- snapshot_id
- board
- secid

required_fields:
- snapshot_id
- snapshot_date
- secid
- board
- engine
- market
- shortname
- secname
- family_code
- contract_code
- instrument_kind
- is_perpetual_candidate
- source_snapshot_id
- schema_version

nullable_fields:
- expiration_date
- last_trade_date
- asset_code
- asset_class
- underlying
- lot_size
- price_step
- price_step_value
- currency
- notes

status_fields:
- normalization_status
- mapping_status
- validation_status

validation_rules:
- primary_key must be unique.
- every row must map to exactly one family_code or be explicitly excluded downstream.
- instrument_kind must be one of expiring_future, perpetual_future_candidate, technical, unknown.
- source_snapshot_id must reference futures_registry_snapshot.snapshot_id.

blocking_conditions:
- duplicate primary key.
- missing family_code for active non-technical future.
- missing source_snapshot_id.
