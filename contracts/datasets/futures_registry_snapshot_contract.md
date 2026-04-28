# futures_registry_snapshot_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_registry_snapshot.v1

purpose: Immutable raw snapshot of the canonical ISS futures securities registry.
producer: futures_registry_snapshot_loader
consumer:
- futures_normalized_instrument_registry_builder
- futures_family_mapping_builder
- futures_asset_class_mapping_builder
- futures_expiration_map_builder
- futures_algopack_availability_probe
- futures_liquidity_screen_builder
- futures_history_depth_screen_builder

path_pattern: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/futures_registry_snapshot.parquet
primary_key:
- snapshot_id
- board
- secid

required_fields:
- snapshot_id
- snapshot_date
- source_system
- source_endpoint_id
- engine
- market
- board
- secid
- shortname
- secname
- raw_payload_json

nullable_fields:
- last_trade_date
- expiration_date
- asset_code
- lot_size
- price_step
- price_step_value
- currency

status_fields:
- registry_status
- ingest_status
- validation_status

validation_rules:
- primary_key must be unique.
- snapshot_date must be ISO date.
- source_system must equal MOEX_ISS.
- board is required in primary key unless a later stronger repo uniqueness rule supersedes this contract.
- raw_payload_json must preserve source registry row.

blocking_conditions:
- missing secid, board, snapshot_id, or snapshot_date.
- duplicate primary key.
- invalid or absent source payload.
