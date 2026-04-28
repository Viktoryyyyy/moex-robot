# futures_expiration_map_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_expiration_map.v1

purpose: Versioned expiration map for ordinary expiring MOEX futures contracts.
producer: futures_expiration_map_builder
consumer:
- futures_continuous_series_builder
- futures_history_depth_screen_builder
- futures_slice1_universe_selector

path_pattern: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={registry_snapshot_date}/futures_expiration_map.parquet
primary_key:
- expiration_map_id
- registry_snapshot_date
- board
- secid

required_fields:
- expiration_map_id
- registry_snapshot_date
- snapshot_id
- board
- secid
- family_code
- is_perpetual
- expiration_status
- schema_version

nullable_fields:
- expiration_date
- last_trade_date
- first_trade_date
- sentinel_date
- decision_source
- review_notes

status_fields:
- expiration_status
- validation_status
- review_status

validation_rules:
- primary key must preserve versioning by expiration_map_id and registry_snapshot_date.
- ordinary expiring futures require expiration_date or last_trade_date.
- perpetual futures must not be forced into ordinary roll logic.
- sentinel/technical dates must be preserved as reviewed evidence when used.

blocking_conditions:
- duplicate primary key.
- ordinary active contract lacks usable expiration evidence.
- perpetual instrument is assigned ordinary expiration without explicit override.
