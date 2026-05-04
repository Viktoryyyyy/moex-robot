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
- futures_continuous_roll_map_builder

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
- decision_source
- schema_version

nullable_fields:
- expiration_date
- last_trade_date
- first_trade_date
- sentinel_date
- review_notes

status_fields:
- expiration_status
- validation_status
- review_status
- decision_source

allowed_values:
  decision_source:
  - registry_expiration_date
  - registry_last_trade_date_fallback
  - manual_reviewed_override
  - unresolved

expiration_anchor_rule:
- roll_anchor_date uses expiration_date when expiration_date is non-null, validated, and decision_source=registry_expiration_date.
- roll_anchor_date uses last_trade_date when expiration_date is unavailable, last_trade_date is non-null and validated, and decision_source=registry_last_trade_date_fallback.
- roll_anchor_date may use a reviewed override only when decision_source=manual_reviewed_override and review_notes explain the override.
- unresolved ordinary contracts must use decision_source=unresolved and must block ordinary continuous roll-map publication.
- perpetual futures must not be forced into ordinary roll logic and must be handled by the consuming continuous roll-map contract as identity when applicable.

validation_rules:
- primary key must preserve versioning by expiration_map_id and registry_snapshot_date.
- decision_source is mandatory and must be one of the allowed enum values.
- ordinary expiring futures require expiration_date or last_trade_date with decision_source registry_expiration_date, registry_last_trade_date_fallback, or manual_reviewed_override.
- decision_source=registry_expiration_date requires non-null expiration_date.
- decision_source=registry_last_trade_date_fallback requires non-null last_trade_date and null or rejected expiration_date.
- decision_source=manual_reviewed_override requires non-empty review_notes.
- decision_source=unresolved is allowed only as an explicit blocker state, not as a buildable roll anchor.
- perpetual futures must not be forced into ordinary roll logic.
- sentinel/technical dates must be preserved as reviewed evidence when used.

blocking_conditions:
- duplicate primary key.
- ordinary active contract lacks usable expiration evidence.
- ordinary buildable contract has missing or invalid decision_source.
- ordinary buildable contract has decision_source=unresolved.
- perpetual instrument is assigned ordinary expiration without explicit override.
