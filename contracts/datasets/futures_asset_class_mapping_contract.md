# futures_asset_class_mapping_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_asset_class_mapping.v1

purpose: Canonical asset class classification for MOEX futures families and instruments.
producer: futures_asset_class_mapping_builder
consumer:
- futures_slice1_universe_selector
- futures_liquidity_screen_builder
- futures_duplicate_exposure_group_builder

path_pattern: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/futures_asset_class_mapping.parquet
primary_key:
- mapping_id
- snapshot_id
- board
- secid

required_fields:
- mapping_id
- snapshot_id
- snapshot_date
- board
- secid
- family_code
- asset_class
- mapping_source
- schema_version

nullable_fields:
- underlying
- asset_group
- override_reason
- review_notes

status_fields:
- mapping_status
- override_status
- validation_status

validation_rules:
- asset_class must be one of currency, index, equity, commodity, rates, crypto, other, unknown.
- each active instrument must have one asset_class per snapshot_id.
- manual overrides must include override_reason.

blocking_conditions:
- duplicate primary key.
- active non-excluded instrument has unknown asset_class without review_notes.
