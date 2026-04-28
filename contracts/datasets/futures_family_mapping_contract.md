# futures_family_mapping_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_family_mapping.v1

purpose: Canonical mapping from MOEX futures secid/contract codes to futures family codes.
producer: futures_family_mapping_builder
consumer:
- futures_asset_class_mapping_builder
- futures_expiration_map_builder
- futures_slice1_universe_selector
- futures_data_lake_loaders

path_pattern: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/futures_family_mapping.parquet
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
- mapping_source
- schema_version

nullable_fields:
- contract_code
- underlying
- override_reason
- review_notes

status_fields:
- mapping_status
- override_status
- validation_status

validation_rules:
- each active secid must map to no more than one family_code per snapshot_id.
- manual overrides must include override_reason.
- mapping_source must be derived_rule, manual_override, or unresolved.

blocking_conditions:
- duplicate primary key.
- active non-excluded instrument has unresolved family_code.
