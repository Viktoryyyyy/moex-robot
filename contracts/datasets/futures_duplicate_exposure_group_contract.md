# futures_duplicate_exposure_group_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_duplicate_exposure_group.v1

purpose: Declarative grouping of futures families/instruments with duplicate or highly overlapping economic exposure.
producer: futures_duplicate_exposure_group_builder
consumer:
- futures_slice1_universe_selector
- futures_data_lake_pm_review

path_pattern: ${MOEX_DATA_ROOT}/futures/screens/snapshot_date={snapshot_date}/futures_duplicate_exposure_group.parquet
primary_key:
- duplicate_exposure_group_id
- snapshot_date
- board
- secid

required_fields:
- duplicate_exposure_group_id
- snapshot_date
- board
- secid
- family_code
- exposure_group_code
- primary_candidate
- schema_version

nullable_fields:
- exposure_description
- duplicate_reason
- preferred_family_code
- review_notes

status_fields:
- grouping_status
- validation_status
- review_status

validation_rules:
- one exposure_group_code may contain multiple instruments or families.
- at most one primary_candidate should be true per exposure_group_code unless explicitly reviewed.
- duplicate exposure grouping must not delete raw registry rows.

blocking_conditions:
- duplicate primary key.
- Slice 1 selects multiple instruments from same exposure_group_code without explicit review.
