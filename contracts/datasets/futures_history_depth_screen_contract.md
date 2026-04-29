# futures_history_depth_screen_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_history_depth_screen.v1

purpose: History-depth screen for deciding whether a MOEX futures instrument/family has enough data for Slice 1 acquisition.
producer: futures_history_depth_screen_builder
consumer:
- futures_slice1_universe_selector
- futures_data_lake_pm_review

path_pattern: ${MOEX_DATA_ROOT}/futures/screens/snapshot_date={snapshot_date}/futures_history_depth_screen.parquet
primary_key:
- history_depth_screen_id
- snapshot_date
- board
- secid

required_fields:
- history_depth_screen_id
- snapshot_date
- board
- secid
- family_code
- screen_from
- screen_till
- history_depth_status
- schema_version

nullable_fields:
- first_available_date
- last_available_date
- available_trading_days
- required_min_trading_days
- missing_days
- threshold_profile_id
- review_notes

status_fields:
- history_depth_status
- validation_status
- review_status

validation_rules:
- threshold_profile_id must match futures_history_depth_thresholds_config when thresholds are applied.
- history_depth_status must be pass, fail, not_checked, or review_required.
- selected Slice 1 instruments must pass or be manually reviewed.

blocking_conditions:
- duplicate primary key.
- selected instrument has fail or not_checked history_depth_status without explicit review.
