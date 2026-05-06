# futures_limited_rfud_pilot_classification_contract

status: implemented_contract
project: MOEX Bot
contract_id: futures_limited_rfud_pilot_classification.v1
contract_version: v1
artifact_class: external_pattern
format: parquet
schema_version: futures_limited_rfud_pilot_classification.v1
path_pattern: ${MOEX_DATA_ROOT}/futures/registry/pilot_classification/snapshot_date={snapshot_date}/limited_rfud_pilot_classification.parquet
producer: moex_data.futures.limited_rfud_pilot_classifier
consumer:
- futures_data_lake_pm_review
- futures_registry_refresh_validation

purpose: Classify the controlled limited RFUD pilot expansion for PM-selected families CR, GD, and GL using existing evidence artifacts only.

scope:
- exactly CR, GD, GL pilot families
- RFUD board only
- classification from existing registry, family mapping, tradestats availability, FUTOI availability, liquidity screen, and history-depth screen artifacts
- preserve existing whitelist instruments SiM6, SiU6, SiU7, SiZ6, USDRUBF
- preserve existing excluded/deferred instruments SiH7 and SiM7
- preserve Continuous v1 policy

primary_key:
- eligibility_snapshot_date
- board
- secid

required_fields:
- eligibility_snapshot_date
- secid
- short_code
- family_code
- board
- engine
- market
- instrument_type
- classification_status
- classification_reason
- deferral_reason
- exclusion_reason
- registry_snapshot_date
- registry_source
- identity_check_status
- board_check_status
- family_mapping_status
- raw_5m_check_status
- futoi_check_status
- liquidity_check_status
- history_depth_check_status
- expiration_policy_status
- perpetual_policy_status
- calendar_quality_status
- continuous_eligibility_status
- source_scope
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- notes
- schema_version

classification_statuses:
- included
- excluded
- deferred

validation_rules:
- Output must contain only family_code in CR, GD, GL.
- included requires all mandatory checks to pass.
- Missing or unresolved FUTOI must classify as deferred, not included.
- Missing or failed raw 5m availability must classify as deferred, not included.
- Liquidity or history-depth failure must classify as deferred, not included.
- Structural identity failure must classify as excluded.
- Continuous v1 fields must remain roll_policy_id=expiration_minus_1_trading_session_v1, adjustment_policy_id=unadjusted_v1, adjustment_factor=1.0.
- This artifact must not trigger historical backfill or all-futures rollout.
