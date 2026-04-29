# futures_excluded_family_rule_contract

status: implemented_contract
project: MOEX Bot
artifact_class: rule_contract
format: markdown
schema_version: futures_excluded_family_rule.v1

purpose: Rule for explicitly excluding futures families/instruments from Slice 1 acquisition or later universe expansion.
producer: futures_slice1_universe_selector
consumer:
- futures_data_lake_pm_review
- futures_data_lake_loader_config_authoring

primary_key:
- rule_id
- rule_version
- family_code

required_fields:
- rule_id
- rule_version
- family_code
- exclusion_status
- exclusion_reason
- schema_version

nullable_fields:
- secid
- asset_class
- duplicate_exposure_group_id
- review_notes
- expires_after_snapshot_date

status_fields:
- exclusion_status
- validation_status
- review_status

validation_rules:
- excluded families must remain visible in raw registry artifacts.
- exclusion_status must be excluded, not_excluded, or review_required.
- exclusion_reason must be present for excluded or review_required status.

blocking_conditions:
- Slice 1 includes a family with exclusion_status excluded without PM override.
