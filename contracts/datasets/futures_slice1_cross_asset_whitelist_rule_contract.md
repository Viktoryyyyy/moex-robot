# futures_slice1_cross_asset_whitelist_rule_contract

status: implemented_contract
project: MOEX Bot
artifact_class: rule_contract
format: markdown
schema_version: futures_slice1_cross_asset_whitelist_rule.v1

purpose: Rule for selecting the first cross-asset candidate whitelist for Slice 1 futures data lake work.
producer: futures_slice1_universe_selector
consumer:
- futures_data_lake_pm_review
- futures_data_lake_loader_config_authoring

primary_key:
- rule_id
- rule_version

required_fields:
- rule_id
- rule_version
- candidate_family_code
- asset_class
- inclusion_status
- inclusion_reason
- schema_version

nullable_fields:
- preferred_secid
- duplicate_exposure_group_id
- liquidity_screen_id
- history_depth_screen_id
- availability_report_id
- review_notes

status_fields:
- inclusion_status
- validation_status
- review_status

validation_rules:
- Slice 1 whitelist must be declarative and finite.
- Inclusion must reference registry, availability, liquidity, and history-depth evidence when available.
- USDRUBF perpetual may be selected by family/instrument identity and must not require continuous series logic.
- Optional third family remains excluded unless explicitly accepted by PM.

blocking_conditions:
- whitelist includes an instrument not present in futures_normalized_instrument_registry.
- whitelist includes failed liquidity, history-depth, or availability status without explicit review.
