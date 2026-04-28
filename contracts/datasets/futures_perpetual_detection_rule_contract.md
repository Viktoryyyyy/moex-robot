# futures_perpetual_detection_rule_contract

status: implemented_contract
project: MOEX Bot
artifact_class: rule_contract
format: markdown
schema_version: futures_perpetual_detection_rule.v1

purpose: Rule for classifying MOEX futures as ordinary expiring contracts or perpetual/non-ordinary futures.
producer: futures_perpetual_detection_rule_evaluator
consumer:
- futures_normalized_instrument_registry_builder
- futures_expiration_map_builder
- futures_slice1_universe_selector

primary_key:
- rule_id
- rule_version

required_fields:
- rule_id
- rule_version
- evaluated_snapshot_id
- secid
- board
- family_code
- is_perpetual
- evidence_type
- decision_status
- schema_version

nullable_fields:
- last_trade_date
- expiration_date
- sentinel_date
- manual_override_reason
- review_notes

status_fields:
- decision_status
- override_status
- validation_status

validation_rules:
- Missing last_trade_date is evidence for perpetual/non-ordinary status but is not the only permitted evidence.
- Reviewed sentinel or technical dates, including far-future registry dates, may be used as evidence of non-ordinary expiration.
- Manual override must include manual_override_reason and reviewer note.
- Ordinary expiring futures must have a usable expiration_date or last_trade_date unless explicitly excluded.

blocking_conditions:
- active selected instrument has unresolved ordinary vs perpetual status.
- sentinel/technical date evidence is used without reviewed evidence_type.
