# futures_continuous_series_v1_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: dataset_family
schema_version: futures_continuous_series_v1.v1

purpose: Contract umbrella for unadjusted continuous futures v1 artifacts built from immutable raw futures contracts for the accepted Slice 1 universe.

producer: src/moex_data/futures/continuous_series_builder.py
consumer:
- futures_data_lake_pm_review
- later_futures_data_access_layer
- later_research_and_backtest_consumers

artifact_members:
- futures_continuous_roll_map
- futures_continuous_5m
- futures_continuous_d1
- futures_continuous_builder_manifest
- futures_continuous_quality_report

raw_vs_continuous_boundary:
- futures_raw_5m, futures_futoi_5m_raw, and futures_derived_d1_ohlcv remain immutable raw/derived-by-contract source zones.
- continuous artifacts are derived views and must never overwrite or mutate raw contract partitions.
- every continuous row must preserve source_contract and source_secid lineage where the source is a concrete MOEX contract.
- unadjusted_v1 means source OHLC prices are copied without back-adjustment and adjustment_factor is always 1.0.

accepted_scope:
- included: SiM6, SiU6, SiU7, SiZ6, USDRUBF
- excluded_deferred: SiH7, SiM7
- Si continuous series may be partial because excluded contracts can create chain gaps.
- partial chain gaps must be reported explicitly and must not be silently bridged.

family_mapping_v1:
- SiM6: family_code=Si
- SiU6: family_code=Si
- SiU7: family_code=Si
- SiZ6: family_code=Si
- USDRUBF: family_code=USDRUBF

roll_policy_id: expiration_minus_1_trading_session_v1
adjustment_policy_id: unadjusted_v1
calendar_status: canonical_apim_futures_xml
calendar_rule:
- roll_date must be calculated from ordered MOEX futures trading sessions.
- roll_date must not be calculated from calendar days alone.
- observed raw bars may validate coverage but must not be the canonical calendar source for roll scheduling.

expiration_anchor_rule:
- roll_anchor_date = expiration_date if non-null and validated.
- else roll_anchor_date = last_trade_date if non-null and validated.
- else roll_status=blocked and decision_source=unresolved.

decision_source_enum:
- registry_expiration_date
- registry_last_trade_date_fallback
- manual_reviewed_override
- unresolved

perpetual_contract_rule:
- USDRUBF is perpetual identity.
- no fake expiration is allowed.
- no ordinary roll schedule is allowed.
- source_contract=USDRUBF.
- continuous_symbol=USDRUBF.
- roll_required=false.

validation_boundary:
- continuous builder must fail closed if roll map is missing, ambiguous, overlapping, or contains unresolved required decisions for the requested family.
- continuous builder must fail closed if raw source partitions are missing for a selected roll window.
- continuous builder must fail closed if excluded instruments appear in continuous outputs.
- strategy, research, runtime, Telegram, and database service layers are outside this contract.

blocking_conditions:
- accepted Slice 1 family mapping cannot be represented in roll map.
- source_contract/source_secid lineage is absent from continuous outputs.
- decision_source is missing or outside the enum.
- calendar_status is not canonical_apim_futures_xml.
- adjustment_factor differs from 1.0 under unadjusted_v1.
- USDRUBF is assigned ordinary expiration or ordinary roll behavior.
- SiH7 or SiM7 appears in produced continuous outputs.
