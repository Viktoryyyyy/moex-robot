# futures_futoi_5m_raw_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_futoi_5m_raw.v1

purpose: Canonical raw 5-minute MOEX FUTOI open-interest rows loaded from MOEX analyticalproducts/futoi for the accepted Slice 1 whitelist, stored separately from raw OHLCV bars.
producer: src/moex_data/futures/futoi_raw_loader.py
consumer:
- futures_futoi_5m_raw_quality_report
- futures_data_lake_pm_review
- later_futures_data_access_layer
- later_futoi_joined_derived_views

path_pattern: ${MOEX_DATA_ROOT}/futures/futoi_raw/trade_date={trade_date}/family={family_code}/secid={secid}/part.parquet
partitioning:
- trade_date
- family_code
- secid
primary_key:
- trade_date
- ts
- secid
- clgroup

required_fields:
- trade_date
- ts
- moment
- board
- secid
- family_code
- source_ticker
- source_scope
- clgroup
- pos
- pos_long
- pos_short
- pos_long_num
- pos_short_num
- source
- source_endpoint_url
- ingest_ts
- schema_version
- short_history_flag
- calendar_denominator_status

nullable_fields:
- systime
- sess_id
- seqnum

status_fields:
- source_scope
- short_history_flag
- calendar_denominator_status

validation_rules:
- primary_key must be unique inside each written partition and across repeated idempotent loads for the same trade_date/secid/clgroup.
- schema_version must equal futures_futoi_5m_raw.v1.
- source must equal MOEX_FUTOI.
- calendar_denominator_status must equal canonical_apim_futures_xml for accepted Slice 1 loader runs.
- clgroup must be non-null.
- pos, pos_long, pos_short, pos_long_num, and pos_short_num must be non-null.
- pos_long and pos_long_num must be greater than or equal to zero.
- pos_short must be less than or equal to zero.
- pos_short_num must be greater than or equal to zero.
- trade_date must match the trade_date partition value.
- secid must match the secid partition value.
- family_code must match the family partition value.
- FUTOI storage must remain separate from futures_raw_5m OHLCV storage.
- Slice 1 FUTOI loader must write only the accepted whitelist: SiM6, SiU6, SiU7, SiZ6, USDRUBF.
- SiU7 may have short_history_flag=true; all other accepted Slice 1 instruments must have short_history_flag=false.
- For ordinary expiring futures where MOEX FUTOI is exposed by ticker/family rather than concrete contract, source_scope must preserve that fact as family_aggregate_futoi.

blocking_conditions:
- missing required field.
- duplicate primary key.
- invalid position sign or participant count.
- non-canonical calendar_denominator_status.
- partition created for excluded instruments SiH7 or SiM7.
- zero rows for an accepted whitelist instrument without explicit failed loader status in the run manifest.
