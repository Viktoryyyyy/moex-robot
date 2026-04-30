# futures_raw_5m_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_raw_5m.v1

purpose: Canonical raw 5-minute MOEX futures OHLCV bars loaded from ALGOPACK FO TradeStats for the accepted Slice 1 whitelist.
producer: src/moex_data/futures/raw_5m_loader.py
consumer:
- futures_raw_5m_quality_report
- futures_data_lake_pm_review
- later_futures_derived_d1_builder
- later_futures_data_access_layer

path_pattern: ${MOEX_DATA_ROOT}/futures/raw_5m/trade_date={trade_date}/family={family_code}/secid={secid}/part.parquet
partitioning:
- trade_date
- family_code
- secid
primary_key:
- trade_date
- ts
- secid

required_fields:
- trade_date
- ts
- end
- session_date
- board
- secid
- family_code
- open
- high
- low
- close
- volume
- source
- ingest_ts
- schema_version
- short_history_flag
- calendar_denominator_status

nullable_fields:
- value
- num_trades
- source_endpoint_url
- source_seqnum

status_fields:
- short_history_flag
- calendar_denominator_status

validation_rules:
- primary_key must be unique inside each written partition and across repeated idempotent loads for the same trade_date/secid.
- schema_version must equal futures_raw_5m.v1.
- source must equal MOEX_ALGOPACK_FO_TRADESTATS.
- calendar_denominator_status must equal canonical_apim_futures_xml for accepted Slice 1 loader runs.
- OHLC fields must be non-null.
- high must be greater than or equal to low.
- open and close must be within low/high inclusive.
- trade_date must match the trade_date partition value.
- secid must match the secid partition value.
- family_code must match the family partition value.
- Slice 1 loader must write only the accepted whitelist: SiM6, SiU6, SiU7, SiZ6, USDRUBF.
- SiU7 may have short_history_flag=true; all other accepted Slice 1 instruments must have short_history_flag=false.

blocking_conditions:
- missing required field.
- duplicate primary key.
- invalid OHLC ordering.
- non-canonical calendar_denominator_status.
- partition created for excluded instruments SiH7 or SiM7.
- zero rows for an accepted whitelist instrument without explicit failed loader status in the run manifest.
