# futures_continuous_htf_ondemand_resampling_contract

status: design_contract
project: MOEX Bot
contract_id: futures_continuous_htf_ondemand_resampling.v1
contract_version: v1
artifact_class: access_contract
format: in_memory_dataframe_or_equivalent
schema_version: futures_continuous_htf_ondemand_resampling.v1

purpose: Shared consumer access contract for on-demand higher-timeframe OHLCV bars derived from futures_continuous_5m.v1 without materializing separate 15m, 30m, 1h, or 4h parquet zones.

source_dataset_dependency:
- dataset_id: futures_continuous_5m
- schema_version: futures_continuous_5m.v1
- contract_file: contracts/datasets/futures_continuous_5m_contract.md
- required_source_granularity: 5m

producer: later_shared_futures_resampling_layer
consumer:
- later_research_consumers
- later_backtest_consumers
- later_strategy_consumers
- later_reporting_consumers

non_materialization_clause:
- 15m, 30m, 1h, and 4h bars under this contract are access-layer outputs only.
- No parquet zones, dataset partitions, CSV files, or other persistent higher-timeframe storage may be created under this contract.
- Persisted higher-timeframe artifacts require a separate dataset contract and explicit PM acceptance.

allowed_request_parameters:
- family_code: required string.
- continuous_symbol: optional string; when omitted, the access layer may resolve the canonical continuous symbol only through an explicit registry or dataset contract.
- roll_policy_id: required string; must match the selected futures_continuous_5m source partitions.
- adjustment_policy_id: required string; must match the selected futures_continuous_5m source partitions.
- timeframe: required enum: 15m, 30m, 1h, 4h.
- start: required inclusive timestamp or trade_date boundary.
- end: required inclusive timestamp or trade_date boundary.
- session_calendar_id: required string identifying the canonical MOEX futures session calendar binding.
- columns: optional explicit subset; primary key, lineage, timestamp, OHLCV, boundary, and schema fields required by this contract must not be dropped before validation.

unsupported_request_parameters:
- arbitrary pandas resample rule strings.
- custom bucket closure or label policy.
- custom fill, interpolation, or synthetic bar options.
- cross-family resampling.
- cross-roll-policy resampling.
- cross-adjustment-policy resampling.
- direct file output path for materialized HTF bars.

required_input_columns:
- trade_date
- end
- session_date
- continuous_symbol
- family_code
- source_secid
- source_contract
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- is_roll_boundary
- roll_map_id
- schema_version
- ingest_ts

input_schema_policy:
- schema_version must equal futures_continuous_5m.v1 for every source row.
- All required input columns are mandatory and non-null unless the source contract is changed by a later accepted version.
- The access layer must validate input schema before aggregation.

output_schema:
- trade_date
- session_date
- bucket_end
- timeframe
- continuous_symbol
- family_code
- source_secid
- source_contract
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- contains_roll_boundary
- source_bar_count
- first_source_end
- last_source_end
- schema_version

output_schema_policy:
- schema_version must equal futures_continuous_htf_ondemand_resampling.v1.
- bucket_end is the right-labeled higher-timeframe bucket timestamp.
- first_source_end and last_source_end must identify the first and last 5m source timestamps used in the output bar.
- source_bar_count must equal the number of source 5m rows aggregated into the bar.
- contains_roll_boundary must be true when any source row has is_roll_boundary=true; such bars are allowed only when all source rows still share one source_secid and one roll metadata set.

primary_key:
- continuous_symbol
- roll_policy_id
- adjustment_policy_id
- timeframe
- trade_date
- session_date
- bucket_end

aggregation_rules:
- open: first source open in timestamp order within the bucket.
- high: maximum source high within the bucket.
- low: minimum source low within the bucket.
- close: last source close in timestamp order within the bucket.
- volume: sum of source volume within the bucket.
- trade_date: preserved from the source rows; one output bucket must contain exactly one trade_date.
- session_date: preserved from the source rows; one output bucket must contain exactly one session_date.
- continuous_symbol: preserved; one output bucket must contain exactly one continuous_symbol.
- family_code: preserved; one output bucket must contain exactly one family_code.
- source_secid: preserved; one output bucket must contain exactly one source_secid.
- source_contract: preserved; one output bucket must contain exactly one source_contract.
- roll_policy_id: preserved; one output bucket must contain exactly one roll_policy_id.
- adjustment_policy_id: preserved; one output bucket must contain exactly one adjustment_policy_id.
- adjustment_factor: preserved; one output bucket must contain exactly one adjustment_factor.
- contains_roll_boundary: logical OR over source is_roll_boundary within the bucket.
- source_bar_count: count of 5m source rows in the bucket.
- No fill, interpolation, forward-fill, back-fill, inferred open, inferred close, synthetic rows, or synthetic volume are permitted.

bucket_policy:
- Buckets are right-closed and right-labeled.
- A source 5m row whose end timestamp equals a bucket boundary belongs to that boundary-labeled bucket.
- The output timestamp field is bucket_end.
- Custom bucket closure, custom labeling, and left-labeled output are forbidden.
- Bucket construction must use the canonical MOEX futures session calendar and must not rely only on wall-clock midnight boundaries.

trade_date_and_session_boundary_policy:
- A bucket must never contain rows from more than one trade_date.
- A bucket must never contain rows from more than one session_date.
- Buckets must be clipped or rejected at session boundaries according to the canonical MOEX futures session calendar.
- A partial bucket at a valid session edge is allowed only when all expected observed 5m bars inside that clipped session interval are present and validation passes.
- A partial bucket caused by missing source bars is forbidden.

roll_and_source_boundary_policy:
- A bucket must never contain rows from more than one source_secid.
- A bucket must never contain rows from more than one source_contract.
- A bucket must never contain rows from more than one roll_policy_id.
- A bucket must never contain rows from more than one adjustment_policy_id.
- A bucket must never contain rows from more than one adjustment_factor.
- A bucket must never bridge a roll_map_id boundary.
- If a roll boundary would fall inside a requested higher-timeframe bucket, the access layer must fail closed rather than aggregate across the boundary.

moex_futures_session_calendar_binding_requirement:
- The access layer must bind to a canonical MOEX futures session calendar contract before serving output.
- The calendar binding must determine trading days, session dates, valid session intervals, planned non-trading days, and session-edge bucket clipping.
- Observed source bars alone are not sufficient as the canonical session calendar.
- If the canonical calendar is unavailable, ambiguous, stale, or inconsistent with the requested date range, the request must fail closed.

validation_rules:
- requested timeframe must be one of 15m, 30m, 1h, 4h.
- source schema must match futures_continuous_5m.v1.
- required input columns must be present and non-null.
- source timestamps must be strictly increasing within continuous_symbol, roll_policy_id, adjustment_policy_id, family_code, trade_date, and session_date.
- duplicate source end timestamps are forbidden within the same continuous_symbol, roll_policy_id, adjustment_policy_id, family_code, trade_date, and session_date.
- OHLC fields must be non-null.
- high must be greater than or equal to low for source and output rows.
- open and close must be within low/high inclusive for source and output rows.
- source rows inside each output bucket must share trade_date, session_date, continuous_symbol, family_code, source_secid, source_contract, roll_policy_id, adjustment_policy_id, adjustment_factor, and roll_map_id.
- expected 5m source bars for each requested session interval must be present according to the canonical MOEX futures session calendar.
- source_bar_count must match the number of validated source 5m bars used in the bucket.
- output primary key must be unique.

fail_closed_rules:
- unsupported timeframe.
- missing canonical MOEX futures session calendar binding.
- missing source dataset contract or schema mismatch.
- missing required input column.
- null required input value.
- duplicate source timestamp.
- non-monotonic source timestamps.
- missing expected 5m source bar inside a requested bucket.
- source gap not explained by the canonical session calendar.
- invalid OHLC relationship.
- mixed trade_date inside a bucket.
- mixed session_date inside a bucket.
- mixed continuous_symbol inside a bucket.
- mixed family_code inside a bucket.
- mixed source_secid or source_contract inside a bucket.
- mixed roll_policy_id, adjustment_policy_id, adjustment_factor, or roll_map_id inside a bucket.
- roll boundary inside a bucket that would require aggregation across two source contracts or roll metadata sets.
- request attempts to persist 15m, 30m, 1h, or 4h bars as a dataset under this contract.

consumer_obligations:
- Consumers must request 15m, 30m, 1h, and 4h bars only through the shared futures resampling layer governed by this contract.
- Consumers must not call pandas resample, DuckDB date_bin, SQL time_bucket, or custom strategy/research resampling directly for these continuous futures bars.
- Consumers must pass explicit timeframe, date range, roll_policy_id, adjustment_policy_id, and session_calendar_id.
- Consumers must preserve output timestamp semantics: bucket_end is right-labeled and represents the close of the higher-timeframe bucket.
- Consumers must not treat on-demand output as a materialized dataset unless a separate materialized dataset contract exists.
- Research, backtest, and strategy code must not implement alternate boundary, fill, or bucket semantics.

blocking_conditions:
- any fail_closed_rule is triggered.
- requested source date range is not covered by valid futures_continuous_5m.v1 source partitions.
- canonical MOEX futures session calendar cannot validate expected bars for the requested interval.
- output would require crossing trade_date, session_date, source_secid, source_contract, roll_map_id, roll_policy_id, or adjustment_policy_id boundaries.
