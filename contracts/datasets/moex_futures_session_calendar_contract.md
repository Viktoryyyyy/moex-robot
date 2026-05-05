# moex_futures_session_calendar_contract

status: design_contract
project: MOEX Bot
contract_id: moex_futures_session_calendar.v1
contract_version: v1
artifact_class: access_contract
format: in_memory_dataframe_or_equivalent
schema_version: moex_futures_session_calendar.v1

purpose: Canonical MOEX futures trading-day and session-calendar binding for data validation, roll-window logic, and on-demand higher-timeframe resampling from futures_continuous_5m.v1.

producer: later_shared_moex_futures_session_calendar_layer
consumer:
- futures_raw_5m_loader
- futures_continuous_roll_map_builder
- futures_continuous_5m_builder
- futures_continuous_htf_ondemand_resampling_layer
- later_research_and_backtest_consumers

source_requirement:
- The canonical source must be a MOEX/ISS futures trading calendar source that can identify futures trading days and non-trading days for the requested date range.
- Observed market bars alone are not a valid calendar source.
- The source binding must identify its endpoint or provider name, retrieval timestamp, requested date range, and source status.
- A cached or derived representation is allowed only as an implementation detail when it preserves the canonical source fields and provenance.
- If MOEX publishes conflicting calendar information for the same date or session interval, the calendar status must be ambiguous and consumers must fail closed.

calendar_identity:
- calendar_id: moex_futures_session_calendar.v1
- market: futures
- exchange: MOEX
- exchange_timezone: Europe/Moscow
- base_granularity_for_intraday_validation: 5m

required_fields:
- calendar_id
- calendar_date
- session_date
- session_seq
- is_trading_day
- is_planned_non_trading_day
- session_interval_id
- session_start
- session_end
- exchange_timezone
- expected_bar_granularity
- expected_first_bar_end
- expected_last_bar_end
- expected_bar_count
- source_name
- source_endpoint_or_provider
- source_retrieved_at
- source_range_start
- source_range_end
- calendar_status
- schema_version

field_semantics:
- calendar_date: exchange-local civil date in Europe/Moscow used by the source calendar.
- session_date: canonical trading session date assigned to bars and roll-map windows.
- session_seq: strictly increasing integer sequence over trading session dates inside the covered range.
- is_trading_day: true only when the futures market is open for the session_date according to the canonical source.
- is_planned_non_trading_day: true when the canonical source marks the date as weekend, holiday, transferred non-working day, or other planned no-trade day.
- session_interval_id: stable identifier for one continuous intraday trading interval within a session_date.
- session_start: inclusive exchange-local timestamp for the interval start.
- session_end: inclusive exchange-local timestamp for the interval end boundary used for right-labeled 5m bars.
- expected_first_bar_end: first expected 5m bar end timestamp inside the session interval.
- expected_last_bar_end: last expected 5m bar end timestamp inside the session interval.
- expected_bar_count: exact expected number of 5m bar ends in the interval.
- calendar_status: one of canonical, unavailable, ambiguous, stale, inconsistent.

session_date_semantics:
- session_date is the canonical trading-session label used by futures_raw_5m.v1, futures_continuous_5m.v1, roll-map valid_from_session/valid_through_session windows, and HTF resampling output.
- Every source 5m bar must map to exactly one session_date and one session_interval_id.
- A trading session may contain one or more intraday intervals.
- Buckets must never cross session_date boundaries.
- Buckets must never cross session_interval_id boundaries unless a later accepted contract explicitly allows a named intra-session break policy.
- Roll-window calculations that use trading-session offsets must use session_seq, not calendar-day arithmetic.

resampling_boundary_validation_rule:
- For each requested family_code, continuous_symbol, roll_policy_id, adjustment_policy_id, timeframe, and date range, the resampling layer must first load the canonical calendar rows covering the full requested range.
- For every source 5m row, end must fall within exactly one valid session interval and equal one expected 5m bar end for that interval.
- For every output bucket, bucket_end must be a valid right-labeled bucket boundary clipped to one session_interval_id and one session_date.
- Source rows inside one output bucket must match the expected 5m bar ends implied by the canonical calendar, the requested timeframe, and session-edge clipping.
- A partial bucket is valid only when it is caused by a canonical session interval edge and contains all expected 5m bars for that clipped bucket.
- A partial bucket caused by missing source bars is invalid.
- Any bucket that would cross trade_date, session_date, session_interval_id, source_secid, source_contract, roll_map_id, roll_policy_id, adjustment_policy_id, or adjustment_factor must fail closed.

validation_rules:
- schema_version must equal moex_futures_session_calendar.v1.
- calendar_id must equal moex_futures_session_calendar.v1.
- exchange_timezone must equal Europe/Moscow.
- expected_bar_granularity must equal 5m for HTF validation consumers.
- calendar_status must equal canonical for any date/session used by consumers.
- session_seq must be strictly increasing across trading session dates.
- session_date must be unique per trading session label and must not be assigned to a planned non-trading day.
- session intervals for the same session_date must not overlap.
- expected_first_bar_end and expected_last_bar_end must fall within their session interval.
- expected_bar_count must be positive for trading intervals and must match the number of expected 5m bar ends.
- The requested source range must be fully covered by source_range_start and source_range_end.

fail_closed_rules:
- missing calendar contract.
- missing required calendar field.
- schema_version mismatch.
- calendar_id mismatch.
- calendar_status not canonical for any requested date or session interval.
- calendar source unavailable for the requested date range.
- requested range not fully covered by source_range_start/source_range_end.
- ambiguous, conflicting, stale, or inconsistent calendar source response.
- missing session interval for a source bar timestamp.
- source bar timestamp maps to more than one session interval.
- source bar timestamp is not an expected 5m bar end.
- missing expected 5m source bar inside a requested bucket.
- extra source bar outside canonical session intervals.
- duplicate or non-monotonic session_seq.
- overlapping session intervals inside the same session_date.
- bucket boundary not validated by the canonical calendar.
- bucket would cross session_date or session_interval_id.
- roll-window trading-session offset cannot be computed from session_seq.

consumer_obligations:
- Consumers must pass or resolve calendar_id=moex_futures_session_calendar.v1 before validating trading sessions or serving HTF bars.
- Consumers must not infer trading sessions from observed bars only.
- Consumers must preserve session_date from the canonical calendar when validating or deriving intraday outputs.
- Consumers must treat missing or ambiguous calendar coverage as a hard blocking condition.

implementation_notes:
- This contract defines the calendar binding only.
- It does not implement the calendar access layer.
- It does not materialize higher-timeframe bars.
- It does not authorize strategy, research, runtime, or server changes.
