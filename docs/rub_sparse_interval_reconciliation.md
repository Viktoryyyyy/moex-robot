# Corroborated empty TradeStats intervals

The September 6 USDRUBF response omits bucket ends 12:05, 13:50 and 16:20 MSK.
The timestamp is an interval end: 12:05 describes [12:00, 12:05), not trades
starting at 12:05. Minute candles from the RFUD USDRUBF endpoint contain no
observations in that interval. Aggregated minute OHLCV matches both adjacent
TradeStats buckets exactly. The same reconciliation succeeds for all three gaps.

The current USDRUBF snapshot producer now queries a bounded 15-minute ISS window
around each isolated missing 5-minute bucket. It requires valid unique completed
minute candles, no candles in the missing interval, and exact OHLCV agreement
with the two existing adjacent TradeStats bars. An empty response, schema error,
non-isolated gap, mismatched value or observed trade in the gap remains an error.
At most twelve gaps are queried per refresh. This does not change generic feed
loading or turn absence alone into an accepted zero-volume bar.

Only corroborated empty intervals may participate in the existing broker-aligned
15-minute aggregation. Aggregate OHLC and volume use actual TradeStats bars only;
no 5-minute price, OI or forward-filled candle is created. The strict default of
the bridge remains unchanged for callers without explicit evidence. Existing
legacy historical clearing rules and EMA's lack of standalone trading authority
also remain unchanged.

Evidence contains the minute response rows, adjacent TradeStats OHLCV, request
window, endpoint, timestamp interpretation and verification time. Feature
availability and the decision as-of cannot precede the verification time. It is
not permissible to backdate a newly corroborated gap into historical model inputs.
The price observation retains its original market_data_as_of separately.

Offline regression tests cover exact matching, mismatched prices/volume, empty
responses, duplicate/schema defects, trades in the allegedly empty interval,
existing TradeStats rows, aggregation without invented volume and future evidence.
A read-only trial against the real September 6 server sources built current
structure from 98 observed 5-minute bars, corroborating all three gaps and producing
33 broker-aligned aggregates. This trial did not publish the production snapshot.

This change resolves this source sparsity case. It does not establish full daily/
weekly readiness, repair arbitrary missing data or accept FUTOI governance gates.
