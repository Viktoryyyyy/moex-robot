# Si/CR historical intraday gap reconciliation

Scope: the isolated 2026-07-20–2026-09-05 captures from
`history_si_u6_20260906_v1` and `history_cr_u6_20260906_v1`.
This analysis does not overwrite raw TradeStats or grant model acceptance.

The 22 internal gaps represent 23 missing 5m slots: CR has one gap spanning
two consecutive missing slots. Bounded authenticated MOEX ISS RFUD minute
candle queries corroborated 19 empty slots. Both adjacent actual TradeStats
bars match the minute aggregates exactly in open/high/low/close/volume.

Four other slots contain trades in the minute source:

| SECID | Missing TradeStats interval end (Moscow) |
|---|---|
| SiU6 | 2026-08-18 17:40 |
| CRU6 | 2026-08-18 17:40 |
| SiU6 | 2026-08-28 12:30 |
| CRU6 | 2026-08-28 12:30 |

A second full-pagination canonical TradeStats request for each SECID/date
still omitted the same bar (202 returned rows per contract/date). Thus a
successful transport and per-row quality result did not establish complete
intraday history. These gaps cannot be classified as no-trade periods.

Each of these four intervals has five complete minute candles, with exact
adjacent raw OHLCV matches. The reconciler exposes an explicitly labelled
secondary-source OHLCV recovery candidate. It keeps open interest, trade count
and traded value unknown, and uses the evidence acquisition time as candidate
availability. It never backdates research availability or silently labels ISS
data as AlgoPack TradeStats.

```sh
PYTHONPATH=src python -m moex_data.rub_history_gap_reconciliation \
  --evidence history_gap_source_probe_20260906.json > gap_reconciliation.json
```

The evidence file retains requested URL/window, SECID, acquisition time,
minute response and adjacent raw bars. The command validates complete minutes,
unique timestamps, exact bounded scope and adjacent Decimal OHLCV equality.
Partial minute coverage remains unresolved. Multiple consecutive empty slots
are classified without creating bars. Exit code 1 indicates unresolved gaps;
exit code 0 indicates classification, not dataset acceptance.

Before H1/D1/W1 production use, explicitly accept a secondary-source dataset
policy for the four candidates or obtain corrected canonical TradeStats.
The current raw-only chain must keep the affected dates blocked for complete
aggregations. Classification of internal slots does not establish first/last
session boundaries, full calendar coverage, continuous roll mapping, five-year
depth, or model validity. Those remain separate checks.
