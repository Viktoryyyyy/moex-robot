# Evidence-bound conflict overrides and exchange-day research history

`python -m moex_data.rub_exchange_history_research --dataset RECONCILED_JSON --boundary BOUNDARY_JSON --crosscheck CROSSCHECK_JSON --output NEW_JSON`

This creates a separate research version. Original TradeStats captures and
accepted pointers are untouched. Output creation is exclusive. Every override
retains the original bar, replacement OHLCV, evidence hashes, policy identifier,
and acquisition-based availability. Turnover and trade count become null;
aggregates preserve unknown additive fields.

An existing edge conflict is eligible only when five complete ISS minutes
exactly repeat the earlier query, both adjacent captured bars agree with
minute aggregates, and a separately requested 10-minute ISS candle agrees with
all ten constituent minutes. This is cross-resolution corroboration within
ISS, not an independent provider or proof of the upstream root cause. The
policy is `stable_iss_minutes_and_10m_research_override_v1`; it does not certify
a canonical TradeStats correction. Remaining coverage failures block the build.

Reviewed period: July 20–September 5, 2026. Weekend sessions map to the next
Monday; Friday evening remains Friday under the post-March-23 unified session.
Sources: https://www.moex.com/n95564 and
https://www.moex.com/ru/derivatives/unified-trading-session . The older weekend
FAQ also describes the pre-unified evening convention and must not override
the newer effective regime.

D1 requires all constituent calendar dates to pass coverage. Scheduled August
closures are excluded. A W1 requires all five trading-day labels with complete
constituent coverage. July 20 lacks July 18–19; September 7 lacks September 6–7.
These partial groups are retained with explicit false coverage flags.

Server results on September 6:

| Metric | SiU6 | CRU6 |
|---|---:|---:|
| Conflict overrides | 6 | 4 |
| Increase in intraday volume | 1133 | 4483 |
| Calendar dates passing boundary/coverage audit | 44 | 44 |
| D1 groups / complete coverage | 36 / 34 | 36 / 34 |
| W1 groups / complete coverage | 8 / 6 | 8 / 6 |
| Complete D1 with exact ISS daily OHLC match | 34 | 34 |

Independent verification confirmed unique timestamps, unchanged bar counts,
exact volume deltas for overrides and conservation of total volume through
D1/W1 grouping. Full coverage is not model acceptance or exhaustive trade-level
validation of every intraday bar.

ISS daily volume remains unreconciled: none of the available 35 daily reference
volumes matches the grouped intraday source volume. This cannot be attributed
solely to the ten conflicts. A full, paginated minute pilot for SiU6 July 21
returned 1004 minutes / 1700469 contracts, exactly matching captured 5m volume,
while its ISS daily candle reports 1713165. The discrepancy could reflect
source methodology or data quality; its cause is not established. Daily ISS
candles can also report the last-trade timestamp as their end (e.g. SiU6
August 12, 23:49:55); intraday candles still require complete fixed durations.

All D1/W1 rows remain RESEARCH_ONLY. Model acceptance and accepted-pointer
promotion are false. Next acceptance work is the daily-volume definition and
source reconciliation, missing capture edges, and broader historical validation.
