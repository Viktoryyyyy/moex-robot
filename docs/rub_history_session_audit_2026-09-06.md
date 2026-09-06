# SiU6/CRU6 calendar and session boundary audit

Scope: July 20 through September 5, 2026 only. No extrapolation to another
contract, year or schedule regime. Calendar evidence reviewed on September 6:

- https://www.moex.com/n101980?nt=112 — July 14 schedule: opening auction
  06:50–07:00 and trading through 23:50; currency futures on weekends from July 18.
- https://www.moex.com/n95564 — weekend sessions 09:50–19:00; closures August
  1–2 and 15–16; weekend sessions belong to the following trading day.
- https://www.moex.com/n96571 — annual holiday and weekend schedule.
- https://www.moex.com/n103333?nt=107 — confirmation of August 15–16 closure.

`python -m moex_data.rub_history_session_acceptance --dataset RECONCILED_JSON --evidence BOUNDARY_JSON`

The audit requires two bounded, correctly identified ISS minute queries per
open date, covering both sides of session boundaries. It compares neighboring
5m OHLCV with minute aggregates using exact decimals. Absent edge intervals
require empty minute evidence and an adjacent matching bar; traded missing
bars, out-of-session trades and conflicting prices/volume block the date.
All expected five-minute positions, including the opening auction, must be
covered by actual bars or corroborated empty evidence. Input file hashes are
included by the CLI. Existing reconciled datasets and accepted pointers are
not modified.

Server evidence: `deploy_backups/history_session_boundary_evidence_20260906.json`,
176 queries / 2,747 minute rows across 88 captured dates. All four absent
calendar dates are scheduled closures. Coverage verification passes for 38
SiU6 dates and 40 CRU6 dates. Ten opening bars conflict between sources:

| Contract | Dates and interval ends (Moscow) |
|---|---|
| SiU6 | Aug 7 07:05; Aug 17 07:10; Aug 18 07:05; Sep 1 07:05; Sep 3 07:05; Sep 4 07:10 |
| CRU6 | Aug 7 07:05; Aug 17 07:10; Aug 18 07:05; Sep 1 07:05 |

These are not floating-point comparison artifacts: differences include close,
high/low and volume. For example, SiU6 Aug 17 07:10 volume is 4,591 in the
captured TradeStats bar versus 5,301 in aggregated ISS minutes. No conflicting
source is silently preferred or overwritten.

Calendar-date coverage is distinct from exchange D1 acceptance. The previous
44 daily and seven weekly previews remain unaccepted: weekend bars must be
assigned to the following exchange trading day before regrouping D1/W1.
The capture also lacks July 18–19 for the first Monday and September 6–7 for
its trailing weekend. These edges cannot be treated as complete trading days.
Model acceptance, exchange D1 acceptance and exchange W1 acceptance remain false.
