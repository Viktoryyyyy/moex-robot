# PROJECT=MOEX_Bot

Task: `live_price_oi_basis_carry_freshness_v1`.
Base: `c5cf37c2e6bcd231ef4faf23d46c6c952ae4abd6`.

## Result and scope

The heavy snapshot now selects the existing opted-in, persisted fast-market
generation after slow historical captures. Market/OI, basis/carry and native
current-pair proof travel together through the existing envelope validation.
It reuses the consumer's 60-second source TTL checks immediately before
publication, so an old heavy observation cannot keep a usable flag merely
because collection finished recently. No network request is added on this path.

Only the live snapshot runner, its publication tests and this task document
change. Historical evidence, FUTOI admissions, source identity, source timestamps,
Stage10, timers, registry, original TTL and trading authority are unchanged.
An enabled missing, failed, corrupted or expired fast cache refuses live facts
and dependent basis; it does not fall back to earlier heavy quotes. Opt-out
continues using the heavy observations, subject to publication-time source age.
Independent futures remain usable when the spot row has expired.

## Reproduced runtime evidence before change

On 2026-09-28 the saved heavy snapshot completed at 20:12:52.877162 Moscow,
but its futures source rows were from 20:10:26–27 (about 146 seconds old).
The persisted flags still reported usable live prices and 14 basis/carry metrics.
The separate 30-second fast timer was healthy; the canonical reader and HTTP
API already overlaid fresh futures. The defect was publication of the heavy file.
CNYRUB_TOM's source row was 19:15:01 with `TRADINGSTATUS=N`; its expired spot
and dependent live metrics must remain unavailable. USD_TOM is outside the live
schema. Neither missing spot is replaced or granted live authority.

## Validation

Synthetic end-to-end delayed refresh tests enter `refresh_snapshot`, allow slow
work to advance the clock by four minutes, publish a real fast envelope, save
the snapshot and call the canonical reader. External acquisitions and unrelated
history work are replaced; fast-envelope admission, TTL and basis derivation are
real. Cases cover fresh/expired/failed/corrupt/missing/disabled fast generations,
same-generation metric legs, read-time expiry, no extra fetch, unchanged slow
values and independent futures with expired spot.

Existing publication fixtures now provide concrete rows/quality metadata so the
new publication gate is exercised instead of trusting a bare READY flag.
Exact test results, remote head, review/checks and applied runtime evidence are
recorded in the task PR after execution.
