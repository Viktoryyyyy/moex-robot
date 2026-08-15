# USDRUBF RUB Intelligence — S7.2 FORTS Intraday Clearing Gap v1

```text
PROJECT=MOEX_Bot
STAGE=S7.2
POLICY=DOCUMENTED_HISTORICAL_SESSION_GAP
CURRENT_LIVE_RUNTIME_DATE_BEHAVIOR=unchanged
BROKER_ORDER_AUTHORITY=none
```

## Empirical Trigger

The first full Phase-3 S7.2 replay produced:

```text
candidate_prediction_days=1027
live_bridge_eligible_prediction_days=59
live_bridge_excluded_prediction_days=968
live_bridge_coverage=0.05744888023369036
```

All 968 exclusions were `broken 15m bucket` errors. Their broker-label clock distribution was dominated by:

```text
14:00 = 887 / 968 = 91.63%
```

The 14:00 pattern appeared almost every trading month from 2022 through early 2026 and then largely disappeared.

## External Schedule Evidence

Official Moscow Exchange materials document the historical FORTS/SPECTRA intraday clearing pause as:

```text
14:00-14:05 Moscow time = intraday/intermediate clearing session
```

Official references used for this policy:

- Moscow Exchange / Plaza II gateway documentation, `p2gate_ru.html`: intermediate clearing in the real SPECTRA system runs 14:00-14:05 Moscow time.
  - https://ftp.moex.com/pub/OTC/RFS_IQS/test/OTCGate/doc/p2gate_ru.html
- Moscow Exchange FX futures parameters: two clearing sessions, including 14:00-14:05 intraday clearing.
  - https://www.moex.com/a7235
- Moscow Exchange announcement dated 2025-11-05: the derivatives market moves to a unified trading session on 2026-03-23 and intermediate clearing is cancelled.
  - https://www.moex.com/n95054

The observed historical data pattern is therefore classified as a documented market-session boundary, not as a missing-data defect, for the exact governed interval below.

## Governed Interval

The bridge recognizes exactly one historical incomplete aligned 15m bucket as expected:

```text
trade_date < 2026-03-23
bucket_label = 14:00 Europe/Moscow
```

Rationale:

- the aligned 15m bucket labeled 14:00 requires 5m bars at 14:00, 14:05, 14:10 under the existing runtime convention;
- the historical exchange pause 14:00-14:05 makes that aligned bucket structurally incomplete;
- no synthetic 15m bar is created for 14:00;
- real 5m bars after the pause remain untouched;
- the next fully aligned bucket, e.g. 14:15/14:20/14:25, is aggregated normally.

## Fail-Closed Boundary

This policy does **not** permit general gap tolerance.

The bridge still raises `LiveShadowBridgeError` for:

- any broken 15m bucket at a clock label other than 14:00;
- a broken 14:00 bucket on or after 2026-03-23;
- duplicate/non-increasing 5m timestamps;
- invalid OHLCV;
- missing complete aligned 15m history overall.

No bars are synthesized, forward-filled, back-filled, timestamp-shifted, or deleted from Level/Structure inputs.

## Runtime Impact

The change is date-bounded to historical sessions before 2026-03-23. Current live 2026 post-unified-session behavior remains fail-closed under the same alignment rules as before.

S7.2 must be rerun against the exact accepted Phase-3 manifest. Any remaining exclusions after applying the documented 14:00 gap policy remain unavailable prediction days and require separate classification before empirical acceptance.

## Acceptance Criteria

```text
DOCUMENTED_1400_GAP_CLASSIFIED=yes
PRE_2026_03_23_1400_BUCKET_SKIPPED_WITHOUT_SYNTHESIS=yes
POST_2026_03_23_1400_GAP_FAIL_CLOSED=yes
OTHER_BROKEN_BUCKETS_FAIL_CLOSED=yes
REAL_5M_BARS_DELETED=no
OHLCV_IMPUTED=no
CURRENT_LIVE_DATE_SEMANTICS_RELAXED=no
UNIT_TESTS=pass
FULL_REPOSITORY_CI=pass
EMPIRICAL_S7_2_RERUN=pending
```
