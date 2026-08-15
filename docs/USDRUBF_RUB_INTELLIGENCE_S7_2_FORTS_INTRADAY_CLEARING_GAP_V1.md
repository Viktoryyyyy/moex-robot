# USDRUBF RUB Intelligence — S7.2 FORTS Intraday Clearing Gap v1

```text
PROJECT=MOEX_Bot
STAGE=S7.2
POLICY=DOCUMENTED_HISTORICAL_SESSION_GAPS
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

All 968 exclusions were `broken 15m bucket` errors. Their broker-label clock distribution was initially dominated by:

```text
14:00 = 887 / 968 = 91.63%
```

After applying only the documented historical 14:00 clearing rule, the controlled rerun produced:

```text
candidate_prediction_days=1027
live_bridge_eligible_prediction_days=80
live_bridge_excluded_prediction_days=947
live_bridge_coverage=0.07789678675754626
19:00 = 828 / 947 remaining exclusions
```

The 19:00 pattern is the next dominant systematic boundary.

## External Schedule Evidence

Official Moscow Exchange materials document the historical derivatives-market schedule before the unified session as:

```text
14:00-14:05 Moscow time = intraday/intermediate clearing session
18:50-19:05 Moscow time = evening/main clearing session
19:05-23:50 Moscow time = evening additional trading session
```

Moscow Exchange also announced that on 2026-03-23 the derivatives market would move to a unified trading session, intermediate clearing would be cancelled, and the main clearing session would move from the historical 18:50-19:05 interval to the end of the trading day.

Official references used for this policy:

- Moscow Exchange futures parameters: main trading 10:00-18:50, evening trading 19:05-23:50, with clearing at 14:00-14:05 and 18:50-19:05.
  - https://www.moex.com/a7211
- Moscow Exchange announcement dated 2025-11-05: derivatives market unified trading session starts 2026-03-23 and intermediate clearing is cancelled.
  - https://www.moex.com/n95054
- Moscow Exchange unified-session announcement: transition between sessions becomes continuous; the main clearing session moves from 18:50-19:05 to 23:50-00:30.
  - https://www.moex.com/n98363

The observed 14:00 and 19:00 historical data patterns are therefore classified as documented market-session boundaries, not missing-data defects, for the exact governed interval below.

## Governed Interval

The bridge recognizes only these historical incomplete aligned 15m buckets as expected:

```text
trade_date < 2026-03-23
bucket_label = 14:00 Europe/Moscow
OR
bucket_label = 19:00 Europe/Moscow
```

Rationale for 14:00:

- the aligned 15m bucket labeled 14:00 requires 5m bars at 14:00, 14:05, 14:10 under the existing runtime convention;
- the historical 14:00-14:05 exchange pause makes that aligned bucket structurally incomplete.

Rationale for 19:00:

- the historical main session ended at 18:50;
- clearing ran 18:50-19:05;
- evening trading resumed at 19:05;
- therefore an aligned 19:00/19:05/19:10 triple cannot represent a continuous tradable 15m interval across the clearing pause.

For both governed boundaries:

- no synthetic 15m bar is created;
- no 5m bar is synthesized, shifted, filled, or deleted;
- real bars after clearing remain untouched;
- the next fully aligned bucket is aggregated normally.

## Fail-Closed Boundary

This policy does **not** permit general gap tolerance.

The bridge still raises `LiveShadowBridgeError` for:

- any broken 15m bucket at a clock label other than 14:00 or 19:00;
- a broken 14:00 or 19:00 bucket on or after 2026-03-23;
- duplicate/non-increasing 5m timestamps;
- invalid OHLCV;
- missing complete aligned 15m history overall.

No bars are synthesized, forward-filled, back-filled, timestamp-shifted, or deleted from Level/Structure inputs.

## Runtime Impact

The change is date-bounded to historical sessions before 2026-03-23. Current live 2026 post-unified-session behavior remains fail-closed under the same alignment rules as before.

S7.2 must be rerun against the exact accepted Phase-3 manifest. Any remaining exclusions after applying the documented 14:00 and 19:00 clearing policies remain unavailable prediction days and require separate classification before empirical acceptance.

## Acceptance Criteria

```text
DOCUMENTED_1400_GAP_CLASSIFIED=yes
DOCUMENTED_1900_GAP_CLASSIFIED=yes
PRE_2026_03_23_1400_BUCKET_SKIPPED_WITHOUT_SYNTHESIS=yes
PRE_2026_03_23_1900_BUCKET_SKIPPED_WITHOUT_SYNTHESIS=yes
POST_2026_03_23_1400_GAP_FAIL_CLOSED=yes
POST_2026_03_23_1900_GAP_FAIL_CLOSED=yes
OTHER_BROKEN_BUCKETS_FAIL_CLOSED=yes
REAL_5M_BARS_DELETED=no
OHLCV_IMPUTED=no
CURRENT_LIVE_DATE_SEMANTICS_RELAXED=no
UNIT_TESTS=pass
FULL_REPOSITORY_CI=pass
EMPIRICAL_S7_2_RERUN=pending
```
