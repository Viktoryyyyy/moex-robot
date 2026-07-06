# EMA 3/19 AI — Availability and Leakage Rules

Status: design artifact only  
Lane: `ema_3_19_ai`  
Runtime impact: none  
Modeling status: blocked

## 1. Purpose

This document defines availability and leakage rules for the EMA 3/19 AI market phase research path.

It is a design/spec artifact only. It does not authorize modeling, calculations, ingestion, backfill, runtime behavior, server apply, tests, contracts, configs, implementation code, or Route B / n8n changes.

## 2. Forecast anchor

The forecast anchor timestamp is:

```text
forecast_anchor_ts = 06:00 Europe/Moscow
```

All prediction simulations and runtime-oriented wording must evaluate feature availability relative to this anchor unless a later approved contract supersedes it.

## 3. D1 T+1 06:00 rule

Daily D1 data for date `T` may be used only from:

```text
T+1 06:00 Europe/Moscow
```

This rule applies unless a stricter source-specific timestamp is proven.

Same-day close data or post-close derived data must not be used before the approved availability timestamp.

## 4. Feature availability rule

Every feature row used in prediction must satisfy:

```text
availability_ts <= forecast_anchor_ts
```

If this condition is not satisfied, the feature row must not be used for that prediction anchor.

If source availability is ambiguous, the design must either:

- apply a conservative lag assumption; or
- exclude the feature until point-in-time availability is proven.

## 5. Point-in-time joins

All source joins must be point-in-time joins.

A joined feature must represent only information that was available as of `forecast_anchor_ts`.

The join must not pull corrected, revised, filled, or future-known values into earlier anchors unless the revision history itself is modeled point-in-time.

## 6. Source availability timestamps

Every source used as a feature source must have either:

- explicit source availability timestamps; or
- documented conservative lag assumptions.

A feature without an availability timestamp or conservative lag rule is not eligible for modeling.

## 7. Calendar rules

Calendar information must be separated into schedule knowledge and post-fact outcomes.

### 7.1 Allowed before anchor

The following may be used if known before `forecast_anchor_ts`:

```text
scheduled_event_known_before_anchor
```

This includes event existence, scheduled time, known agenda category, or other pre-anchor schedule metadata, subject to source availability proof.

### 7.2 Not allowed before availability

The following must not be used before their actual availability:

- post-fact outcomes;
- realized decisions;
- realized rates;
- meeting results;
- official releases published after the anchor;
- post-event narratives;
- retrospective explanations.

Calendar outcome leakage blocks modeling.

## 8. Label and annotation availability boundary

Label / annotation availability must not be included in a source availability matrix as model features.

Manual labels are supervised targets `y` for offline training and evaluation only.

Manual labels must not become:

- features;
- input fields;
- runtime state;
- assistant-visible state;
- source availability rows;
- production context.

## 9. No-lookahead controls

The following are forbidden:

- future labels;
- future OHLCV;
- future returns;
- same-day close values before availability;
- post-fact macro values before release;
- corrections or revisions unless modeled point-in-time;
- leakage from manually adjusted labels into features;
- calendar outcomes before availability;
- availability assumptions that use knowledge from the future.

## 10. Modeling gate

Any unresolved leakage violation blocks modeling.

A modeling task may not start until availability and leakage rules are represented in an approved implementation scope and validated under point-in-time conditions.