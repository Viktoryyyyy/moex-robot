# Temporal applicability of RUB factual inputs

The API `temporal_applicability` view separates three meanings without granting
additional authority or reinterpreting missing data as a neutral factor.

* Current FUTOI pair: persisted READY/consumer admission plus causal source event,
  publication, availability, ingest and receipt, each no older than the existing
  1200-second heavy lifetime. Expiry, future timestamps, a failed attempt or retained
  data revoke current Si and CR authority at read time. A fresh previous-date fact
  cannot keep current authority alive. Fast quotes retain their independent 60-second gate.
* Previous observed date: a dated observation requires a FRESH record, a receipt
  within 1200 seconds, and a matching instrument-specific observed-date witness.
  The legacy field `previous_completed_session` is retained for compatibility but
  does not establish session completion. Its typed meaning is
  `PREVIOUS_OBSERVED_DATE_FACT`, with completion UNKNOWN and current use false.
* Brent published daily price: the existing source and receipt gates remain in
  force. A new receipt does not become the price event time or change the original
  trade date. A dated daily CLOSE is neither an intraday price nor historical PIT acceptance.

The view never upgrades existing consumer gates, modifies archived facts or fetches
data. Previous-date observation availability is descriptive, not a new grant of
previous-session or historical authority. CR's narrower acceptance remains intact.

## Separate schedule evidence review, 2026-09-08

Official publications reviewed:

- https://www.moex.com/n101220?nt=106 — effective 2026-07-14, futures opening auction
  06:50–07:00, morning trading to 10:00, main session to 19:00, evening to 23:50 Moscow time.
- https://www.moex.com/n103379 — effective 2026-09-14, the main session begins at
  09:00; this future change must not be applied to earlier dates.
- https://www.moex.com/ru/tradingcalendar?type=trading — the retrieved page displayed
  a 2026-09-07 observation, not verified instrument/date coverage for 2026-09-08.

These publications establish schedule rules, not actual open/closed session state
at consumption time. No date-specific calendar coverage or exceptional closures
are accepted by this change. Session state remains UNKNOWN. No dependency on the
two unavailable ISS calendar URLs, weekday inference or inferred weekend closure
is introduced. Separate reviewed instrument/date intervals and exceptions are
still required before any completion/closure claim can be made.

## Published plan view

`temporal_applicability.published_schedule_plan` describes the published plan for
Si and CR futures families on explicitly listed Moscow civil dates, 7–14 September.
The SHA-256 pinned artifact is `contracts/intelligence/rub_published_schedule_2026-09-08.json`.
Its review timestamp is the knowledge boundary: earlier reads remain UNKNOWN.
The July hours apply through 11 September; the September rule begins on the 14th.
The published weekend exclusions on 12–13 September come from
https://www.moex.com/n95564?nt=112. No weekday or weekend fallback is used beyond
the enumerated dates. Intervals include their start and exclude their end.

`published_plan_covered` only means that this dated plan is present. It does not
assert that later exceptional announcements were exhaustively checked, that
trading actually occurred, or that a session completed. `PUBLISHED_NO_SESSION`
and `OUTSIDE_PUBLISHED_INTERVALS` are plan descriptions, never actual CLOSED.
The actual session state and previous-date completion remain UNKNOWN. Consumer
authority, quote lifetimes, historical PIT acceptance and execution are unchanged.
Civil dates are not reinterpreted as exchange trading dates; this matters for
weekend sessions assigned to the next trading day. Missing or altered artifacts
produce UNKNOWN, without a network request. The plan requires a new reviewed
artifact to extend its finite coverage or incorporate later announcements.
