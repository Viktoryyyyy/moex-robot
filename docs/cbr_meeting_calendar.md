# Received Bank of Russia meeting schedule

The factual collector obtains the official calendar from
`https://www.cbr.ru/dkp/cal_mp/` over verified HTTPS. It archives the original
HTML and a receipt manifest without overwriting prior versions. Reconciliation
checks source identity, hashes, the scoped calendar for the current year,
receipt chronology and the 1200-second receipt limit before admitting the
published schedule.

`components.cbr_meeting_calendar` provides schedule context only. The macro
inventory exposes upcoming dated entries as `SCHEDULED`. A meeting date does
not establish a decision, an actual publication or the meeting's starting
time. Planned press-release and press-conference hours, where explicitly
published by the source, are separate fields. Unknown actual timestamps remain
null. Same-day entries are still plans; passing their planned hour cannot prove
that publication occurred.

Stale, failed, retained, malformed or changed evidence revokes schedule use.
Past entries are not inferred to have occurred. Lack of future entries does
not imply a closed market or completion of a year. A newly received schedule
version is not available before its receipt and cannot establish historical
point-in-time features.

This adapter serves the current RUB factual snapshot. It does not activate or
change the older research calendar placeholder's design-only permissions.
Full event-calendar, macro, horizon, model and execution acceptance remain
closed.
