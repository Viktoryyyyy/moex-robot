# Source observations when the calendar is unavailable

On 2026-09-07, server probes of /iss/calendars.json and /iss/calendars/futures.json returned HTTP 200 with text/html rather than calendar JSON. This records those two responses, not an assertion that every MOEX calendar service is unavailable.

The live factual path has no calendar dependency. The API now includes source_observation_context for all seven expected instruments. It distinguishes fresh source rows, stale rows, rejected rows and unknown/invalid source timestamps. These are observations of updates, not proof of a recent trade or of an open/closed session. The existing quality and 60-second source-age gates remain authoritative.

Every instrument's session_state remains UNKNOWN until separate authoritative session evidence is accepted. Missing rows do not prove a nontrading day; stale spot does not prove the exchange closed; fresh Sunday rows are not rejected just because it is Sunday. Native trade dates and opaque trading-status codes are preserved without inventing a date from the clock or interpreting unverified status codes. Source time and last-trade time remain separate.

There is no API calendar fetch, weekday fallback, holiday guess, TTL increase or new model/trading authority. Last completed FUTOI session remains based on observed instrument-specific tradestats dates, with exact-date FUTOI retrieval and the existing source validation. This does not supply future session schedules or prove full historical calendar coverage.

## CR acceptance checkpoint

A frozen replay on 2026-09-07 18:32 UTC reproduced the current CR factual payload from raw partition e3e0768ef39177bf9e3f8511049472b15ccce668b10a158c0804633ae7254ada (source event 18:25 UTC, net balance zero). The same unchanged latest_aligned_factual validator rejected archived failed partition 5dc08b3cf6523d8e56f062c3c1cda4130759ee5a66e941c245ec138f47e1a0ab with the original net balance error. Both file hashes were verified.

This accepts neither the rejected pair nor the whole intraday history. CR governance remains blocked pending its explicit acceptance decision and policy for inconsistent source publications. Si factual acceptance remains unchanged. Provider root cause has not been established. Training remains paused.

Next: accept evidence-backed session intervals/closures through an independent reviewed source or explicit versioned calendar artifact; retain UNKNOWN outside proven coverage. Do not make completion depend on the two nonfunctional URLs.
