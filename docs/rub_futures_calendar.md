# Received published futures calendar

`rub_futures_calendar.load(root=DATA_ROOT, env=ENV)` reads the authenticated, fixed
route `https://apim.moex.com/iss/calendars/futures.json` with `show_all_days=1`,
`from` and `till`. The interval is Moscow today minus seven through plus fourteen
civil days, capped at the current year's boundaries. The caller loads the existing
project environment; the module uses `MOEX_API_KEY` without logging it. Requests
uses the configured `REQUESTS_CA_BUNDLE` (or `CURL_CA_BUNDLE`), or standard verified
TLS. Redirects are refused, responses must be JSON and at most two million bytes.

The source block is `off_days`, with exact columns tradedate, is_traded,
trade_session_date, reason, updatetime. Every queried civil date must occur exactly
once. The strict integer source flag determines whether the plan includes trading;
in particular an N reason with is_traded=0 remains a nontrading published date.
Recognized reasons are H/W/N/T. H requires is_traded=0 and T requires is_traded=1;
unknown or contradictory reasons fail closed. No weekday heuristics are used.
Only W rows may map forward to another civil date;
the destination must occur within this response and have is_traded=1 and reason
other than W. Missing destinations at a bounded/year-end edge fail closed.

`days` exposes civil_date, is_traded, trade_session_date, reason, source_update_time
and normalized trading_date (null for no planned trading, mapped date for W, otherwise
the civil date). `updatetime` remains the literal nullable source value. Its timezone
and exact publication timestamp are not established. Impossible future source update
dates are rejected; no invented intraday publication time is emitted.

Raw JSON and the normalized receipt manifest are written exclusively under
`raw/external/moex_futures_calendar`, named by SHA-256. Reconciliation verifies both
hashes, exact query identity, full raw replay and causal request/receipt/use times.
Both the receipt and the published-plan admission expire after 1200 seconds. A failed
refresh, altered/missing evidence or prior revocation cannot regain admission by read.

The component name is `futures_calendar`; `calendar_plan_usable=true` applies only
to the `PUBLISHED_CALENDAR_ONLY` scope. All actual-session, completion, forecast-target,
historical, full-forecast and action authority stays false; actual_session_state is
UNKNOWN. A published mapped trading day is not evidence that its sessions happened
or completed. This collector does not call the current-day session-schedule endpoint.
