# Rosstat weekly CPI document replay

`moex_research.external_data.rosstat_weekly_cpi` reads a SHA-256-addressed
receipt produced by the verified HTTPS client and checks its raw HTML hash,
source route, pinned CA identities and request/receipt/consumption chronology.
The parser requires one weekly estimate title, a matching summary and a
matching national CPI estimate table with identified reference columns.
It preserves three separate base-100 indices (previous registration, month
start, year start). The weekly percent change is exactly index minus 100.
The month-to-date estimate is not final monthly CPI.

The initial supported format is a same-month period of at most ten inclusive
calendar days. Cross-month publications and layout changes fail closed until
their actual formats are reviewed and covered by tests. Script/style content
is ignored; ambiguous summaries and conflicting table values are refused.

The fixture is an exact title/summary/first-table excerpt from
https://rosstat.gov.ru/storage/mediabank/134_02-09-2026.html,
whose full received HTML SHA-256 is
`1e308b8c32e6df0f5163e3119adb5b3ae501b9760d172f5ff89537774c6adb75`.
The excerpt is a parser fixture, not a production acquisition receipt.

Run with `--manifest PATH --manifest-sha256 HASH --as-of AWARE_ISO_TIME
--output DIRECTORY`. The resulting JSON is frozen under its own SHA-256.
Each changed source vintage remains separate. Availability is actual receipt
time; the filename is never interpreted as publication time. There is no
network request, model run, trading action or change to the current API.

This is document semantic validation only: no latest-release selection,
publication timestamp, calendar acceptance, consensus surprise, historical PIT
acceptance or forecast alignment is inferred. Consumer/factual authority stays
false and the production macro completeness blocker remains. Old evidence can
be replayed for audit, which does not make it a current accepted observation.
