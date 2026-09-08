# Macro facts and scheduled publications

`factual_release.macro_evidence_inventory` is a reproducible projection of the
existing verified CBR and Rosstat components. It replays their archived evidence
at the release's explicit consumption time and excludes expired, changed,
failed or malformed inputs. It does not fetch new data or reuse a cached
inventory's admission flags. Direct descriptions use the read-time reference,
or snapshot generation time when that reference is absent; `build(..., now=...)`
and the API use consumption time.

Facts retain source units, observation periods, effective dates, publication
dates and receipt times separately. A key-rate effective date is not a decision
announcement timestamp. RUONIA's observation date is not its publication date.
Unknown publication hours remain null. CPI index bases remain separate, including
null bases in source documents that do not report them.

The next Rosstat weekly CPI publication is a separate `SCHEDULED` event backed
by the archived index calendar. Its scheduled day and Moscow timezone do not
prove an actual publication or an exact publication hour. A failed calendar
replay removes the event. Consensus and surprise are unknown.

The inventory lists the four macro blocks already required by the production
matrix: CBR rates, Minfin FX operations, Rosstat macro and the event calendar.
Missing evidence and unresolved series/vintage policies remain explicit gaps.
The registered banking-liquidity source is a policy gap; it does not become an
accepted numerical series merely because it exists in the registry.

The inventory does not complete the required-series policy, the full macro or
event-calendar blocks, historical PIT acceptance, D1/W1 alignment, model
acceptance or trading authority. Matrix requirements remain unchanged.

`requirements_policy` pins an engineering minimum of eight requirements across
the existing four blocks. It reuses registered CBR source identities and keeps
the latest Minfin FX/gold announcement as an explicit open requirement.
Monthly CPI and four banking-liquidity metrics are collected as received dated
context; historical vintages, tax events, the global calendar, H.10 and consensus
remain unresolved. This minimum is not an exhaustive
forecast-factor definition.

Monthly CPI retains separate indices against the previous month, previous
December and the same month of the previous year. Percentage changes are the
printed index minus 100; cumulative-average indices are not year-on-year CPI.
The monthly archive does not provide a verified publication date/hour; a date
in a filename is not used as publication evidence. Latest listed monthly data
remain usable as dated context with this limitation.

Banking liquidity retains the four published beginning-of-day values and their
printed precision. The current view may contain revisions. An informational
arithmetic residual is exposed without correcting the source numbers or claiming
an error bound. `uncertainty` separates printed granularity, unknown publication
times and revision limitations; it supplies no probabilities, numerical accuracy
bounds or forecast weights. Verified limited facts remain available, while
failed provenance/replay and expired receipts still revoke admission.

`requirements_coverage` links currently admitted facts or scheduled events to
that checklist; it does not grant full requirement acceptance. In addition to
Rosstat, the [received CBR meeting schedule](cbr_meeting_calendar.md) supplies
separate planned events. A valid upcoming CBR entry removes only the missing
CBR-schedule evidence marker. The global calendar requirement remains open.

Frozen exports include the inventory in `release.json`; replay requires the
original snapshot and referenced evidence files, exact code revision and the
same consumption time. The inventory's availability is not a forecast.
