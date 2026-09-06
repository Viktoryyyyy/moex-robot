# Explicit-contract reconciled research history

`python -m moex_data.rub_reconciled_history --run CAPTURE_DIRECTORY --evidence GAP_EVIDENCE_JSON --output NEW_JSON`

The builder verifies the isolated capture, rechecks each partition hash after
reading it, and binds minute evidence to the captured neighboring OHLCV bars.
It applies the explicit `explicit_iss_ohlcv_repair_research_only_v1` policy:
complete five-minute ISS reconstructions become separately sourced research
bars. Empty intervals remain coverage records. Original TradeStats files and
accepted pointers are untouched. Output creation is exclusive.

Each research bar retains source identity and actual acquisition time. The
output binds the capture manifest and evidence by SHA-256. Recovered bars have
unknown value and trade count; any affected aggregate retains null for those
fields rather than publishing a partial sum. These data do not supply OI.

H1 uses right-labelled Moscow clock hours: a 10:00 interval end belongs to the
09:00–10:00 hour. FULL_CLOCK_HOUR requires all twelve five-minute positions,
including explicitly corroborated empty intervals. Other hours and all D1/W1
rows are OBSERVED_WINDOW_ONLY. Weekly rows include partial weeks, always
marked week_complete=false. Consumers must not treat these previews as accepted
daily/weekly series. Availability is no earlier than acquisition of the bars
and corroboration evidence; this backfill is not historical point-in-time data.

The boundary report shows observed first/last bar ends for every requested
date. It does not infer exchange sessions or closures from weekdays or from
source emptiness. Authoritative historical session calendars and boundary
verification remain prerequisites for complete daily and weekly acceptance.
The existing native USDRUBF/CNYRUBF HTF contract is not extended by this module.

On 2026-09-06, isolated server builds from July 20–September 5 captures produced
8,083 SiU6 bars and 8,066 CRU6 bars, adding two ISS bars to each. Nineteen CRU6
empty intervals were preserved as coverage only. Both manifests and all 88
captured daily partitions were verified before reading. No model readiness,
continuous-family roll readiness, or trading permission is granted.
