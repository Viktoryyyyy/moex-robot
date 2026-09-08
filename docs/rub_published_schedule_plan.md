# Reviewed published Si/CR plan through September 21

The v2 plan adds explicit civil dates September 15–21, 2026. On September
15–18 and 21 the morning trading period is 07:00–09:00, the main period is
09:00–19:00 and the evening period is 19:00–23:50, with the opening auction
06:50–07:00. September 19–20 have the published weekend auction 09:50–10:00
and additional trading period 10:00–19:00. Intervals are Moscow local time,
with exclusive ends. September 12–13 remain explicit published exceptions.

The evidence is the combination of official MOEX announcements:

- https://www.moex.com/n103379 — effective September 14 regime and weekend hours.
- https://www.moex.com/n95564?nt=112 — 2026 weekend sessions and exceptions.
- https://www.moex.com/n101220?nt=106 — currency futures eligible for weekend sessions from July 18.
- https://www.moex.com/n94172 — holiday exceptions and standard remaining dates.

All four pages were fetched over verified HTTPS on September 8, 2026 between
08:23:19 and 08:23:27 UTC. Raw HTML and canonical JSON receipt files are in the
delivery artifact `outputs/calendar-20260908`, named by SHA-256. The tracked
`rub_schedule_sources_2026-09-08.json` manifest records exact source URLs,
request/receipt times, sizes, raw hashes and receipt hashes. The v2 plan binds
this manifest by SHA-256; the reader pins the complete v2 plan hash.

The raw archive is not loaded by the online reader and is not committed to
the repository. Therefore the response explicitly retains
`source_archive_replayed=false`; the verified archived receipt metadata is
provided for independent offline inspection. Missing or modified source
manifests fail closed. A broken applicable update does not fall back to the
older plan. Source hashes alone do not certify future unchanged schedules.

For consumption before the v2 review at `2026-09-08T08:24:24.787012+00:00`,
the original pinned plan remains applicable. The new review is never
backdated. Outside the explicitly enumerated civil dates, the reader returns
UNKNOWN rather than extrapolating weekday rules.

This is a published plan only. It cannot establish actual opening, absence
of an interruption, completed sessions, trading targets, historical PIT
acceptance or later emergency schedule changes. These permissions remain
false and actual session state remains UNKNOWN. No future exception absence
is asserted. Further updates require another reviewed and archived finite
plan; collecting actual session-state evidence is a separate task.
