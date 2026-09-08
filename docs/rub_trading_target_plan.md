# Published target candidates and unresolved forecast definitions

The read API adds `trading_target_plan`, based on freshly replayed authenticated
MOEX calendar rows and the separately reviewed finite published schedule.
It does not change `target_trading_date` or `target_date_proven` in factual releases.

The new candidate policy describes the next trading day whose first published
session has not yet started. It is not an accepted forecast/label definition.
Calendar civil dates mapped to the same trading day are grouped before comparing
their earliest opening with the read time. The September 19–20 additional sessions
belong to September 21: that day is already started at the Saturday opening,
including at 06:00 on Monday. Friday evening belongs to Friday. These relations
come from explicit calendar rows, without weekday extrapolation.

The candidate requires agreement between calendar flags and reviewed schedule
coverage. Unknown coverage or a conflicting earlier date cannot be skipped in
favor of a later apparently valid candidate. Failed/expired calendar receipts
remove the candidate. Published plans do not prove actual execution or absence
of future emergency changes; those authorities remain false.

Existing research contracts use different entry/exit conventions. Product W1
has not been established as either a calendar-week bucket or five forward
trading days, so neither is selected automatically. Both horizon definitions
remain unaccepted, and no model is trained or evaluated.

Actual completion requires independent source-native state and trading-day
identity. The SPECTRA `FORTS_SESSIONSTATE_REPL.session_state` documentation has
such state semantics, but no verified live stream has been connected by this
change. An ISS history row, settlement price or the passing of scheduled end
time cannot substitute for that evidence.
