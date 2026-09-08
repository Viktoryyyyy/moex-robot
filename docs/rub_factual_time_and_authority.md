# Canonical factual time and permission consistency

CBR key-rate and RUONIA observations previously exposed legacy policy boundaries
as `published_at`. The canonical API now preserves the boundary separately as
`policy_available_not_before`, sets publication time to unknown, and uses the
actual `ingested_at` receipt for system availability. The RUONIA source publication
date remains a date; the key-rate effective date is not a publication date.
Legacy research adapter contracts are unchanged; this change grants no historical
PIT acceptance or immutable-source admission. Numeric observations are preserved.

Read-time FUTOI checks also revoke component and top-level permissions when current
factual evidence is absent, including snapshots without an authority block. Existing
blocked data is not promoted. A current valid pair still does not admit a prior
session or full intraday history.

Basis factual permission can legitimately describe one admitted metric while the
complete requested scope remains PARTIAL. The source matrix exposes
`factual_context_usable` and `admitted_metric_ids` separately from full-forecast
usability. This distinction preserves valid partial facts and avoids presenting
an empty READY container as admitted evidence.

Production verification must confirm canonical CBR fields, basis metric identifiers,
missing-evidence FUTOI regression tests and the remaining incomplete forecast state.
Training, model evaluation, order submission and third-party messages remain disabled.
