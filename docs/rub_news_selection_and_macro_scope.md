# News selection and macro scope

The factual snapshot selects at most 20 news events in rounds: newest eligible
event from each source, then the next round until the budget is exhausted.
It keeps at most one latest event per existing cluster and refuses conflicting
event identities or invalid causal timestamps, including candidates outside the
display limit. Invalid-quality events cannot enter the selection. Source
coverage is a deterministic policy, not a claim of RUB relevance or importance.
If sources outnumber slots, the reported eligible and represented counts differ.

Every candidate's existing normalized event metadata and the selected IDs are
frozen by SHA-256 in `raw/news_selection`. The API summary references that audit,
so omitted events remain reviewable. Raw bodies and headlines are not retained
by this change. The analysis view keeps UNKNOWN impact and no directional
authority. A 100-event human-reviewed relevance/deduplication corpus remains
pending; the synthetic source-flood regression is not that acceptance corpus.
The existing clustering threshold and provenance validation are unchanged.
Legacy shadow calls without an audit root keep their existing recency policy.

The CBR component explicitly declares its scope as key rate and RUONIA only.
Missing Minfin FX operations, Rosstat macro and release calendar are listed.
The deterministic macro direction placeholder is shown as UNKNOWN with no
confidence or drivers, while actual observations are preserved. Numerical
release surprise remains null without verified consensus. This is a factual
coverage correction, not acceptance of the missing providers or a model result.
