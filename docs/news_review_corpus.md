# News review corpus preparation

`moex_research.intelligence.news_review_corpus` takes a frozen selection audit
and its expected SHA-256, then exports an immutable JSON review queue. It samples
at least 100 different existing clusters in source rounds, preserving original
URLs, causal times, content hashes, source provenance and whether an event was
shown in the bounded live view. Existing cluster identity is not a gold label.

All reviewer fields start null. A human must inspect the original source,
confirm actual event identity/deduplication, label RUB relevance and explain the
judgment. The queue contains metadata rather than complete source text; URLs
must be checked against their archived content identities where available.
Human review and relevance acceptance remain false. This is data preparation,
not classifier training, model evaluation or automatic news-impact analysis.

Before scoring any future classifier, freeze reviewed labels and acceptance
criteria independently; do not fill reviewer fields with synthetic approvals.
The current user pause on model evaluation remains in force.
