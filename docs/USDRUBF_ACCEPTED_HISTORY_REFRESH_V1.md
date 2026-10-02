# USDRUBF accepted history refresh

PROJECT=MOEX_Bot
Task: usdrubf_accepted_history_refresh_v1

## Scope recorded before source changes

Base/main/server: 2d2cc1a792a0870617792b67e817cf839893d325.
Single mutation owner: this Codex task. Independent read-only review is separate.
Branch: codex/usdrubf-accepted-history-refresh-v1.
Scope: canonical accepted_quote_history current-history resolver, observation
adapter/default CLI observation template, bounded-buffer option in the existing
Stage7 D1 materializer, regression tests and this runbook. The latter two source
files were also checked against the same open-PR inventory before modification.
No collector, scheduler, training,
trading, forecast registration or historical pointer mutation.
All 27 open PR file lists checked on 2026-10-02. No overlapping source files;
PR562 changes handoff documentation/examples, PR371 ingestion contracts,
PR369 FUTOI, remaining old registry/management/research PRs are separate.

## Cause and normal update path

Stage2 content attestation is deliberately a fixed four-instrument baseline.
The old observation reader only admitted that baseline. Stage10 already checks
and freezes newer raw 5m partitions and advances accepted Stage7 D1. The missing
link was reader access to those admitted immutable deltas.

The current reader reuses that existing admission, verifies exact raw SHA
lineage against accepted D1, successful promoted Stage10 parents, source dates,
lineage manifests, physical partition quality and reconstructed D1 values.
Missing or conflicting deltas fail closed. No weekdays establish trading dates.
No extra writer or manual pointer update is necessary: normal Stage10 publication
automatically advances the next read. The original fixed Stage2 mode is unchanged.

Use observation reader configuration `{"mode":"accepted_current",
"data_root":"/home/trader/moex_bot/data"}`. It resolves the currently admitted
range; fixed accepted_start_date/accepted_end_date overrides are rejected.
Every factual capture stores exact admission anchors and selected raw objects.
Existing observation retry/reproduction uses those frozen objects, not a new
current read. Calling accepted_facts directly can probe real bars without
registering a forecast or running an evaluation.

Closed dates must precede the current Moscow date, D1 availability and successful
parent finish must not exceed the reader clock, and individual future bars are
excluded. Intraday absence is not filled; the evaluator still uses the explicit
declared observation grid and reports unavailable facts honestly. Historical
publication time remains unknown; this is not historical point-in-time proof.

## Operational limits

The current resolver bounds run discovery to 4096 directories, each evidence
file to 8 MiB and retained evidence to 128 MiB. Exceeding a bound fails closed.
It does not create a second collection schedule. Future provider availability,
systemd execution and successful Stage10 quality/admission remain prerequisites;
a successful one-time reader probe does not prove indefinite regular operation.
