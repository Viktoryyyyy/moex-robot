# Futures D1 date-source compatibility

Task: `futures_d1_date_provenance_compatibility_v1` / #571.
Base: `fd0bf213dc1dc1ea6e9ecac11230975c729cbf19`, after separate #570/#562 delivery.

The daily D1 reader rejected a real mixed inventory because it required XML
everywhere and stamped XML onto every result. Continuous raw readers also used
that assumption, while the roll producer already emitted observed-date provenance.
The actual paths are raw → derived D1 and raw → continuous 5m → continuous D1;
derived raw D1 is an eligibility prerequisite, not the continuous price input.

## Admission and evidence meaning

`date_source_provenance` checks retained admission metadata, not historical
cryptographic attestation of an exchange response. Existing raw and admission
schemas did not preserve raw content hashes. New hashes identify the bytes read
by this derivation; they cannot retrospectively prove historical bytes or PIT.

Both `canonical_apim_futures_xml` and
`authoritative_observed_algopack_tradestats` require exact RFUD schema,
instrument/family/date/partition identity, AlgoPack source and instrument endpoint,
valid OHLCV/timestamps, no duplicate raw key and one label per derived day.
Observed rows additionally require the explicit source instrument column. Old XML
rows without that column retain their narrower endpoint-based identity meaning.

Admission uses both retained layouts:

- `all_universe/quality/raw_5m_backfill` plus the matching chunk manifest:
  schema, run/chunk/eligibility identity, instrument/family/date range, output
  membership, completed source, successful quality and zero errors must agree.
  A successful instrument in a partial chunk is allowed; failed/skipped/deferred
  instruments are rejected. Historical cumulative path lists are filtered by
  exact instrument/family. `started_at` is a run ID, not a timestamp.
- `quality/raw_5m_loader` plus its loader manifest: the original run ID,
  exact ingest time, instrument summary, endpoint and calendar summary must agree.
  This adapter admits existing selected partitions; it does not expand selection.

For each candidate witness, the complete instrument partition cohort must still
exist with a consistent ingest, row count and min/max timestamps. Raw newer than
the manifest is rejected. Missing, replaced or inconsistent evidence fails closed.
This deliberately cannot recover some historical cohorts whose inputs or
manifests were overwritten; an older pass with the same path is insufficient.

Admission freezes one Moscow-date cutoff. A trade date must have elapsed before
that cutoff. This is not evidence of complete sessions or final source revisions.
Gaps are not filled; unknown gap counts remain null. XML historical missing-day
counts remain visible, including when the original producer accepted them.
An observed-date set never establishes a complete exchange calendar.

## Propagation and compatibility

New D1 and continuous outputs carry `calendar_denominator_status` and
`date_source_evidence_json`. The latter preserves partition identity, raw ingest,
raw/quality/manifest hashes, run identity, missing/gap information and the explicit
limited evidence scope. Different dates may retain different labels. Conflicting
labels in one derived symbol/date group fail; neither first-label selection nor
OHLC aggregation may hide malformed inputs.

Continuous also carries `roll_date_source_status` and `roll_date_source`, distinct
from raw provenance. Supported roll pairs are exactly:

| Status | Source |
|---|---|
| `canonical_apim_futures_xml` | `MOEX_APIM_XML:/iss/calendars` |
| `authoritative_observed_algopack_tradestats` | `moex_algopack_fo_tradestats_5m:/iss/datashop/algopack/fo/tradestats.json` |

Quality and manifests retain per-date/per-partition evidence and honest mixed
summaries. Existing continuous v1 artifacts without the additive fields remain
readable with provenance unrecorded; their historical meaning is not rewritten.
Malformed partial new fields are rejected. Source/eligibility/roll/expiry selection
and aggregation arithmetic are unchanged. No producer, collector or scheduler is
added, and canonical ingestion migration remains a separate workstream.

The Slice 1 compatibility runner validates the actual retained roll map's
status/source pairs against the continuous manifest. New observed or mixed
manifests require a matching roll summary; old XML manifests without the additive
summary remain readable when their retained roll map agrees.

Admission and D1 validation precede output mutations. A failed retry preserves
previous artifacts. Identical derived partitions are not replaced just to change
their ingest clock; changed valid outputs retain the existing partition semantics.
Prior raw, quality and producer manifests are never edited by these readers.

## Scope and verification

One implementation owner; independent review is read-only. The affected-file
manifest and amendments were posted to #571 before editing: the shared admission
helper, six existing readers/quality modules, three test files, nine directly
affected dataset contracts and this document. #371 overlaps only the raw compatibility contract;
its collector/registry work is not copied.

Regression coverage exercises real Parquet raw/admission → D1 → continuous
boundaries, both retained layouts, mixed histories, rejected labels/identity/
chronology/evidence, unknown gaps, duplicates, unclosed dates, retry preservation,
quality/manifest agreement and a real failing D1 subprocess through the daily
orchestrator. Exact-head CI/review and controlled runtime results are recorded in
the delivery PR. A bounded controlled run does not establish that a later natural
scheduled run succeeded or that all historical inputs can be admitted.
