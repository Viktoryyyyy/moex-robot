# Futures Limited Controlled Batch Classification Contract

Status: provisional controlled rollout contract
Project: MOEX Bot
Scope: limited controlled batch classification only

## Purpose

Define a narrow classification contract for the accepted limited controlled rollout batch:

- W
- MM
- MX

This contract is intentionally isolated from:

- CR/GD/GL pilot rollout
- Slice 1 whitelist behavior
- daily refresh expansion
- continuous eligibility promotion
- full futures rollout

## Input contract

Artifact class: external_pattern

Required snapshot:

- snapshot_date=2026-05-06

Input evidence source:

- rfud_candidates scoped artifacts only

The classifier may read:

- csv
- json
- parquet

from approved rfud_candidates snapshot partitions.

## Allowed families

Exactly these families may be evaluated:

- W
- MM
- MX

No other family may enter the produced controlled batch artifact.

## Preservation requirements

The implementation must preserve:

### Existing pilot

- CR
- GD
- GL

### Existing Slice 1 whitelist

- SiM6
- SiU6
- SiU7
- SiZ6
- USDRUBF

### Explicit non-promotion set

- SiH7
- SiM7

The classifier must not modify existing rollout artifacts or configs.

## Classification semantics

If all are true:

- family is in allowed limited batch
- raw/FUTOI evidence is available
- liquidity/history state is review_required

then classification result must be:

- controlled_provisional

The following are forbidden:

- included
- accepted
- canonical
- production_ready

## Continuous eligibility

Continuous eligibility remains separate and not accepted.

Every produced row must preserve:

- continuous_eligibility_status=not_accepted

The classifier must not:

- trigger continuous builds
- trigger roll logic
- trigger backfill
- trigger daily refresh expansion

## Output artifact

Artifact class: external_pattern

Required output fields:

- snapshot_date
- family
- classification_status
- liquidity_history_status
- raw_futoi_status
- continuous_eligibility_status
- evidence_source
- controlled_batch_id

Required semantics:

- exactly one row per accepted family
- only W/MM/MX may appear
- all rows must be controlled_provisional

## Failure isolation

If one family fails validation:

- remaining valid families may still classify
- invalid family must not implicitly promote others

## Forbidden side effects

Forbidden:

- rollout expansion
- modifying CR/GD/GL state
- modifying Slice 1 state
- backfill
- scheduler activation
- runtime/research/trading changes
- family ranking

## Result status

All outputs produced under this contract are provisional.
