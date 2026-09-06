# FUTOI recurring acceptance verification — 2026-09-06

The automatic Stage10 service ran on 2026-09-06 at 00:30 MSK and exited
successfully at 00:34:31 MSK. Its run ID is
`step10_daily_20260906_213000`; the requested completed date is 2026-09-05.
The run ID combines a Moscow date with a UTC clock.

Both Si and CR candidates report quality PASS and source freshness FRESH
for 2026-09-05, with 218 raw rows each. Their quality-report and refresh-manifest
SHA-256 values still match. However, neither canonical raw partition matches
the SHA recorded by that run. Subsequent refreshes replace those partitions.
Therefore the existing scheduled run cannot satisfy recurring acceptance.
This is a provenance retention defect, not evidence of invalid position values.

The source-native factual producer now archives verified raw bytes, quality
reports and refresh manifests by content hash, and reads the frozen raw file
when deriving its factual payload. It also preserves each complete run result
before updating current.json. A changed source or corrupted existing archive
fails closed. Historical missing bytes are not reconstructed or re-attested.

Factual live acceptance also no longer implicitly enables Stage5 pointer
promotion. That requires an additional explicit boolean
`authority.stage5_promotion_authority`, defaulting to false.

The recurring gate and CR smoke gate remain blocked. Acceptance requires a
new automatic scheduled run using the corrected producer, verification of all
archived hashes, and separate CR acceptance. A manual production smoke run
validates this repair but must not be presented as scheduled-run evidence.
Directional, action and buy/sell authority remain disabled.

Regression coverage includes canonical-file replacement, corrupt archives,
changed bytes before archival, Si/CR provenance preservation, archived run
results, and explicit Stage5 permission with passed and blocked gates.
