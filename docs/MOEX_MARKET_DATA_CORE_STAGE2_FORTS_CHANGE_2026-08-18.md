# MOEX Market Data Core — Stage 2 FORTS Change

Date: 2026-08-18
Status: quotes_pilot_passed_futoi_apim_revalidation_required

## Scope

Stage 2 prepares canonical FORTS Quotes and FUTOI lanes for:

- `usdrubf_futures_family` → `USDRUBF`;
- `cnyrubf_futures_family` → `CNYRUBF`;
- `si_futures_family` → explicit current contract `SiU6`;
- `cr_futures_family` → explicit current contract `CRU6`.

For `SiU6` and `CRU6`, observed expiration and last-trade date is `2026-09-17`. Automatic roll selection is not enabled.

## Quotes evidence — accepted

Server pilots on 2026-08-18 proved authenticated MOEX AlgoPack FORTS tradestats for `USDRUBF`, `CNYRUBF`, `SiU6`, and `CRU6`.

Canonical Quotes source:

- source_id: `moex_algopack_fo_tradestats_5m`;
- contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`;
- host: `apim.moex.com`;
- authentication: Bearer `MOEX_API_KEY`.

The one-date server pilot for `2026-08-17` passed for all four identities with 203 rows per instrument and zero duplicate, gap, null-OHLC, or invalid-OHLC counts.

Canonical Quotes storage:

`${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Canonical Quotes pointer:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

## FUTOI evidence correction

The original Stage 2 FUTOI availability conclusion was invalid.

A public MOEX endpoint probe had been interpreted as available because the response contained one row. Server validation later showed that the `futoi` block for the tested public transport contained only the column `ERROR_MESSAGE`; the row was an error payload, not FUTOI data. Therefore the previous FUTOI `available/completed` evidence is withdrawn.

FUTOI must not use public `iss.moex.com` transport or public ISS fallback.

Canonical FUTOI source after correction:

- source_id: `moex_algopack_futoi`;
- contract: `contracts/sources/futures/moex_algopack_futoi.v1.yaml`;
- host: `apim.moex.com`;
- authentication: Bearer `MOEX_API_KEY`;
- exact route namespace: `/iss/analyticalproducts/futoi/securities/{TICKER}.json` on the APIM host;
- response block: `futoi`;
- explicit request fields: `from`, `till`, `latest=1`;
- public ISS transport: forbidden;
- public ISS fallback: forbidden;
- `ERROR_MESSAGE` payload: fail closed.

The `/iss/...` path above is the MOEX API route namespace on `apim.moex.com`; it does not authorize public `iss.moex.com` transport.

The four registry ticker bindings remain `usdrubf`, `cnyrubf`, `si`, and `cr`, but their FUTOI `availability_status` and `probe_status` are reset to `not_checked`. APIM availability must be revalidated before any FUTOI pilot or materialization can be accepted.

Canonical FUTOI storage remains separate from Quotes:

`${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Canonical FUTOI pointer contract:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

## Fail-closed state

- FUTOI APIM availability: revalidation required;
- FUTOI pilot readiness: false;
- FUTOI materialization flags: false;
- backfill readiness: false;
- accepted pointer readiness: false;
- observed-source refresh readiness: false;
- scheduler: disabled;
- D1/W1 derivation: disabled;
- research: disabled;
- automatic Si/CR roll: disabled.

## Next acceptance action

1. validate repository correction and APIM-only guard in CI;
2. merge the correction to `main`;
3. apply exact merged SHA on server;
4. probe authenticated APIM FUTOI for `usdrubf`, `cnyrubf`, `si`, and `cr`;
5. promote only identities whose APIM payload passes the required FUTOI schema;
6. run one-date FUTOI pilots only after that promotion.
