# USDRUBF News/Macro Source Registry v1

PROJECT=MOEX_Bot

Status: source registry contract only  
Canonical machine-readable source: `contracts/intelligence/usdrubf_news_macro_source_registry_v1.json`

## Purpose

Define the first concrete provider set for RUB Intelligence before any live News/Macro acquisition is enabled.

## Source hierarchy

1. **Official primary** — factual authority after source identity, publication timestamp and availability validation.
2. **Official secondary** — confirmation/context within the institution's scope; prefer the primary authority for the underlying act.
3. **Major agency/financial media** — factual use only through an approved route and provider-specific rights policy.
4. **X/Twitter** — high-velocity discovery only. No X post is factual authority in v1.

## Stage 12B ready candidates

### Russia / MOEX

- Bank of Russia press releases RSS — `cbr_press_rss`
- Bank of Russia news/interviews/speeches RSS — `cbr_events_rss`
- Moscow Exchange all-news RSS — `moex_all_news_rss`
- Moscow Exchange FX-news RSS — `moex_fx_news_rss`

Existing macro adapters are reused rather than duplicated:

- `moex_brent_futures_daily`
- `cme_wti_pre_moex`
- `cbr_ruonia_daily`
- `cbr_key_rate_daily`
- `cbr_banking_liquidity_daily`

### United States

- Federal Reserve all press releases RSS — `fed_press_all_rss`
- Federal Reserve monetary-policy RSS — `fed_monetary_policy_rss`
- BLS Employment Situation RSS — `bls_employment_situation_rss`
- BLS CPI RSS — `bls_cpi_rss`
- BLS release calendar — `bls_release_calendar`
- U.S. Treasury press releases — `us_treasury_press_releases`
- OFAC Recent Actions — `ofac_recent_actions`
- White House releases — `whitehouse_releases`

### EU / Energy

- Council of the EU press releases/statements — `eu_council_press_releases`
- European Commission news — `eu_commission_news`
- OPEC official releases — `opec_press_releases`

## Blocked pending source work

These desired sources are not authorized for Stage 12B live ingestion yet:

- Kremlin — stable event route/timestamp adapter required.
- Russian Ministry of Finance — stable route and timestamp semantics required.
- Rosstat — stable machine-readable index and availability policy required.
- Russian MFA — stable news index/timestamp adapter required.
- Reuters — approved acquisition route and rights policy required; no scraping fallback.

## X/Twitter discovery whitelist

### Official / principal accounts

- `@WhiteHouse`
- `@USTreasury`
- `@SecScottBessent`
- `@federalreserve`
- `@BLS_gov`
- `@StateDept`
- `@EU_Commission`
- `@OPECSecretariat`
- `@mfa_russia`
- `@MID_RF`
- `@ZelenskyyUa`
- `@MedvedevRussia`

### Wire discovery

- `@Reuters`

### Fast market squawk

- `@DeItaone`
- `@FirstSquawk`
- `@financialjuice`
- `@Newsquawk`

### OSINT early warning

- `@Faytuks`
- `@sentdefender`
- `@clashreport`

The active v1 whitelist is intentionally bounded to 20 accounts. Accounts outside the whitelist are ignored by Stage 12A policy until a separate registry change is accepted.

## X/Twitter policy

X exists only to reduce detection latency.

An X post may create or update an **unconfirmed candidate cluster**, but cannot create a usable `NewsEvent` by itself.

Confirmation must come from:

- an official primary publication;
- an appropriate official secondary publication; or
- a major agency source using a separately approved acquisition route and rights policy.

Multiple reposts do not constitute multiple confirmations. A quoted or reposted primary source must be resolved back to the original publisher.

Official-X, squawk and OSINT posts cannot directly trigger `ACTION`.

## Availability

Scheduled event time is not publication time.

Every downstream factual event must have a provable `available_at`. Content is unusable before that timestamp. Embargoed macro releases are never assumed available at their scheduled release time unless the actual source publication is observed.

## Rights

By default do not persist:

- full news article text;
- raw X post text;
- Reuters raw content.

Persist source/provenance metadata, timestamps, content hashes, cluster identity and bounded structured classification outputs. No redistribution right is inferred from technical access.

## Boundary

Stage 12A performs no live fetch, X API access/scraping, scheduler, Flowise Applied State, alert delivery, broker action, order placement, autonomous trading or server apply.
