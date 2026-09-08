# Verified, host-scoped Rosstat HTTPS

Rosstat failed ordinary server TLS validation because the site omitted the
intermediate needed by its leaf certificate. The leaf's AIA identifies the
2024 issuer; the common legacy download contains the 2022 issuer with a different
key. The chain was verified with OpenSSL, including `rosstat.gov.ru` hostname.

Pinned public certificate provenance:

- Root: https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt,
  obtained over normally verified HTTPS. The download route is documented by
  https://www.rustore.ru/help/developers/monetization/payment-callback/Preparing-the-server-for-RuStore%20API .
- Issuer 2024: http://nuc-cdp.digital.gov.ru/cdp/subca_ssl_rsa2024.crt,
  obtained from leaf AIA and cryptographically verified against that root.
  The HTTP transport is not its trust basis; the verified signature is.
- Root DER SHA-256: `d26d2d0231b7c39f92cc738512ba54103519e4405d68b5bd703e9788ca8ecf31`.
- Exact PEM file SHA-256 values are pinned in `rosstat_https.CERTIFICATES`.
  The root download's CRLF line endings are normalized to LF for Git; its DER
  fingerprint and certificate content are unchanged.

`rosstat_https` builds a private TLS context with certificate and hostname
verification enabled. It accepts only explicit document paths on the exact
Rosstat HTTPS hostname. Redirects, query strings, unexpected content types,
oversized documents and modified certificate files fail closed. No global SSL
defaults, OS certificate stores or unrelated clients are changed. Certificate
expiry is still enforced; renewal requires a reviewed certificate update.

Example: `PYTHONPATH=.:src python -m moex_research.external_data.rosstat_https
--url https://rosstat.gov.ru/storage/mediabank/134_02-09-2026.html --output PATH`.
This freezes the body and a receipt manifest. Transport success does not establish
CPI identity, numerical validity, latest-release selection, historical PIT or
macro completeness. Semantic parsing and canonical API admission remain separate.
The existing production macro gates stay unchanged.
