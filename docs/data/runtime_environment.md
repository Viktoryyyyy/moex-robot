# MOEX Bot Runtime Environment

## Canonical server env file

The canonical runtime environment file on the MOEX Bot server is:

`/home/trader/moex_bot/.env`

This file is Applied State and contains runtime secrets such as `MOEX_API_KEY` plus required non-secret runtime settings.

Rules:

- GitHub stores path references and non-secret runtime-setting values only; secret values must never be committed.
- `MOEX_API_KEY` rotations or replacements must update `/home/trader/moex_bot/.env`; do not create or restore a repository-local secret file for key rotation.
- `REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt` is required in `/home/trader/moex_bot/.env` for MOEX HTTPS runtime after the 2026 certificate-chain change. Do not disable TLS verification and do not use `verify=False`.
- Runtime commands that require an explicit env file must use `/home/trader/moex_bot/.env`.
- `/home/trader/moex_bot/moex-robot/.env` is not a canonical runtime secret file and must be absent after the server migration to the parent env path.
- Parameterless python-dotenv discovery is valid only when the repository-local duplicate is absent, allowing discovery to continue to `/home/trader/moex_bot/.env`.
- Removal of the repository-local duplicate is an Applied State migration performed only after verifying that the canonical parent env exists; verification must not print secret values.
- GitHub/repository remains Source of Truth for architecture; the server remains Applied State only.
