# MOEX Bot Runtime Environment

## Canonical server env file

The canonical runtime environment file on the MOEX Bot server is:

`/home/trader/moex_bot/.env`

This file is Applied State and contains runtime secrets such as `MOEX_API_KEY`.

Rules:

- GitHub stores this path reference only; secret values must never be committed.
- Runtime commands that require environment loading must use this exact file when an explicit env file is needed.
- `/home/trader/moex_bot/moex-robot/.env` is not the canonical runtime secret file.
- GitHub/repository remains Source of Truth for architecture; the server remains Applied State only.
