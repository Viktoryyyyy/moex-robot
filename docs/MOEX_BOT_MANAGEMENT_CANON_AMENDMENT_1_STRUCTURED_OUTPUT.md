# MOEX Bot Management Canon Amendment 1 — Structured Output Project Marker

status: active
version: 1.0
approved_by: PM_L1_OWNER_DELEGATED
adopted_at: 2026-07-21
amends: `docs/MOEX_BOT_MANAGEMENT_CANON.md` section 2 only

## Purpose

The management canon requires every management output to start with `PROJECT=MOEX_Bot`.

Machine-readable Agent outputs must remain valid JSON and therefore cannot use a non-JSON text prefix.

## Rule

For human-readable Browser/chat output, the first line remains:

```text
PROJECT=MOEX_Bot
```

For machine-readable JSON output, the required equivalent project marker is the first JSON property:

```json
{
  "project": "MOEX_Bot"
}
```

No prose may precede the JSON object.

A JSON result without `"project": "MOEX_Bot"` does not satisfy project-isolation output requirements after this amendment is applied to the relevant Agent setting.

## Scope

This exception applies only to outputs that must be parsed as JSON, including:
- Flowise Lead external structured results;
- Flowise Worker structured results returned to Lead;
- future machine-readable management integrations.

It does not remove the textual first-line marker requirement from Browser or human-readable chat responses.

## Precedence

This amendment is an owner-approved clarification and supersedes only the output-format requirement in section 2 of `docs/MOEX_BOT_MANAGEMENT_CANON.md` for valid machine-readable JSON.

All other management canon rules remain unchanged.