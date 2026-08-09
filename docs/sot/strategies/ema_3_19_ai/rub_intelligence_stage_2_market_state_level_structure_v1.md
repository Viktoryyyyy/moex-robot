# RUB Intelligence Stage 2 — Market State and Level/Structure Contract

PROJECT=MOEX_Bot

Task ID: `rub_intelligence_stage_2_market_state_level_contract_v1`

## Purpose

Freeze the v1 deterministic market-state and level/structure semantics for USDRUBF before runtime implementation.

## Architecture decision

Technical market facts are produced by deterministic Python code. LLM/Flowise agents may interpret structured facts and combine them with structured news, macro, positioning, and model context, but they must not originate numeric technical levels or classify test/retest/breakout events.

The frozen technical lifecycle includes zone-based levels and the states:

`APPROACH → TEST → REJECTION/BREAKOUT_ATTEMPT → BREAKOUT → RETEST_PENDING → RETEST → RETEST_HOLD/RETEST_FAIL → ACCEPTANCE/FALSE_BREAKOUT`.

A single tick is insufficient to establish a test. Breakout and acceptance require closed-bar confirmation. Look-ahead is forbidden.

## Stage boundary

This stage is contract-only. It does not authorize runtime implementation, server apply, news ingestion, LLM news classification, alert delivery, broker action, or trading action.

## Next stage

Implement the deterministic Level & Structure Engine against this frozen contract and validate it on bounded historical scenarios covering successful retest and failed retest / false breakout.
