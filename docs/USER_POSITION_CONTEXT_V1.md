# USDRUBF explicit user position context

PROJECT=MOEX_Bot

This state is factual user-supplied context only. It does not grant directional, action, standalone BUY/SELL/OUT, Stage5, broker, or risk-management authority.

## Persisted state

The canonical state file is stored alongside the existing chat-analysis snapshot state under `MOEX_DATA_ROOT` as:

`state/rub_intelligence/chat_analysis_snapshot/user_position_context.json`

The persisted object contains exactly:

- `instrument=USDRUBF`;
- `direction=LONG | SHORT | FLAT`;
- `average_entry_price`;
- `user_input_updated_at`;
- `source_semantics=explicit_user_input`.

For `LONG` and `SHORT`, `average_entry_price` must be a finite positive numeric value. For `FLAT`, `average_entry_price` must be `null`.

Missing or invalid persisted state is not interpreted as `FLAT`. The canonical snapshot exposes it as unavailable and does not infer direction or average price from market data, chat history, broker state, or any other source.

## Canonical snapshot attachment

The current `rub_chat_analysis_snapshot.v1` publisher attaches a top-level `user_position_context` block containing exactly:

- `instrument`;
- `direction`;
- `average_entry_price`;
- `user_input_updated_at`;
- `status`;
- `availability`;
- `explicit_user_input`.

Valid state uses `status=AVAILABLE`, `availability=EXPLICIT_USER_INPUT_AVAILABLE`, and `explicit_user_input=true`.

No state uses `status=UNAVAILABLE`, `availability=NO_EXPLICIT_USER_INPUT`, and `explicit_user_input=false`.

Invalid state fails closed with `status=UNAVAILABLE`, `availability=INVALID_EXPLICIT_USER_INPUT`, and `explicit_user_input=false`.

The attachment is a local file read only. It performs no network call and does not affect canonical component readiness.

## Manual update

Use the existing project Python environment and run:

`python -m src.moex_research.runners.usdrubf_user_position_context --direction LONG --average-entry-price 86.15`

`python -m src.moex_research.runners.usdrubf_user_position_context --direction SHORT --average-entry-price 86.15`

`python -m src.moex_research.runners.usdrubf_user_position_context --direction FLAT`

The update validates fail-closed and publishes the state atomically with the existing temp-file + `os.replace` mechanism.
