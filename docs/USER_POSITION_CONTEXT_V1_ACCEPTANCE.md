# user_position_context_v1 acceptance scope

PROJECT=MOEX_Bot
TASK_ID=user_position_context_v1
LANE=rub_intelligence

Accepted scope is limited to explicit USDRUBF user-supplied position direction and average entry price.

Required behavior:

- `LONG` and `SHORT` require a finite positive `average_entry_price`;
- `FLAT` requires `average_entry_price=null`;
- missing state is unavailable and is not `FLAT`;
- only explicit user input can establish position state;
- persistence is local and atomic;
- canonical snapshot attachment adds no risk, sizing, recommendation, BUY/SELL/OUT, broker, or automation fields;
- canonical authority and Stage5 flags are unchanged;
- no network call is added by the user-position state path.

Implementation scope:

- existing canonical `chat_analysis_snapshot` state directory;
- one persisted `user_position_context.json` state file;
- one small manual-state runner;
- attachment into the existing canonical layered snapshot publisher;
- unit tests and documentation only.
