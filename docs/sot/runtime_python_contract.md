# Runtime Python Contract

Canonical runtime environment for MOEX Bot.

## Server Runtime

Project repo root:

/home/trader/moex_bot/moex-robot

Virtual environment:

/home/trader/moex_bot/venv

Canonical interpreter:

/home/trader/moex_bot/venv/bin/python

Python version (validated):

Python 3.12.x

## Execution Convention

All CLI modules must be launched using the project interpreter:

/home/trader/moex_bot/venv/bin/python

Example canonical launch pattern:

python -c "from dotenv import load_dotenv; load_dotenv(); import runpy; runpy.run_module('src.cli.run_telegram_signal_notifier', run_name='__main__')"

## Policy

GitHub stores the runtime contract only. The virtual environment itself is server-local and must not be committed to the repository.
