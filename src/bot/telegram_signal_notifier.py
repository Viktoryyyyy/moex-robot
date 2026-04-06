import json
import os
import csv
from datetime import date
from pathlib import Path
from urllib import request, parse

CHECKPOINT_PATH = Path("data/state/telegram_signal_notifier_checkpoint.json")
SIGNALS_DIR = Path("data/signals")
STRATEGY = "ema_3_19_15m_block_adverse"
INSTRUMENT = "Si"


def _env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"missing required env: {name}")
    return val


def _load_checkpoint():
    if not CHECKPOINT_PATH.exists():
        return None
    try:
        return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError("broken checkpoint json: " + str(e))


def _save_checkpoint(identity):
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(identity, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CHECKPOINT_PATH)


def _identity(row):
    return {
        "trade_date": str(row["trade_date"]),
        "seq": str(row["seq"]),
        "bar_end": str(row["bar_end"]),
        "action": str(row["action"]),
    }


def _send(token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    req = request.Request(url, data=data)
    with request.urlopen(req) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not payload.get("ok"):
        raise RuntimeError("telegram send failed")


def _format(row):
    pos = row["new_pos"]
    direction = "LONG" if pos == "1" else "SHORT" if pos == "-1" else "FLAT"

    parts = [
        f"strategy: {STRATEGY}",
        f"instrument: {INSTRUMENT}",
        f"bar: {row['bar_end']}",
        f"action: {row['action']}",
        f"position: {direction}",
        f"price: {row['price']}",
        f"seq: {row['seq']}",
        f"day_pnl: {row['cum_pnl']}",
    ]

    if row.get("prev_pos") not in ("0", 0):
        parts.append(f"last_trade_pnl: {row['realized_pnl']}")

    return " | ".join(parts)


def run():
    token = _env("TELEGRAM_BOT_TOKEN")
    chat = _env("TELEGRAM_CHAT_ID")

    today = date.today().isoformat()
    path = SIGNALS_DIR / f"ema_3_19_15m_realtime_{today}.csv"

    if not path.exists():
        return 0

    rows = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return 0

    cp = _load_checkpoint()

    if cp is None:
        _save_checkpoint(_identity(rows[-1]))
        return 0

    found = False
    new_rows = []

    for r in rows:
        if not found:
            if _identity(r) == cp:
                found = True
            continue
        new_rows.append(r)

    if not found and cp.get("trade_date") == today:
        raise RuntimeError("checkpoint identity not found in current trade log")

    for r in new_rows:
        msg = _format(r)
        _send(token, chat, msg)
        _save_checkpoint(_identity(r))

    return 0
