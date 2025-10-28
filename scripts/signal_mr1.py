#!/usr/bin/env python3
# coding: utf-8
import os, re, glob, sys
import pandas as pd
from datetime import datetime

TIME_CANDIDATES = ["timestamp","datetime","end","DATETIME","ts_SYSTIME","ts_tradedate","ts_tradetime","TRADEDATE","TIME","TRADETIME","date","time"]
CLOSE_CANDIDATES = ["ts_pr_close","ts_sec_pr_close","CLOSE","close","LAST","PRICECLOSE","LAST_PRICE","PRICE"]
SIG_CANDIDATES = ["mr1_signal", "signal_mr1", "signal"]
AUX_FIELDS = ["ts_vol","VOL","volume","ts_oi_close","OPENPOSITION","openinterest","liq_flag_low"]

def extract_date_from_name(path: str):
    m = re.search(r"si_5m_(\\d{4}-\\d{2}-\\d{2})\\.csv$", os.path.basename(path), re.IGNORECASE)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except ValueError:
        return None

def pick_latest_si_csv():
    files = glob.glob("si_5m_*.csv")
    if not files:
        return None
    dated = [(p, extract_date_from_name(p)) for p in files]
    dated = [t for t in dated if t[1] is not None]
    if dated:
        return sorted(dated, key=lambda x: x[1])[-1][0]
    return max(files, key=lambda p: os.path.getmtime(p))

def first_existing(df, candidates):
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower() in low:
            return low[c.lower()]
    return None

def combine_date_time(df):
    """
    Если есть TRADEDATE и TIME/TRADETIME — создаём столбец 'timestamp' ISO.
    Возвращаем имя столбца времени, который стоит использовать.
    """
    import pandas as pd
    date_cols = [c for c in df.columns if c.upper() in ("TRADEDATE","TRADE_DATE","DATE")]
    time_cols = [c for c in df.columns if c.upper() in ("TIME","TRADETIME","TRADE_TIME")]
    if date_cols and time_cols:
        dcol = date_cols[0]
        tcol = time_cols[0]
        try:
            ts = pd.to_datetime(df[dcol].astype(str) + " " + df[tcol].astype(str), errors="coerce")
            if ts.notna().any():
                df["timestamp"] = ts
                return "timestamp"
        except Exception:
            pass
    return None

def normalize_time(val):
    if pd.isna(val):
        return "N/A"
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    s = str(val)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return s

def map_signal(sig_val):
    if pd.isna(sig_val):
        return "🟦 NO SIGNAL"
    s = str(sig_val).strip().upper()
    try:
        f = float(s)
        if f > 0:  return "🟢 BUY"
        if f < 0:  return "🔴 SELL"
        return "🟦 NO SIGNAL"
    except Exception:
        pass
    if s in ("BUY","LONG","+1","1"): return "🟢 BUY"
    if s in ("SELL","SHORT","-1"):  return "🔴 SELL"
    if s in ("0","NONE","NO","FLAT","HOLD"): return "🟦 NO SIGNAL"
    return f"🟦 {s}"

import json, argparse
from pathlib import Path

STATE_PATH = Path(".state/mr1_last.json")

def load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(d):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser(description="Сформировать и (опц.) отправить сообщение MR-1 по последней свече Si 5m.")
    ap.add_argument("--send", action="store_true", help="Отправить сообщение в Telegram (по умолчанию только печать).")
    ap.add_argument("--force", action="store_true", help="Игнорировать антидубликат и отправить в любом случае.")
    args = ap.parse_args()

    path = pick_latest_si_csv()
    if not path:
        print("⚠️ Не найдено файлов si_5m_*.csv в текущей директории.")
        sys.exit(0)

    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ Файл пуст: {path}")
        sys.exit(0)

    time_cols = [c for c in ["timestamp","datetime","DATETIME","end"] if c in df.columns]
    df = df.sort_values(by=time_cols or df.columns.tolist(), ascending=True)
    row = df.tail(1).iloc[0]
    t_col = first_existing(df, TIME_CANDIDATES)
    c_col = first_existing(df, CLOSE_CANDIDATES)
    s_col = first_existing(df, SIG_CANDIDATES)

    ts = normalize_time(row[t_col]) if t_col else "N/A"
    close_val = row[c_col] if c_col else None
    close_str = "N/A" if pd.isna(close_val) else f"{float(close_val):.2f}"
    sig_str = map_signal(row[s_col]) if s_col else "🟦 NO SIGNAL"

    extras = []
    for f in AUX_FIELDS:
        if f in df.columns and pd.notna(row.get(f)):
            try:
                extras.append(f"{f}={float(row[f]):.3f}")
            except Exception:
                extras.append(f"{f}={row[f]}")

    extras_str = (" | " + " ".join(extras)) if extras else ""
    msg = (
        f"MOEX Bot — MR-1\\n"
        f"{sig_str}\\n"
        f"Инструмент: Si (5m)\\n"
        f"Время бара: {ts}\\n"
        f"Close: {close_str}{extras_str}"
    )

    print("-"*60)
    print(msg)
    print("Источник:", path)
    print("-"*60)

    # --- антидубликат ---
    # Ключ = (ts, sig_str, close_str). Если совпадает с прошлым — не шлём (если нет --force).
    state = load_state()
    key_curr = {"ts": ts, "sig": sig_str, "close": close_str}
    key_prev = state.get("last_sent")

    should_send = True
    if not args.force and key_prev == key_curr:
        should_send = False
        print("🔁 Антидубликат: содержимое не изменилось — отправка пропущена. (Добавь --force для принудительной отправки)")

    if args.send and should_send:
        # HTML-формат: слегка подчистим строку
        html_msg = (
            f"<b>MOEX Bot — MR-1</b>\n"
            f"{sig_str}\n"
            f"Инструмент: <b>Si (5m)</b>\n"
            f"Время бара: <code>{ts}</code>\n"
            f"Close: <b>{close_str}</b>" + (extras_str and f" | <code>{extras_str[3:]}</code>") # без " | "
        )
        from tg_utils import send_message
        resp = send_message(html_msg)
        ok = resp.get("ok")
        mid = (resp.get("result") or {}).get("message_id")
        print(f"Telegram отправка: OK={ok}, message_id={mid}")
        if ok:
            state["last_sent"] = key_curr
            save_state(state)
    elif args.send and not should_send:
        print("ℹ️ Отправка отключена из-за антидубликата.")
    else:
        print("ℹ️ Режим dry-run: отправка выключена.")

if __name__ == "__main__":
    main()

