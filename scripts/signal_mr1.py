#!/usr/bin/env python3
# coding: utf-8
import os, re, glob, sys
import pandas as pd
from datetime import datetime

TIME_CANDIDATES = ["timestamp","datetime","end","DATETIME","ts_SYSTIME","ts_tradedate","ts_tradetime","TRADEDATE","TIME","TRADETIME","date","time"]
CLOSE_CANDIDATES = ["ts_pr_close","ts_sec_pr_close","CLOSE","close","LAST","PRICECLOSE","LAST_PRICE","PRICE"]
SIG_CANDIDATES = ["mr1_signal","signal_mr1","signal","side","mr1","mr_1","trade_signal","SIG","Sig","SIGNAL"]
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

from config_utils import load_config


LOG_FILE = Path(".state/signal_log.csv")

def log_signal(ts: str, sig: str, close: float, path: str, status: str):
    """Записывает строку лога в .state/signal_log.csv"""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    import csv
    with LOG_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if f.tell() == 0:
            w.writerow(["timestamp","signal","close","file","status"])
        w.writerow([ts, sig, close, path, status])



def resolve_latest_daily_csv() -> str:
    import os
    files = list(Path('.').glob('si_5m_20*.csv'))
    valid = []
    for f in files:
        name = f.name
        # только одиночная дата, без подстроки _features и без двойных диапазонов
        if '_features' in name or re.search(r'\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}', name):
            continue
        if re.search(r'\d{4}-\d{2}-\d{2}', name):
            valid.append(f)
    if not valid:
        return None
    # сортируем по имени (в нём дата) и берём последний
    valid.sort(key=lambda x: x.name)
    latest = valid[-1]
    print(f"[resolve] Используем {latest.name}")
    return str(latest)


def main():
    ap = argparse.ArgumentParser(description="Сформировать и (опц.) отправить сообщение MR-1 по последней свече Si 5m.")
    ap.add_argument("--send", action="store_true", help="Отправить сообщение в Telegram (по умолчанию только печать).")
    ap.add_argument("--ignore-liq", action="store_true", help="Игнорировать фильтр ликвидности при отправке.")
    ap.add_argument("--force", action="store_true", help="Игнорировать антидубликат и отправить в любом случае.")
    args = ap.parse_args()

    # конфиг по умолчанию (HUB)
    cfg = load_config()


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
    if s_col:
        print(f"🔎 Использован столбец сигнала: {s_col}")

    extras = []
    for f in AUX_FIELDS:
        if f in df.columns and pd.notna(row.get(f)):
            try:
                extras.append(f"{f}={float(row[f]):.3f}")
            except Exception:
                extras.append(f"{f}={row[f]}")

    extras_str = (" | " + " ".join(extras)) if extras else ""
    
msg = (
    f"📊 MOEX Bot — MR-1 (Mean Reversion)\n"
    f"──────────────────────────────\n"
    f"Инструмент: {symbol}\n"
    f"⏱ Бар: {ts} (МСК)\n"
    f"💰 Close: {close_val:,.0f} ₽\n"
    f"📦 Объём: {ts_vol:,.0f}  OI: {ts_oi_close:,.0f}\n"
    f"💧 Ликвидность: {'OK' if liq_flag_low==0 else 'LOW'} ({liq_flag_low:.3f})\n"
    f"📁 Источник: {path}\n"
    f"──────────────────────────────\n"
    f"{sig_str}"
)

    print("-"*60)
    print(msg)
    print("Источник:", path)
    # логируем сигнал (dry)
    try:
        log_signal(str(ts), sig_str, float(close_val) if "close_val" in locals() else None, path, "dry")
    except Exception as e:
        print(f"⚠️ Ошибка записи лога: {e}")
    # --- фильтр ликвидности ---
    liq_ok = True
    liq_reason = "OK"
    # приоритет: liq_smooth (старый конвейер)
    if "liq_smooth" in df.columns:
        liq_thr = float(cfg.get("liq_threshold", 0.5))
        try:
            liq_ok = float(row.get("liq_smooth")) < liq_thr
            liq_reason = f"liq_smooth={float(row.get('liq_smooth')):.3f} < {liq_thr:.2f}"
        except Exception:
            pass
    # fallback: liq_flag_low (0 — ликвидно, 1 — низкая ликвидность)
    elif "liq_flag_low" in df.columns:
        try:
            liq_ok = float(row.get("liq_flag_low")) == 0.0
            liq_reason = f"liq_flag_low={float(row.get('liq_flag_low')):.3f}"
        except Exception:
            pass

    if args.send and not args.ignore_liq and not liq_ok:
        print(f"🚫 Ликвидность не проходит фильтр: {liq_reason}. Отправка отключена.")
        return

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
        text_
msg = (
    f"📊 MOEX Bot — MR-1 (Mean Reversion)\n"
    f"──────────────────────────────\n"
    f"Инструмент: {symbol}\n"
    f"⏱ Бар: {ts} (МСК)\n"
    f"💰 Close: {close_val:,.0f} ₽\n"
    f"📦 Объём: {ts_vol:,.0f}  OI: {ts_oi_close:,.0f}\n"
    f"💧 Ликвидность: {'OK' if liq_flag_low==0 else 'LOW'} ({liq_flag_low:.3f})\n"
    f"📁 Источник: {path}\n"
    f"──────────────────────────────\n"
    f"{sig_str}"
)
            + (f"\nЛиквидность: <code>{liq_reason}</code>")
        )
        from tg_utils import send_message
        try:
            resp = send_message(msg)
            ok = resp.get("ok")
        except Exception as e:
            ok = False
            print(f"✖️ Ошибка отправки: {e}")
        mid = (resp.get("result") or {}).get("message_id")
        print(f"Telegram отправка: OK={ok}, message_id={mid}")
        if ok:
            state["last_sent"] = key_curr
            save_state(state)
        # логируем исход независимо от успеха
        try:
            log_signal(str(ts), sig_str, float(close_val) if "close_val" in locals() else None, path, "sent" if ok else "error")
        except Exception as e:
            print(f"⚠️ Ошибка записи лога: {e}")
    elif args.send and not should_send:
        print("ℹ️ Отправка отключена из-за антидубликата.")
    else:
        print("ℹ️ Режим dry-run: отправка выключена.")

if __name__ == "__main__":
    main()

