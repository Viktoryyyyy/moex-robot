#!/usr/bin/env python3
# coding: utf-8
"""
Online MR-1 для Si (5m) без файлов:
- свечи: APIM datashop tradestats -> fallback ISS candles (RFUD -> без доски)
- obstats (если доступен APIM) -> флаг ликвидности; иначе ликвидность OK
- расчёты на лету: ret1, vol20, SMA(w), STD(w), MR-1 (k)
- отправка в Telegram через tg_utils.send_message (plain text)
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import os, sys, argparse, json, math
from datetime import datetime, timedelta
import requests, pandas as pd

# локальные утилиты проекта (у тебя они уже есть)
from dotenv import load_dotenv
from scripts.config_utils import load_config
from scripts.tg_utils import send_message

APIM_BASE = "https://apim.moex.com"
ISS_BASE  = "https://iss.moex.com"

def mk_session() -> requests.Session:
    load_dotenv()
    s = requests.Session()
    tok = os.getenv("MOEX_API_KEY", "")
    if tok:
        s.headers["Authorization"] = "Bearer " + tok
    s.headers["User-Agent"] = "moex_bot_online/1.0"
    s.timeout = 15
    return s

# ---------- нормализация ----------
def norm_tradestats(df: pd.DataFrame) -> pd.DataFrame:
    low = {c.lower(): c for c in df.columns}
    def pick(*names):
        for n in names:
            k = low.get(n.lower())
            if k: return k
        return None
    # поля
    c = pick("ts_pr_close","ts_sec_pr_close","close")
    o = pick("ts_pr_open","open")
    h = pick("ts_pr_high","high")
    l = pick("ts_pr_low","low")
    v = pick("ts_vol","volume","vol")
    d = pick("ts_tradedate","tradedate","date")
    t = pick("ts_tradetime","tradetime","time")
    ts= pick("timestamp","ts_systime")

    if ts and ts in df.columns:
        ts_ser = pd.to_datetime(df[ts], errors="coerce")
    elif d and t and d in df.columns and t in df.columns:
        ts_ser = pd.to_datetime(df[d].astype(str) + " " + df[t].astype(str), errors="coerce")
    else:
        ts_ser = pd.Series(pd.NaT, index=df.index)

    out = pd.DataFrame({
        "timestamp": ts_ser,
        "open":  pd.to_numeric(df[o], errors="coerce") if o in df.columns else pd.NA,
        "high":  pd.to_numeric(df[h], errors="coerce") if h in df.columns else pd.NA,
        "low":   pd.to_numeric(df[l], errors="coerce") if l in df.columns else pd.NA,
        "close": pd.to_numeric(df[c], errors="coerce") if c in df.columns else pd.NA,
        "volume":pd.to_numeric(df[v], errors="coerce") if v in df.columns else pd.NA,
    }).dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return out

def norm_candles_iss(js: dict) -> pd.DataFrame:
    cols = (js.get("candles") or {}).get("columns", [])
    data = (js.get("candles") or {}).get("data", [])
    if not cols or not data:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])
    df = pd.DataFrame(data, columns=cols)
    out = pd.DataFrame({
        "timestamp": pd.to_datetime(df.get("begin"), errors="coerce"),
        "open":  pd.to_numeric(df.get("open"), errors="coerce"),
        "high":  pd.to_numeric(df.get("high"), errors="coerce"),
        "low":   pd.to_numeric(df.get("low"), errors="coerce"),
        "close": pd.to_numeric(df.get("close"), errors="coerce"),
        "volume":pd.to_numeric(df.get("volume"), errors="coerce"),
    }).dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return out

# ---------- источники ----------
def fetch_tradestats_5m(s: requests.Session, symbol: str, limit: int) -> pd.DataFrame | None:
    try:
        url = f"{APIM_BASE}/iss/datashop/algopack/fo/tradestats.json"
        params = {"ticker": symbol, "tickers": symbol, "interval": 5, "limit": int(limit), "iss.meta":"off"}
        r = s.get(url, params=params)
        r.raise_for_status()
        js = r.json()
        tbl = None
        # ищем первую таблицу вида {columns, data}
        for v in js.values():
            if isinstance(v, dict) and "columns" in v and "data" in v:
                tbl = pd.DataFrame(v["data"], columns=v["columns"])
                break
        if tbl is not None and not tbl.empty:
            return norm_tradestats(tbl)
    except Exception:
        return None
    return None

def fetch_candles_iss_5m(s: requests.Session, symbol: str, limit: int) -> pd.DataFrame:
    # 1) с доской RFUD
    try:
        url = f"{ISS_BASE}/iss/engines/futures/markets/forts/boards/RFUD/securities/{symbol}/candles.json"
        params = {"interval": 5, "limit": int(limit), "iss.meta": "off"}
        r = s.get(url, params=params)
        r.raise_for_status()
        df = norm_candles_iss(r.json())
        if not df.empty:
            return df
    except Exception:
        pass
    # 2) без доски
    try:
        url = f"{ISS_BASE}/iss/engines/futures/markets/forts/securities/{symbol}/candles.json"
        params = {"interval": 5, "limit": int(limit), "iss.meta": "off"}
        r = s.get(url, params=params)
        r.raise_for_status()
        return norm_candles_iss(r.json())
    except Exception:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])

def fetch_obstats_liq(s: requests.Session, symbol: str) -> dict:
    """
    Пытаемся достать признак ликвидности из obstats (APIM).
    Если нет доступа/ошибка — возвращаем OK.
    """
    try:
        url = f"{APIM_BASE}/iss/datashop/algopack/fo/obstats.json"
        params = {"ticker": symbol, "tickers": symbol, "interval": 5, "limit": 1, "iss.meta":"off"}
        r = s.get(url, params=params)
        r.raise_for_status()
        js = r.json()
        tbl = None
        for v in js.values():
            if isinstance(v, dict) and "columns" in v and "data" in v:
                tbl = pd.DataFrame(v["data"], columns=v["columns"])
                break
        if tbl is None or tbl.empty:
            return {"ok": True, "reason": "liq:n/a"}
        low = {c.lower(): c for c in tbl.columns}
        # ищем liq_smooth или аналог
        liq_col = low.get("liq_smooth") or low.get("liq") or None
        if liq_col and liq_col in tbl.columns:
            val = pd.to_numeric(tbl[liq_col], errors="coerce").iloc[-1]
            return {"ok": bool(val < 0.5), "reason": f"liq_smooth={float(val):.3f}"}
        return {"ok": True, "reason": "liq:n/a"}
    except Exception:
        return {"ok": True, "reason": "liq:fallback"}

# ---------- расчёты MR-1 ----------
def calc_mr1_signal(df: pd.DataFrame, w: int, k: float) -> dict:
    df = df.sort_values("timestamp").copy()
    ser = pd.to_numeric(df["close"], errors="coerce")
    # признаки, как в HUB
    ret1 = ser.pct_change(1)
    vol20 = ret1.rolling(20, min_periods=5).std()
    sma = ser.rolling(w, min_periods=max(5, w//5)).mean()
    std = ser.rolling(w, min_periods=max(5, w//5)).std()

    last = ser.iloc[-1]
    mu   = sma.iloc[-1]
    sd   = std.iloc[-1]
    sig = "🟦 NO SIGNAL"
    if pd.notna(last) and pd.notna(mu) and pd.notna(sd) and sd > 0:
        if last < mu - k*sd:
            sig = "🟢 BUY"
        elif last > mu + k*sd:
            sig = "🔴 SELL"
    return {
        "signal": sig,
        "close": float(last) if pd.notna(last) else None,
        "mu": float(mu) if pd.notna(mu) else None,
        "sd": float(sd) if pd.notna(sd) else None
    }

def format_msg(symbol: str, ts: datetime, sig: str, close: float, vol: float|None, liq_reason: str, source: str, k: float, w: int) -> str:
    vol_s = f"{int(vol):,}" if (vol is not None and not math.isnan(vol)) else "n/a"
    close_s = f"{int(close):,}" if (close is not None and not math.isnan(close)) else "n/a"
    return (
        "📊 MOEX Bot — MR-1 (Mean Reversion)\n"
        "──────────────────────────────\n"
        f"Инструмент: {symbol}\n"
        f"⏱ Бар: {ts.strftime('%Y-%m-%d %H:%M:%S')} (МСК)\n"
        f"💰 Close: {close_s} ₽\n"
        f"📦 Объём: {vol_s}\n"
        f"💧 Ликвидность: {liq_reason}\n"
        f"Параметры: k={k:.2f}, w={w}, вход t+1\n"
        f"Источник: ONLINE\n"
        "──────────────────────────────\n"
        f"{sig}"
    )

def main():
    ap = argparse.ArgumentParser(description="Online MR-1 без файлов (Si 5m).")
    ap.add_argument("--symbol", default=os.getenv("SI_SYMBOL","SiZ5"))
    ap.add_argument("--limit", type=int, default=180)
    ap.add_argument("--send", action="store_true", help="отправить в Telegram")
    ap.add_argument("--dry", action="store_true", help="не отправлять, только печать")
    args = ap.parse_args()

    cfg = load_config()
    k = float(cfg.get("k", 1.15))
    w = int(cfg.get("mr1_window", 60)) if cfg.get("mr1_window") else 60

    s = mk_session()
    # свечи: APIM -> ISS
    df = fetch_tradestats_5m(s, args.symbol, args.limit)
    if df is None or df.empty:
        df = fetch_candles_iss_5m(s, args.symbol, args.limit)
    if df.empty:
        print("⚠️ Нет онлайн-баров. Проверь доступ к APIM/ISS.")
        return

    # ликвидность: obstats (если доступно)
    liq = fetch_obstats_liq(s, args.symbol)
    liq_reason = liq.get("reason", "liq:n/a")
    liq_ok = bool(liq.get("ok", True))

    # MR-1 на лету
    calc = calc_mr1_signal(df, w=w, k=k)
    row = df.iloc[-1]
    ts = pd.to_datetime(row["timestamp"]).tz_localize(None)  # выводим без tz
    vol = float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None
    msg = format_msg(args.symbol, ts, calc["signal"], calc["close"], vol, ("OK" if liq_ok else "LOW") + f" ({liq_reason})", "ONLINE", k, w)

    print(msg)

    if args.send and not args.dry:
        try:
            resp = send_message(msg)
            ok = resp.get("ok", False)
            print(f"Telegram отправка: OK={ok}, message_id={resp.get('result',{}).get('message_id')}")
        except Exception as e:
            print(f"✖️ Ошибка отправки: {e}")

if __name__ == "__main__":
    main()
