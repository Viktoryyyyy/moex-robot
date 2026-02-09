#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_api_base/1.2").strip()
TK  = os.getenv("MOEX_API_KEY", "").strip()

TICK_FUT = os.getenv("MOEX_FUT_TICKER", "SiZ5")
BASE_OI  = os.getenv("MOEX_OI_BASE", "si")
TZ_MSK   = ZoneInfo("Europe/Moscow")

H = {"Authorization": "Bearer " + TK, "User-Agent": UA}

def iss_get(path, params=None):
    url = f"{API}/{path.lstrip('/')}"
    r = requests.get(url, headers=H, params=params or {}, timeout=25)
    r.raise_for_status()
    return r.json()

def to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns", [])
    data = block.get("data", [])
    # Валидируем строки
    rows = []
    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                if cols:
                    rows.append([row.get(c) for c in cols])
                else:
                    rows.append(list(row.values()))
            elif isinstance(row, (list, tuple)):
                rows.append(list(row))
            else:
                continue
    if cols and any(len(r) != len(cols) for r in rows):
        rows = [r for r in rows if len(r) == len(cols)]
    try:
        df = pd.DataFrame(rows, columns=cols)
    except Exception:
        df = pd.DataFrame(columns=cols)
    # Имена строго строками
    df.columns = [str(c) for c in df.columns]
    return df

# ---------- tradestats ----------
def load_tradestats(day: str) -> pd.DataFrame:
    j = iss_get(f"/iss/datashop/algopack/fo/tradestats/{TICK_FUT}.json",
                {"from":day, "till":day})
    b = j.get("data") or {}
    if not b or "columns" not in b:
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    df = to_df(b)
    need = {"tradedate","tradetime","pr_open","pr_high","pr_low","pr_close","vol"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=["end","open","high","low","close","volume"])
    df["end"] = (df["tradedate"].astype(str) + " " + df["tradetime"].astype(str) + "+03:00")
    out = pd.DataFrame({
        "end": df["end"],
        "open": pd.to_numeric(df["pr_open"], errors="coerce"),
        "high": pd.to_numeric(df["pr_high"], errors="coerce"),
        "low":  pd.to_numeric(df["pr_low"],  errors="coerce"),
        "close":pd.to_numeric(df["pr_close"],errors="coerce"),
        "volume":pd.to_numeric(df["vol"], errors="coerce")
    })
    return out

# ---------- futoi ----------
def load_futoi(day: str) -> pd.DataFrame:
    j = iss_get(f"/iss/analyticalproducts/futoi/securities/{BASE_OI}.json",
                {"from":day, "till":day})
    b = j.get("futoi") or {}
    if not b or "columns" not in b:
        return pd.DataFrame(columns=["end","oi_fiz","oi_yur","oi_total"])
    df = to_df(b)
    need = {"tradedate","tradetime","clgroup","pos"}
    if not need.issubset(df.columns):
        return pd.DataFrame(columns=["end","oi_fiz","oi_yur","oi_total"])
    df["end"] = (df["tradedate"].astype(str) + " " + df["tradetime"].astype(str) + "+03:00")
    piv = df.pivot_table(index="end", columns="clgroup", values="pos", aggfunc="last")
    for c in ("FIZ","YUR"):
        if c not in piv.columns:
            piv[c] = pd.NA
    piv = piv.rename(columns={"FIZ":"oi_fiz","YUR":"oi_yur"}).reset_index()
    piv["oi_total"] = piv[["oi_fiz","oi_yur"]].abs().sum(axis=1, skipna=False)
    return piv[["end","oi_fiz","oi_yur","oi_total"]]

# ---------- obstats ----------
def _uniqueify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Сделать имена колонок уникальными: depth -> depth, depth#2, depth#3 ..."""
    new_cols = []
    seen = {}
    for c in df.columns:
        if c not in seen:
            new_cols.append(c)
            seen[c] = 1
        else:
            seen[c] += 1
            new_cols.append(f"{c}#{seen[c]}")
    df = df.copy()
    df.columns = new_cols
    return df

def _safe_to_numeric(series) -> pd.Series | None:
    """Вернёт числовую Series или None, если привести нельзя/нечего."""
    # если случайно нам подсунули DataFrame (из-за дубликатов) — отбрасываем
    if hasattr(series, "ndim") and getattr(series, "ndim", 1) != 1:
        return None
    try:
        s = pd.to_numeric(series, errors="coerce")
        return s if s.notna().any() else None
    except Exception:
        return None

def load_obstats(day: str) -> pd.DataFrame:
    try:
        j = iss_get(f"/iss/datashop/algopack/fo/obstats/{TICK_FUT}.json",
                    {"from":day, "till":day})
        b = j.get("data") or {}
        if not b or "columns" not in b:
            return pd.DataFrame(columns=["end"])
        df = to_df(b)
        # ключ времени
        if {"tradedate","tradetime"}.issubset(df.columns):
            df["end"] = (df["tradedate"].astype(str) + " " + df["tradetime"].astype(str) + "+03:00")
        elif "end" in df.columns:
            pass
        else:
            return pd.DataFrame(columns=["end"])

        # делаем имена уникальными ещё ДО выборки
        df = _uniqueify_columns(df)

        # выбираем полезные кандидаты по префиксам/именам (учтём возможные #2)
        want = ("spread", "spr", "best_bid", "best_ask", "bb", "ba",
                "bb_qty", "ba_qty", "imbalance", "book_imbalance",
                "liq", "liq_fast", "liq_smooth", "liquidity",
                "depth_buy", "depth_sell", "depth_total")
        keep = ["end"] + [c for c in df.columns if c == "end" or any(c.startswith(w) for w in want)]
        out = df[keep].copy()

        # конвертация только в Series и только если реально числовая
        numeric = {}
        for c in out.columns:
            if c == "end":
                continue
            s = _safe_to_numeric(out[c])
            if s is not None:
                numeric[c] = s

        if not numeric:
            # нечего добавлять
            return out[["end"]]

        num_df = pd.DataFrame(numeric)
        num_df.insert(0, "end", out["end"].values)
        # ещё раз уберём дубликаты имён, если вдруг появились из concat
        num_df = num_df.loc[:, ~num_df.columns.duplicated()]
        return num_df
    except Exception as e:
        print(f"WARN: obstats parse failed for {day}: {e}", file=sys.stderr)
        return pd.DataFrame(columns=["end"])

def run_for_day(day_str: str):
    ts = load_tradestats(day_str)
    oi = load_futoi(day_str)
    ob = load_obstats(day_str)
    df = ts.merge(oi, on="end", how="left")
    if not ob.empty:
        df = df.merge(ob, on="end", how="left")
    df = df.sort_values("end").reset_index(drop=True)
    print(f"\n===== {day_str} (MSK) — {TICK_FUT} =====")
    print(f"rows={len(df)} cols={list(df.columns)}")
    if df.empty:
        print("WARN: no data")
        return
    print("# head(3)")
    print(df.head(3).to_csv(index=False))
    print("# tail(3)")
    print(df.tail(3).to_csv(index=False))

def main():
    if not TK or len(TK) < 10:
        print("ERROR: MOEX_API_KEY missing/too short", file=sys.stderr); sys.exit(1)
    today_msk = datetime.now(TZ_MSK).date()
    days = [(today_msk - timedelta(days=i)).isoformat() for i in range(0,3)][::-1]  # D-2,D-1,D
    print(f"TICK_FUT={TICK_FUT} BASE_OI={BASE_OI} DAYS={days}")
    for d in days:
        try:
            run_for_day(d)
        except requests.HTTPError as e:
            print(f"\n===== {d} (MSK) — {TICK_FUT} =====")
            print(f"HTTP ERROR: {e.response.status_code} {e.response.text[:200].replace(chr(10),' ')}")
        except Exception as e:
            print(f"\n===== {d} (MSK) — {TICK_FUT} =====")
            print(f"ERROR: {e}")

if __name__ == "__main__":
    main()
