#!/usr/bin/env python3
import os, glob, argparse, pandas as pd, numpy as np
from datetime import datetime

REQ_ANY = [
    ["end","OPEN","HIGH","LOW","CLOSE","volume"],            # уже нормализованные файлы
    ["TRADEDATE","TIME","OPEN","HIGH","LOW","CLOSE","VOL"],  # сырые до нормализации
]

def read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as e:
        return pd.DataFrame()

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    # Вариант 1: уже нормализован
    if {"end","OPEN","HIGH","LOW","CLOSE"}.issubset(cols):
        out = df.copy()
        out["end"] = pd.to_datetime(out["end"], errors="coerce")
        if "volume" not in out.columns and "VOL" in out.columns:
            out["volume"] = pd.to_numeric(out["VOL"], errors="coerce")
        return out[["end","OPEN","HIGH","LOW","CLOSE","volume"]]
    # Вариант 2: TRADEDATE+TIME
    if {"TRADEDATE","TIME","OPEN","HIGH","LOW","CLOSE"}.issubset(cols):
        out = df.copy()
        out["end"] = pd.to_datetime(out["TRADEDATE"].astype(str) + " " + out["TIME"].astype(str),
                                    errors="coerce")
        volc = "volume" if "volume" in cols else ("VOL" if "VOL" in cols else None)
        if volc is None:
            out["volume"] = np.nan
        else:
            out["volume"] = pd.to_numeric(out[volc], errors="coerce")
        return out[["end","OPEN","HIGH","LOW","CLOSE","volume"]]
    # Иначе — пусто
    return pd.DataFrame(columns=["end","OPEN","HIGH","LOW","CLOSE","volume"])

def offgrid_mask(ts: pd.Series, freq_sec=300):
    # true если метка времени не кратна 5 минутам (UTC-наивно)
    s = pd.to_datetime(ts, errors="coerce")
    secs = (s.view("int64") // 10**9) % freq_sec
    return (secs != 0) | s.isna()

def day_str(d: pd.Timestamp) -> str:
    try:
        return d.date().isoformat()
    except Exception:
        return "N/A"

def per_file_report(path: str) -> dict:
    raw = read_csv(path)
    norm = normalize(raw)
    rep = {
        "file": os.path.basename(path),
        "rows": int(len(raw)),
        "rows_norm": int(len(norm)),
        "min_end": "N/A", "max_end": "N/A",
        "dups_end": 0,
        "offgrid_5m": 0,
        "nan_OPEN": 0, "nan_HIGH": 0, "nan_LOW": 0, "nan_CLOSE": 0, "nan_vol": 0,
        "neg_vol": 0,
        "ohlc_inconsistent": 0,
        "spillover_out_of_day": 0,
        "status": "OK"
    }
    if norm.empty:
        rep["status"] = "EMPTY_OR_BAD_SCHEMA"
        return rep

    # базовые преобразования
    norm = norm.copy()
    norm["end"]   = pd.to_datetime(norm["end"], errors="coerce")
    for c in ["OPEN","HIGH","LOW","CLOSE","volume"]:
        norm[c] = pd.to_numeric(norm[c], errors="coerce")

    # min/max
    rep["min_end"] = day_str(pd.to_datetime(norm["end"].min()))
    rep["max_end"] = day_str(pd.to_datetime(norm["end"].max()))

    # дубликаты end
    rep["dups_end"] = int(norm["end"].duplicated().sum())

    # off-grid по 5м
    rep["offgrid_5m"] = int(offgrid_mask(norm["end"]).sum())

    # NaN
    rep["nan_OPEN"]  = int(norm["OPEN"].isna().sum())
    rep["nan_HIGH"]  = int(norm["HIGH"].isna().sum())
    rep["nan_LOW"]   = int(norm["LOW"].isna().sum())
    rep["nan_CLOSE"] = int(norm["CLOSE"].isna().sum())
    rep["nan_vol"]   = int(norm["volume"].isna().sum())

    # отрицательный объём
    rep["neg_vol"] = int((norm["volume"] < 0).fillna(False).sum())

    # логика OHLC: HIGH >= max(OPEN,CLOSE,LOW) и LOW <= min(...)
    hi_bad = (norm["HIGH"] < norm[["OPEN","LOW","CLOSE"]].max(axis=1))
    lo_bad = (norm["LOW"]  > norm[["OPEN","LOW","CLOSE"]].min(axis=1))
    rep["ohlc_inconsistent"] = int((hi_bad | lo_bad).sum())

    # spillover: строки не принадлежат ожидаемому дню из имени файла (если имя вида si_5m_YYYY-MM-DD.csv)
    fname = os.path.basename(path)
    spill = 0
    for part in fname.split("_"):
        if len(part) == 10 and part[:4].isdigit() and part[4] == "-" and part[7] == "-":
            target_day = part
            spill = int((norm["end"].dt.date.astype(str) != target_day).sum())
            break
    rep["spillover_out_of_day"] = spill

    # статус
    bad_flags = [
        rep["rows_norm"] == 0,
        rep["dups_end"] > 0,
        rep["offgrid_5m"] > 0,
        rep["nan_CLOSE"] > 0,
        rep["ohlc_inconsistent"] > 0,
        rep["spillover_out_of_day"] > 0
    ]
    rep["status"] = "OK" if not any(bad_flags) else "NEEDS_FIX"
    return rep

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pattern", default="si_5m_*.csv", help="глоб-паттерн YTD файлов")
    args = ap.parse_args()

    files = sorted(glob.glob(args.pattern))
    if not files:
        print(f"Нет файлов по паттерну: {args.pattern}")
        return

    rows = []
    for f in files:
        try:
            rows.append(per_file_report(f))
        except Exception as e:
            rows.append({"file": os.path.basename(f), "status": f"ERROR: {e}"})

    df = pd.DataFrame(rows)
    # суммарная сводка
    total = len(df)
    bad = int((df["status"] != "OK").sum())
    print(f"\n=== SUMMARY ===")
    print(f"Checked files: {total}, Issues: {bad}")
    if bad:
        print("\nTop problematic files:")
        cols = ["file","status","rows","rows_norm","dups_end","offgrid_5m","nan_CLOSE","ohlc_inconsistent","spillover_out_of_day"]
        print(df[df["status"]!="OK"][cols].head(25).to_string(index=False))

    # детальная таблица (первые 50 строк) — чтобы глазами посмотреть
    print("\n=== DETAILS (first 50) ===")
    cols_show = ["file","status","min_end","max_end","rows_norm","dups_end","offgrid_5m",
                 "nan_OPEN","nan_HIGH","nan_LOW","nan_CLOSE","nan_vol","neg_vol","ohlc_inconsistent","spillover_out_of_day"]
    print(df[cols_show].head(50).to_string(index=False))

    # Сохраним полный отчёт в CSV
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"diag_backfill_report_{ts}.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved report: {out}")

if __name__ == "__main__":
    main()
