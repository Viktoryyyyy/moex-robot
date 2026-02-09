#!/usr/bin/env python3
import os, sys, requests, pandas as pd
from datetime import date

API = "https://apim.moex.com"
HDR = {"Authorization": "Bearer " + os.getenv("MOEX_API_KEY",""),
       "User-Agent": "moex_bot_futoi_export/1.0"}

def to_df(block):
    cols = block.get("columns")
    data = block.get("data")
    meta = block.get("metadata")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    else:
        if not cols and isinstance(meta, dict): cols = list(meta.keys())
        df = pd.DataFrame(data=data, columns=cols)
    df.columns = [str(c).lower() for c in df.columns]
    return df

def fetch_day(ticker:str, d:str)->pd.DataFrame:
    u = f"{API}/iss/analyticalproducts/futoi/securities/{ticker}.json?from={d}&till={d}"
    r = requests.get(u, headers=HDR, timeout=30)
    r.raise_for_status()
    j = r.json()
    blk = j.get("futoi") or j.get("securities") or {}
    df = to_df(blk)
    if "tradedate" in df.columns:
        df = df[df["tradedate"].astype(str).str[:10] == d]
    return df

def main():
    d = "2025-10-22"
    base = "si"
    out = f"futoi_{base}_{d}_joined.csv"

    df = fetch_day(base, d)
    # минимально необходимые поля
    need = ["tradedate","tradetime","ticker","clgroup","pos","pos_long","pos_short",
            "pos_long_num","pos_short_num","systime","trade_session_date"]
    for c in need:
        if c not in df.columns: df[c] = None

    # нормализуем время и сортируем
    df["_dt"] = pd.to_datetime(df["tradedate"].astype(str).str[:10] + " " + df["tradetime"].astype(str), errors="coerce")
    df = df.sort_values(["_dt","clgroup"])

    # оставляем только FIZ/YUR и делаем wide-join по таймстемпу
    cols_value = ["pos","pos_long","pos_short","pos_long_num","pos_short_num"]
    # формируем ключ времени
    df["ts"] = df["tradedate"].astype(str).str[:10] + " " + df["tradetime"].astype(str)
    df = df[df["clgroup"].isin(["FIZ","YUR"])]

    # сводная таблица: по каждой метрике префиксы FIZ_/YUR_
    wide_parts = []
    for grp in ["FIZ","YUR"]:
        part = df[df["clgroup"] == grp][["ts"] + cols_value].copy()
        part = part.rename(columns={c: f"{grp.lower()}_{c}" for c in cols_value})
        wide_parts.append(part)
    wide = wide_parts[0].merge(wide_parts[1], on="ts", how="outer")

    # добавим вспомогательные поля (ticker, tradedate, systime) из ближайших записей
    meta_cols = ["ticker","tradedate","tradetime","systime","trade_session_date"]
    meta = (df.sort_values("_dt")
              .groupby("ts", as_index=False)[meta_cols]
              .agg(lambda x: x.iloc[-1] if len(x) else None))
    res = meta.merge(wide, on="ts", how="left")

    # итоговый порядок колонок
    final_cols = ["tradedate","tradetime","ticker",
                  "fiz_pos","fiz_pos_long","fiz_pos_short","fiz_pos_long_num","fiz_pos_short_num",
                  "yur_pos","yur_pos_long","yur_pos_short","yur_pos_long_num","yur_pos_short_num",
                  "systime","trade_session_date"]
    # переименованные колонки уже в res
    # убедимся, что все есть
    for c in final_cols:
        if c not in res.columns: res[c] = None
    res = res[final_cols].sort_values(["tradedate","tradetime"])

    res.to_csv(out, index=False)
    print(f"OK: {out} rows={len(res)}")

if __name__ == "__main__":
    main()
