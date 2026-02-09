#!/usr/bin/env python3
# pivot FUTOI by clgroup (YUR/FIZ) in-place
import os, sys, argparse, pandas as pd

p=argparse.ArgumentParser()
p.add_argument("--ticker", default="si")
p.add_argument("--date", required=True)  # YYYY-MM-DD
a=p.parse_args()

path=f"futoi_{a.ticker}_{a.date}.csv"
if not os.path.exists(path):
    print(f"не найден {path}"); sys.exit(1)

df=pd.read_csv(path)

# key = tradedate + tradetime
lc={c.lower():c for c in df.columns}
dcol=lc.get("tradedate") or lc.get("date") or lc.get("trade_date")
tcol=lc.get("tradetime") or lc.get("time") or lc.get("trade_time")
if not dcol or not tcol:
    print("нет tradedate/tradetime в FUTOI"); sys.exit(2)
df["key"]=df[dcol].astype(str)+" "+df[tcol].astype(str)

# тип клиента: clgroup (ожидаем YUR/FIZ)
ctype = lc.get("clgroup")
if not ctype:
    print("нет clgroup в FUTOI. Колонки:", list(df.columns)); sys.exit(3)

# числовые метрики на пивот (исключим служебные)
num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
drop = {ctype,"key","seqnum"}
num_cols=[c for c in num_cols if c not in drop]
if not num_cols:
    print("нет числовых метрик для пивота"); sys.exit(4)

# pivot по clgroup
wide = df.pivot_table(index="key", columns=ctype, values=num_cols, aggfunc="first")

# нормализуем имена групп (yur/fiz/прочие буквенно-цифровые)
def norm(x): 
    return str(x).strip().lower()

wide.columns = [f"fo_{m}_{norm(g)}" for m,g in wide.columns.to_flat_index()]
wide = wide.reset_index()

# сохранить с бэкапом
bak = path + ".bak"
os.replace(path, bak)
wide.to_csv(path, index=False)
print(f"pivot ok -> {path} (backup: {bak}); cols: {len(wide.columns)} rows: {len(wide)}")
print("пример колонок:", [c for c in wide.columns if c.startswith("fo_")][:8])
