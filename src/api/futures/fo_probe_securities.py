#!/usr/bin/env python3
import pandas as pd
from lib_moex_api import get_json

def main():
    j = get_json(
        "/iss/engines/futures/markets/forts/boards/rfud/securities.json",
        {},
        timeout=20.0,
    )

    print("Top-level keys:", list(j.keys()))

    block = j.get("securities") or j.get("data") or {}
    print("Block type:", type(block))

    cols = block.get("columns")
    data = block.get("data")

    print("Columns:", cols)
    if not cols or not data:
        print("No columns or data in 'securities' block")
        return

    print(f"Total rows in securities: {len(data)}")

    df = pd.DataFrame(data, columns=cols)

    # Показать все колонки, которые выглядят как даты экспирации
    date_like = [c for c in df.columns if "MAT" in c.upper() or "LAST" in c.upper() or "DATE" in c.upper()]
    print("Date-like columns:", date_like)

    # Отфильтруем только Si*
    if "SECID" not in df.columns:
        print("SECID column not found")
        return

    df_si = df[df["SECID"].astype(str).str.startswith("Si")].copy()
    print(f"Rows with SECID starting 'Si': {len(df_si)}")

    # Выведем первые 10 строк по Si с ключевыми полями
    cols_to_show = ["SECID"] + [c for c in date_like if c in df_si.columns]
    cols_to_show = list(dict.fromkeys(cols_to_show))  # убрать дубли, сохранить порядок

    print("\nSample Si rows:")
    print(df_si[cols_to_show].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
