#!/usr/bin/env python3
import os, sys, argparse, requests, pandas as pd

API = os.getenv("MOEX_API_URL", "https://apim.moex.com").rstrip("/")
UA  = os.getenv("MOEX_UA", "moex_bot_fx_lib/1.0").strip()

def headers():
    t = os.getenv("MOEX_API_KEY", "").strip()
    h = {"User-Agent": UA or "moex_bot_fx_lib/1.0"}
    if t:
        h["Authorization"] = "Bearer " + t
    return h

def get_json(path: str, params=None, timeout: int = 25):
    r = requests.get(f"{API}{path}", headers=headers(), params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def blocks(js: dict) -> dict:
    out = {}
    for k, v in js.items():
        if isinstance(v, dict) and "columns" in v and "data" in v:
            out[k] = {"columns": v.get("columns") or [], "data": v.get("data") or []}
        elif isinstance(v, list) and len(v) >= 2 and isinstance(v[0], dict) and "columns" in v[0]:
            cols = v[0].get("columns") or []
            data = v[1].get("data") or []
            out[k] = {"columns": cols, "data": data}
    return out

def to_df(block: dict) -> pd.DataFrame:
    cols = block.get("columns") or []
    data = block.get("data") or []
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return pd.DataFrame(data)
    return pd.DataFrame(data=data, columns=cols)

def resolve_fx_by_key(key: str, board: str = "CETS") -> str:
    js = get_json("/iss/engines/currency/markets/selt/boards/CETS/securities.json",
                  params={"securities.columns": "SECID,SHORTNAME,BOARDID"})
    b = blocks(js)
    sec_block = b.get("securities", {})
    df = to_df(sec_block)
    if df.empty:
        raise RuntimeError("CETS securities is empty")
    df = df[df["BOARDID"].astype(str).str.upper().eq(board.upper())].copy()
    ku = key.upper()
    df["score"] = (
        df["SECID"].astype(str).str.upper().str.contains(ku).astype(int) * 2 +
        df["SHORTNAME"].astype(str).str.upper().str.contains(ku).astype(int)
    )
    df = df[df["score"] > 0].copy()
    if df.empty:
        raise RuntimeError(f"no FX match for key={key}")
    df["is_TOM"] = df["SECID"].astype(str).str.upper().str.endswith("_TOM")
    df = df.sort_values(["is_TOM", "score", "SECID"], ascending=[False, False, True])
    return str(df.iloc[0]["SECID"])

def resolve_trade_date(secid: str) -> str:
    js = get_json(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}.json")
    b = blocks(js)
    dv = to_df(b.get("dataversion", {}))
    if dv.empty or "trade_session_date" not in dv.columns:
        raise RuntimeError("no dataversion.trade_session_date")
    return str(dv.iloc[0]["trade_session_date"])

def probe_fx(key: str, date: str = "auto"):
    secid = resolve_fx_by_key(key)
    day = resolve_trade_date(secid) if date == "auto" else date

    js_sec = get_json(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}.json")
    js_cand = get_json(f"/iss/engines/currency/markets/selt/boards/CETS/securities/{secid}/candles.json",
                       params={"from": day, "till": day, "interval": 1})

    print("SECID:", secid)
    print("DATE:", day)

    b_sec = blocks(js_sec)
    for name, blk in b_sec.items():
        df = to_df(blk)
        print(f"[securities.{name}] cols={list(df.columns)} rows={len(df)}")
        if not df.empty:
            print(df.head(3).to_string(index=False))
    b_c = blocks(js_cand)
    c_blk = b_c.get("candles", {})
    c_df = to_df(c_blk)
    print(f"[candles] cols={list(c_df.columns)} rows={len(c_df)}")
    if not c_df.empty:
        print(c_df.head(3).to_string(index=False))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True)
    ap.add_argument("--date", default="auto")
    args = ap.parse_args()
    try:
        probe_fx(args.key, args.date)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
