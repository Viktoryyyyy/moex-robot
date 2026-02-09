#!/usr/bin/env python3
import os, sys, time, json, signal, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

UA = os.getenv("MOEX_UA", "moex_bot_loop_cny_5m/1.1").strip()
API_PUBLIC = "https://iss.moex.com"
SECID = os.getenv("CNY_SECID", "CNYRUB_TOM")
MSK = ZoneInfo("Europe/Moscow")

POLL_SEC = int(os.getenv("POLL_SEC", "5"))
PARTIAL  = int(os.getenv("PARTIAL", "1"))  # 1=печатаем обновления внутри бара; 0=только финальные

def ceil_5min(ts: datetime) -> datetime:
    if ts.second == 0 and ts.minute % 5 == 0:
        return ts.replace(microsecond=0)
    m = (ts.minute // 5 + 1) * 5
    h = ts.hour
    d = ts
    if m >= 60:
        m -= 60; h += 1
        if h >= 24:
            h = 0; d = d + timedelta(days=1)
    return d.replace(hour=h, minute=m, second=0, microsecond=0)

class Bar5m:
    __slots__=("end","open","high","low","close","volume","turnover","trades")
    def __init__(self, end):
        self.end=end; self.open=None; self.high=None; self.low=None; self.close=None
        self.volume=0; self.turnover=0.0; self.trades=0
    def add(self, price, qty, val):
        if self.open is None:
            self.open=self.high=self.low=self.close=price
        else:
            if price>self.high: self.high=price
            if price<self.low:  self.low=price
            self.close=price
        self.volume += int(qty)
        self.turnover += float(val)
        self.trades += 1
    def sig(self):
        return (self.end, self.open, self.high, self.low, self.close, self.volume, round(self.turnover,2), self.trades)
    def to_dict(self, final=False):
        return {
            "end": self.end.strftime("%Y-%m-%d %H:%M:%S"),
            "open": self.open, "high": self.high, "low": self.low, "close": self.close,
            "volume": self.volume, "turnover": round(self.turnover,2), "trades": self.trades,
            "final": bool(final),
        }

def fetch_latest(start=0):
    url = f"{API_PUBLIC}/iss/engines/currency/markets/selt/boards/CETS/securities/{SECID}/trades.json"
    r = requests.get(url, params={"start":start}, headers={"User-Agent":UA,"Accept":"application/json"}, timeout=(5,25))
    r.raise_for_status()
    j = r.json()
    blk = j.get("trades",{})
    return blk.get("columns",[]), blk.get("data",[])

def main():
    seen=set()
    cur=None
    last_emitted_sig=None

    def emit(bar, final):
        nonlocal last_emitted_sig
        if not bar or bar.trades==0: return
        s = bar.sig()
        if final or PARTIAL:
            # частичные печатаем только при изменениях
            if final or s != last_emitted_sig:
                print(json.dumps(bar.to_dict(final=final), ensure_ascii=False), flush=True)
                last_emitted_sig = s

    def on_exit(sig,frm):
        emit(cur, True)
        sys.exit(0)

    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    while True:
        try:
            cols, data = fetch_latest(0)
            idx = {n:i for i,n in enumerate(cols)}
            need = ["TRADENO","TRADETIME","PRICE","QUANTITY","VALUE","SYSTIME"]
            if not all(k in idx for k in need):
                time.sleep(POLL_SEC); continue

            new=[]
            for row in data:
                tn = row[idx["TRADENO"]]
                if tn in seen: continue
                seen.add(tn)
                dstr = str(row[idx["SYSTIME"]]).split(" ")[0]
                tstr = str(row[idx["TRADETIME"]])
                try:
                    dt = datetime.fromisoformat(f"{dstr} {tstr}").replace(tzinfo=MSK)
                except Exception:
                    continue
                price=float(row[idx["PRICE"]]); qty=int(row[idx["QUANTITY"]]); val=float(row[idx["VALUE"]])
                new.append((dt, price, qty, val))
            new.sort(key=lambda x:x[0])

            for dt,price,qty,val in new:
                end = ceil_5min(dt)
                if (cur is None) or (end != cur.end):
                    # закрываем предыдущий бар
                    emit(cur, True)
                    # открываем новый
                    cur = Bar5m(end)
                    last_emitted_sig = None  # сброс, чтобы первый снапшот нового бара напечатался
                cur.add(price, qty, val)

            # периодический эмит «живого» бара (при изменениях)
            emit(cur, False)

        except Exception as e:
            print(json.dumps({"error": str(e)}, ensure_ascii=False), flush=True)
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
