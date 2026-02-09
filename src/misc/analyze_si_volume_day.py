#!/usr/bin/env python3
import argparse, os, sys, math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    raise KeyError(f"Required column not found. Tried: {candidates}")

def load_day(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Нормализация названий колонок
    cols = {c:c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    tcol = find_col(df, ["end","datetime","TRADEDATE_TIME","time","TIME","end_ts"])
    ocol = find_col(df, ["OPEN","open"])
    hcol = find_col(df, ["HIGH","high"])
    lcol = find_col(df, ["LOW","low"])
    ccol = find_col(df, ["CLOSE","close"])
    vcol = find_col(df, ["volume","VOL","vol"])
    oicol = None
    for cand in ["oi_total","OPENPOSITION","oi","OPENPOSITION_TOTAL","OPENPOSITION_SUM"]:
        if cand in df.columns:
            oicol = cand; break

    df = df[[tcol, ocol, hcol, lcol, ccol, vcol] + ([oicol] if oicol else [])].copy()
    df.columns = ["end","open","high","low","close","volume"] + (["oi"] if oicol else [])
    # Время
    df["end"] = pd.to_datetime(df["end"])
    df.sort_values("end", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Базовые признаки
    df["range"] = (df["high"] - df["low"]).abs()
    df["body"]  = (df["close"] - df["open"]).abs()
    df["dir"]   = np.sign(df["close"].diff()).fillna(0)
    df["ret"]   = df["close"].pct_change().fillna(0)

    # Объёмные скользящие
    win = 20 if len(df) >= 20 else max(5, len(df)//3 if len(df)//3>=5 else 5)
    df["v_ma"]  = df["volume"].rolling(win).mean()
    df["v_std"] = df["volume"].rolling(win).std(ddof=0)
    df["v_z"]   = (df["volume"] - df["v_ma"]) / df["v_std"].replace(0, np.nan)
    df["rng_ma"] = df["range"].rolling(win).mean()
    df["rng_p25"] = df["range"].rolling(win).quantile(0.25)
    df["rng_p75"] = df["range"].rolling(win).quantile(0.75)

    if "oi" in df.columns:
        df["oi_chg"] = df["oi"].diff().fillna(0)

    return df

def mark_patterns(df: pd.DataFrame) -> pd.DataFrame:
    marks = [[] for _ in range(len(df))]

    def add(i, tag):
        if i>=0 and i<len(marks): marks[i].append(tag)

    # 1) Climactic Volume: v_z >= 2 и длинная тень против хода последнего импульса
    prev_dir = 0
    for i in range(1, len(df)):
        d = df.iloc[i]
        # ориентируемся на последние 3 бара
        recent_dir = np.sign(df["close"].iloc[max(0,i-3):i].diff().sum())
        wick_up = (d["high"] - max(d["open"], d["close"])) / (d["range"] + 1e-9)
        wick_dn = (min(d["open"], d["close"]) - d["low"]) / (d["range"] + 1e-9)
        if d["v_z"] >= 2.0 and d["range"] > 0:
            if recent_dir > 0 and wick_up > 0.35: add(i, "ClimacticVolume")
            if recent_dir < 0 and wick_dn > 0.35: add(i, "ClimacticVolume")

    # 2) No Demand / No Supply: узкий спред и объём ниже среднего
    for i, d in df.iterrows():
        if np.isnan(d["rng_p25"]) or np.isnan(d["v_ma"]): continue
        narrow = d["range"] <= d["rng_p25"]
        lowvol = d["volume"] <= 0.7 * d["v_ma"]
        if narrow and lowvol:
            if d["close"] >= d["open"]: add(i, "NoDemand")
            else: add(i, "NoSupply")

    # 3) Stopping Volume: всплеск объёма + разворотный бар
    for i in range(1, len(df)):
        d = df.iloc[i]
        if d["v_z"] >= 1.5 and d["range"]>0:
            # разворотный контекст: после серии падений бар закрывается в верхней половине,
            # или после серии ростов закрывается в нижней половине
            prev3 = df.iloc[max(0,i-3):i]
            trend = np.sign(prev3["close"].diff().sum())
            close_pos = (d["close"] - d["low"]) / (d["range"] + 1e-9)  # 0..1
            if trend < 0 and close_pos > 0.6: add(i, "StoppingVolume")
            if trend > 0 and close_pos < 0.4: add(i, "StoppingVolume")

    # 4) Test / Shakeout: обновили минимум вчера/сегодня и быстро вернулись, объём невысок
    for i in range(1, len(df)):
        d = df.iloc[i]
        prev = df.iloc[i-1]
        low_break = d["low"] < prev["low"]
        quick_back = d["close"] > prev["close"]
        lowvol = (d["v_ma"]>0) and (d["volume"] < 0.9 * d["v_ma"])
        if low_break and quick_back and lowvol:
            add(i, "TestShakeout")

    # 5) Effort vs Result: высокий объём при узком диапазоне
    for i, d in df.iterrows():
        if np.isnan(d["rng_p25"]) or np.isnan(d["v_ma"]) or np.isnan(d["v_std"]): continue
        if d["v_z"] >= 1.2 and d["range"] <= d["rng_p25"]:
            add(i, "EffortVsResult")

    # 6) Absorption: 3 бара подряд с повышенным объёмом и малым суммарным прогрессом цены
    for i in range(2, len(df)):
        seg = df.iloc[i-2:i+1]
        if seg["v_z"].min() >= 1.0:
            progress = abs(seg["close"].iloc[-1] - seg["close"].iloc[0])
            avg_rng = seg["range"].mean()
            if progress <= 0.8 * avg_rng:
                add(i, "Absorption")

    # 7) Volume Divergence: локальный новый high при меньшем экстремуме объёма
    highs = df["high"].rolling(5).max()
    for i in range(5, len(df)):
        if df["high"].iloc[i] >= highs.iloc[i-1] and df["volume"].iloc[i] < df["volume"].iloc[i-1]:
            add(i, "VolumeDivergence")

    # 8) OI Divergence: знак изменения цены против знака изменения OI
    if "oi" in df.columns:
        for i in range(1, len(df)):
            pchg = df["close"].iloc[i] - df["close"].iloc[i-1]
            oichg = df["oi"].iloc[i] - df["oi"].iloc[i-1]
            if pchg>0 and oichg<0: add(i, "OIdiv_up_price_down_oi")
            if pchg<0 and oichg>0: add(i, "OIdiv_down_price_up_oi")

    df = df.copy()
    df["patterns"] = [";".join(m) if m else "" for m in marks]
    return df

def plot_day(df: pd.DataFrame, out_png: str):
    # Фигуры: верх — цена + объём; низ — OI (если есть)
    has_oi = "oi" in df.columns
    fig_h = 7 if has_oi else 5
    fig = plt.figure(figsize=(12, fig_h))
    gs = fig.add_gridspec(2 if has_oi else 1, 1, height_ratios=[3,1] if has_oi else [1])

    ax1 = fig.add_subplot(gs[0,0])
    t = df["end"]
    ax1.plot(t, df["close"], linewidth=1.2)
    ax1.set_title("Si — Close + Volume (5m)")
    ax1.set_ylabel("Price")

    # Объём — вторичная ось
    ax1b = ax1.twinx()
    ax1b.bar(t, df["volume"], width=0.003*(t.max()-t.min()).total_seconds(), alpha=0.35)
    ax1b.plot(t, df["v_ma"], linewidth=1.0, alpha=0.8)
    ax1b.set_ylabel("Volume")

    # Пометки паттернов точками сверху графика цены
    y = df["close"].values
    for i, patt in enumerate(df["patterns"]):
        if not patt: continue
        ax1.scatter(t.iloc[i], y[i], s=28, marker="o")

    # Легенда-список по уникальным паттернам
    # (цвет по умолчанию; акцент идёт через маркеры на графике)
    uniq = sorted({p for s in df["patterns"] if s for p in s.split(";")})
    if uniq:
        ax1.legend([f"{len([1 for s in df['patterns'] if p in s])}× {p}" for p in uniq],
                   loc="upper left", fontsize=8, frameon=False)

    if has_oi:
        ax2 = fig.add_subplot(gs[1,0], sharex=ax1)
        ax2.plot(t, df["oi"], linewidth=1.2)
        ax2.set_ylabel("Open Interest")
        ax2.set_title("Open Interest")

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--file", default=None, help="Path to si_5m_<date>.csv. If omitted, will try si_5m_<date>.csv in CWD.")
    args = ap.parse_args()

    path = args.file or f"si_5m_{args.date}.csv"
    if not os.path.exists(path):
        print(f"ERROR: file not found: {path}", file=sys.stderr)
        sys.exit(2)

    df = load_day(path)
    df = mark_patterns(df)

    out_csv = f"patterns_{args.date}.csv"
    df_out = df[["end","open","high","low","close","volume"] + (["oi"] if "oi" in df.columns else []) + ["patterns"]].copy()
    df_out.to_csv(out_csv, index=False)
    out_png = f"si_patterns_{args.date}.png"
    plot_day(df, out_png)

    print(f"Saved: {out_csv}")
    print(f"Saved: {out_png}")
    # Краткая сводка по паттернам
    from collections import Counter
    cnt = Counter()
    for s in df["patterns"]:
        if s:
            for p in s.split(";"):
                cnt[p]+=1
    if cnt:
        print("Summary:")
        for k,v in cnt.most_common():
            print(f"{k}: {v}")
    else:
        print("No patterns detected by current rules.")
if __name__ == "__main__":
    main()
