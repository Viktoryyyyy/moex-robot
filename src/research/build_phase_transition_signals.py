import os
import glob
import pandas as pd
import numpy as np

OUT_PATH = "data/research/phase_transition_signals.csv"
LABELS_PATH = "data/research/ema_pnl_transition_days.csv"

TRAIN_START = "2020-01-01"
TRAIN_END   = "2023-12-31"
TEST_START  = "2024-01-01"
TEST_END    = "2025-12-31"

def pick_master_path():
    cand = sorted(glob.glob("data/master/master_5m_si_cny_futoi_obstats_*with*_regime*.csv"))
    if cand:
        return cand[-1]
    cand = sorted(glob.glob("data/master/master_5m_si_cny_futoi_obstats_*.csv"))
    if cand:
        return cand[-1]
    raise FileNotFoundError("No master file found in data/master/")

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def third_thursday(year, month):
    d = pd.Timestamp(year=year, month=month, day=1)
    offset = (3 - d.weekday()) % 7
    first_thu = d + pd.Timedelta(days=offset)
    return (first_thu + pd.Timedelta(days=14)).normalize()

def add_calendar(df_day):
    df_day["weekday"] = df_day["date"].dt.weekday
    df_day["is_mon"] = (df_day["weekday"] == 0).astype(int)
    df_day["is_fri"] = (df_day["weekday"] == 4).astype(int)

    exp_months = [3, 6, 9, 12]
    exps = []
    for d in df_day["date"]:
        y = int(d.year)
        m = int(d.month)
        next_m = None
        next_y = y
        for qm in exp_months:
            if qm >= m:
                next_m = qm
                break
        if next_m is None:
            next_m = 3
            next_y = y + 1
        exps.append(third_thursday(next_y, next_m))
    df_day["days_to_expiry"] = (pd.to_datetime(exps) - df_day["date"]).dt.days
    df_day["is_expiry_week"] = (df_day["days_to_expiry"].between(0, 4)).astype(int)
    return df_day

def zscore_past(series, window=60):
    mu = series.shift(1).rolling(window=window, min_periods=20).mean()
    sd = series.shift(1).rolling(window=window, min_periods=20).std(ddof=0)
    return (series - mu) / sd

def build_daily_from_5m(df):
    if "end" in df.columns:
        ts = pd.to_datetime(df["end"])
    elif "datetime" in df.columns:
        ts = pd.to_datetime(df["datetime"])
    elif ("TRADEDATE" in df.columns) and ("TIME" in df.columns):
        ts = pd.to_datetime(df["TRADEDATE"].astype(str) + " " + df["TIME"].astype(str))
    else:
        raise KeyError("No timestamp columns found (end/datetime/TRADEDATE+TIME).")

    df = df.copy()
    df["ts"] = ts
    df["date"] = df["ts"].dt.normalize()

    ocol = find_col(df, ["open_fo", "open", "OPEN", "pr_open"])
    hcol = find_col(df, ["high_fo", "high", "HIGH", "pr_high"])
    lcol = find_col(df, ["low_fo", "low", "LOW", "pr_low"])
    ccol = find_col(df, ["close_fo", "close", "CLOSE", "pr_close"])
    vcol = find_col(df, ["volume_fo", "volume", "VOL", "vol", "VOLUME"])

    if not all([ocol, hcol, lcol, ccol]):
        raise KeyError(f"Missing OHLC columns. Found: open={ocol}, high={hcol}, low={lcol}, close={ccol}")

    fx_close = find_col(df, ["close_fx", "CLOSE_FX", "cny_close", "CNY_CLOSE", "close_cny", "CLOSE_CNY"])
    fx_open  = find_col(df, ["open_fx", "OPEN_FX", "cny_open", "CNY_OPEN", "open_cny", "OPEN_CNY"])

    oi_fiz = find_col(df, ["pos_fiz", "POS_FIZ", "oi_fiz", "OI_FIZ", "openposition_fiz", "OPENPOSITION_FIZ"])
    oi_yur = find_col(df, ["pos_yur", "POS_YUR", "oi_yur", "OI_YUR", "openposition_yur", "OPENPOSITION_YUR"])

    liq = find_col(df, ["liq_smooth", "LIQ_SMOOTH", "liq", "LIQ"])
    spread_l1 = find_col(df, ["spread_l1_mean", "spread_l1", "SPREAD_L1_MEAN"])
    spread_l5 = find_col(df, ["spread_l5_mean", "spread_l5", "SPREAD_L5_MEAN"])
    spread_l20 = find_col(df, ["spread_l20_mean", "spread_l20", "SPREAD_L20_MEAN"])

    g = df.sort_values("ts").groupby("date", as_index=False)

    day = g.agg(
        open_first=(ocol, "first"),
        close_last=(ccol, "last"),
        high_max=(hcol, "max"),
        low_min=(lcol, "min"),
        vol_sum=(vcol, "sum") if vcol else (ccol, "size"),
    )

    day["range"] = day["high_max"] - day["low_min"]
    day["rel_range"] = day["range"] / day["close_last"].replace(0, np.nan)
    day["trend_ratio"] = (day["close_last"] - day["open_first"]).abs() / day["range"].replace(0, np.nan)

    if fx_close is not None:
        fx_day = g.agg(fx_close_last=(fx_close, "last"))
        if fx_open is not None:
            fx_day2 = g.agg(fx_open_first=(fx_open, "first"))
            fx_day = fx_day.merge(fx_day2, on="date", how="left")
            fx_day["cny_ret"] = (fx_day["fx_close_last"] / fx_day["fx_open_first"].replace(0, np.nan)) - 1.0
        else:
            fx_day["cny_ret"] = fx_day["fx_close_last"].pct_change()
        day = day.merge(fx_day[["date", "cny_ret"]], on="date", how="left")
    else:
        day["cny_ret"] = np.nan

    if (oi_fiz is not None) and (oi_yur is not None):
        oi_day = g.agg(oi_fiz_last=(oi_fiz, "last"), oi_yur_last=(oi_yur, "last"))
        oi_day["oi_total"] = oi_day["oi_fiz_last"].fillna(0) + oi_day["oi_yur_last"].fillna(0)
        day = day.merge(oi_day[["date", "oi_total"]], on="date", how="left")
    else:
        day["oi_total"] = np.nan

    if liq is not None:
        liq_day = g.agg(liq_mean=(liq, "mean"))
        day = day.merge(liq_day, on="date", how="left")
    else:
        day["liq_mean"] = np.nan

    for name, col in [("spread_l1_mean", spread_l1), ("spread_l5_mean", spread_l5), ("spread_l20_mean", spread_l20)]:
        if col is not None:
            sday = g.agg(**{name: (col, "mean")})
            day = day.merge(sday, on="date", how="left")
        else:
            day[name] = np.nan

    day = day.sort_values("date").reset_index(drop=True)

    for c in ["range", "rel_range", "trend_ratio", "cny_ret", "liq_mean",
              "spread_l1_mean", "spread_l5_mean", "spread_l20_mean", "oi_total"]:
        day[c + "_yday"] = day[c].shift(1)

    day["d_range_yday"] = day["range"].shift(1) - day["range"].shift(2)
    day["d_rel_range_yday"] = day["rel_range"].shift(1) - day["rel_range"].shift(2)
    day["d_oi_yday"] = day["oi_total"].shift(1) - day["oi_total"].shift(2)
    day["d_cny_ret_yday"] = day["cny_ret"].shift(1) - day["cny_ret"].shift(2)

    day["vol_z"] = zscore_past(day["rel_range"], window=60)
    day["vol_z_yday"] = day["vol_z"].shift(1)

    day = add_calendar(day)
    return day

def build_conditions(train_df):
    cuts = {}
    cols = [
        "range_yday","rel_range_yday","trend_ratio_yday","vol_z_yday",
        "liq_mean_yday","spread_l1_mean_yday","spread_l5_mean_yday","spread_l20_mean_yday",
        "oi_total_yday","cny_ret_yday"
    ]
    for c in cols:
        if c not in train_df.columns:
            continue
        s = train_df[c].dropna()
        if len(s) >= 200:
            cuts[c] = {"q10": float(s.quantile(0.10)), "q90": float(s.quantile(0.90))}
    return cuts

def eval_feature(df, label_col, mask):
    n = int(mask.sum())
    if n <= 0:
        return np.nan, 0
    return float(df.loc[mask, label_col].mean()), n

def ok_rule(base_rate, cond_rate, n):
    if (n is None) or (n < 30) or np.isnan(cond_rate) or np.isnan(base_rate) or base_rate <= 0:
        return False
    lift = cond_rate / base_rate
    return (lift >= 1.20) and ((cond_rate - base_rate) >= 0.02)

def build_rows(df_train, df_test, label_col, event_type):
    rows = []
    base_train = float(df_train[label_col].mean())
    base_test  = float(df_test[label_col].mean())

    cuts = build_conditions(df_train)

    for c, q in cuts.items():
        q10, q90 = q["q10"], q["q90"]

        for side, thr, op in [("top10", q90, ">="), ("bot10", q10, "<=")]:
            if op == ">=":
                m_tr = df_train[c] >= thr
                m_te = df_test[c] >= thr
            else:
                m_tr = df_train[c] <= thr
                m_te = df_test[c] <= thr

            cr_tr, n_tr = eval_feature(df_train, label_col, m_tr)
            cr_te, n_te = eval_feature(df_test, label_col, m_te)

            lift_tr = (cr_tr / base_train) if (base_train > 0 and not np.isnan(cr_tr)) else np.nan
            lift_te = (cr_te / base_test) if (base_test > 0 and not np.isnan(cr_te)) else np.nan

            rows.append({
                "feature": f"{c}_{side}",
                "event_type": event_type,
                "base_rate": base_train,
                "conditional_rate": cr_tr,
                "lift": lift_tr,
                "train_ok": int(ok_rule(base_train, cr_tr, n_tr)),
                "test_ok": int(ok_rule(base_test, cr_te, n_te)),
            })

    for c in ["d_range_yday","d_rel_range_yday","d_oi_yday","d_cny_ret_yday","days_to_expiry","is_expiry_week","is_mon","is_fri"]:
        if c not in df_train.columns:
            continue

        if c in ("is_expiry_week","is_mon","is_fri"):
            m_tr = df_train[c] == 1
            m_te = df_test[c] == 1
            name = f"{c}_eq1"
        elif c == "days_to_expiry":
            m_tr = df_train[c].between(0, 3)
            m_te = df_test[c].between(0, 3)
            name = f"{c}_le3"
        else:
            m_tr = df_train[c] > 0
            m_te = df_test[c] > 0
            name = f"{c}_pos"

        cr_tr, n_tr = eval_feature(df_train, label_col, m_tr)
        cr_te, n_te = eval_feature(df_test, label_col, m_te)

        lift_tr = (cr_tr / base_train) if (base_train > 0 and not np.isnan(cr_tr)) else np.nan
        lift_te = (cr_te / base_test) if (base_test > 0 and not np.isnan(cr_te)) else np.nan

        rows.append({
            "feature": name,
            "event_type": event_type,
            "base_rate": base_train,
            "conditional_rate": cr_tr,
            "lift": lift_tr,
            "train_ok": int(ok_rule(base_train, cr_tr, n_tr)),
            "test_ok": int(ok_rule(base_test, cr_te, n_te)),
        })

    return rows

def main():
    master_path = pick_master_path()
    print("MASTER:", master_path)
    if not os.path.exists(LABELS_PATH):
        raise FileNotFoundError(f"Missing labels file: {LABELS_PATH}")

    df = pd.read_csv(master_path)
    day = build_daily_from_5m(df)

    labs = pd.read_csv(LABELS_PATH)
    if "date" not in labs.columns:
        raise KeyError(f"Labels file has no 'date' column. cols={list(labs.columns)}")
    for c in ["enter_trend","vol_upshift"]:
        if c not in labs.columns:
            raise KeyError(f"Labels file missing '{c}'. cols={list(labs.columns)}")

    labs = labs[["date","enter_trend","vol_upshift"]].copy()
    labs["date"] = pd.to_datetime(labs["date"]).dt.normalize()

    day = day.merge(labs, on="date", how="inner")
    day = day.dropna(subset=["enter_trend","vol_upshift"]).copy()

    day["date"] = pd.to_datetime(day["date"])
    tr = day[(day["date"] >= TRAIN_START) & (day["date"] <= TRAIN_END)].copy()
    te = day[(day["date"] >= TEST_START) & (day["date"] <= TEST_END)].copy()

    if len(tr) < 300 or len(te) < 100:
        raise RuntimeError(f"Not enough days for split. train={len(tr)} test={len(te)}")

    print(f"DAYS: train={len(tr)} test={len(te)}")
    print(f"BASE enter_trend train={tr['enter_trend'].mean():.4f} test={te['enter_trend'].mean():.4f}")
    print(f"BASE vol_upshift train={tr['vol_upshift'].mean():.4f} test={te['vol_upshift'].mean():.4f}")

    rows = []
    rows += build_rows(tr, te, "enter_trend", "enter_trend")
    rows += build_rows(tr, te, "vol_upshift", "vol_upshift")

    out = pd.DataFrame(rows)
    out = out.sort_values(["event_type","test_ok","train_ok","lift"], ascending=[True, False, False, False])

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_csv(OUT_PATH, index=False)

    print("OUT:", OUT_PATH)
    print(out.head(25).to_string(index=False))

if __name__ == "__main__":
    main()
