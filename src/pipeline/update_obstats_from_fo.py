#!/usr/bin/env python3
"""
Инкрементальное обновление OBSTATS 5m по FO-master.

Логика:
- FO-master: data/fo/si_5m_2020-01-01_<END>.csv (по умолчанию берём последний).
- OBSTATS-master (если есть): data/master/obstats_si_5m_*.csv.
- Если OBSTATS ещё нет: полный пересчёт через obstats_5m_full_from_master.py по всему FO.
- Если OBSTATS есть:
    * читаем существующий master (df_old),
    * last_day = max(end).date(),
    * строим временный FO-skeleton только по датам >= from_date (переписываем последний день),
    * запускаем obstats_5m_full_from_master.py по этому skeleton,
    * склеиваем df_old(<from_date) + новый хвост, сохраняем в data/master.
"""

import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import subprocess


ROOT = Path(__file__).resolve().parents[2]  # ~/moex_bot
DATA_FO = ROOT / "data" / "fo"
DATA_MASTER = ROOT / "data" / "master"


def find_script(name: str) -> Path:
    candidates = list(ROOT.rglob(name))
    if not candidates:
        raise FileNotFoundError("Не найден скрипт " + name + " под " + str(ROOT))
    return candidates[0]


def find_latest_file(pattern: str):
    files = sorted(ROOT.glob(pattern))
    return files[-1] if files else None


def run(cmd, env=None):
    print("RUN:", " ".join(cmd))
    subprocess.run(cmd, check=True, env=env)


def main():
    # 1) FO-master: берём последний si_5m_2020-01-01_*.csv
    fo_latest = find_latest_file("data/fo/si_5m_2020-01-01_*.csv")
    if fo_latest is None:
        raise SystemExit("FO-master не найден (data/fo/si_5m_2020-01-01_*.csv)")

    print("FO master:", fo_latest)
    fo = pd.read_csv(fo_latest)
    fo["end"] = pd.to_datetime(fo["end"])
    end_target = fo["end"].max().date()
    print("FO end_target:", end_target)

    # 2) Ищем существующий OBSTATS-master (если есть)
    obstats_existing = find_latest_file("data/master/obstats_si_5m_*.csv")

    obstats_script = find_script("obstats_5m_full_from_master.py")
    lib_path = find_script("lib_moex_api.py")
    lib_dir = lib_path.parent

    if obstats_existing is None:
        print("OBSTATS: master не найден, делаем полный пересчёт")
        env_full = os.environ.copy()
        env_full["MASTER_PATH"] = str(fo_latest)
        env_full["PYTHONPATH"] = str(lib_dir)

        run([sys.executable, str(obstats_script)], env=env_full)

        candidates = sorted(ROOT.glob("obstats_si_5m_*.csv"))
        if not candidates:
            raise FileNotFoundError(
                "Не найден obstats_si_5m_*.csv после полного пересчёта"
            )
        obstats_path = candidates[-1]
        dest = DATA_MASTER / obstats_path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(obstats_path.read_bytes())
        print("OBSTATS master:", dest)
        return

    # 3) Инкрементальный режим
    print("OBSTATS existing master:", obstats_existing)
    df_old = pd.read_csv(obstats_existing)
    df_old["end"] = pd.to_datetime(df_old["end"])
    last_day = df_old["end"].max().date()
    print("OBSTATS last_day:", last_day, "target_end:", end_target)

    if end_target < last_day:
        print("OBSTATS: end_target < last_day, обновление не требуется")
        return

    # Всегда переписываем последний день
    from_date = last_day
    print("OBSTATS update from_date:", from_date)

    # 4) Строим временный FO-skeleton только по датам >= from_date
    mask_tail = fo["end"].dt.date >= from_date
    fo_tail = fo.loc[mask_tail].copy()

    tmp_master = DATA_MASTER / (
        "_tmp_master_obstats_"
        + from_date.isoformat()
        + "_"
        + end_target.isoformat()
        + ".csv"
    )
    tmp_master.parent.mkdir(parents=True, exist_ok=True)
    fo_tail.to_csv(tmp_master, index=False)
    print("OBSTATS tmp MASTER_PATH:", tmp_master)

    # 5) Запускаем obstats_5m_full_from_master.py по временной выборке FO
    env_tail = os.environ.copy()
    env_tail["MASTER_PATH"] = str(tmp_master)
    env_tail["PYTHONPATH"] = str(lib_dir)
    run([sys.executable, str(obstats_script)], env=env_tail)

    candidates = sorted(ROOT.glob("obstats_si_5m_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            "Не найден obstats_si_5m_*.csv после инкрементального пересчёта"
        )
    obstats_tail_path = candidates[-1]
    print("OBSTATS tail file:", obstats_tail_path)

    df_tail = pd.read_csv(obstats_tail_path)
    df_tail["end"] = pd.to_datetime(df_tail["end"])

    # старые строки только до from_date-1
    mask_old = df_old["end"].dt.date < from_date
    df_base = df_old.loc[mask_old].copy()
    df_all = pd.concat([df_base, df_tail], ignore_index=True)
    df_all = df_all.sort_values("end").drop_duplicates("end", keep="last")

    start_str = df_all["end"].min().date().isoformat()
    out_name = "obstats_si_5m_" + start_str + "_" + end_target.isoformat() + ".csv"
    dest = DATA_MASTER / out_name
    dest.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(dest, index=False)

    print("OBSTATS master updated:", dest)
    print("Rows:", len(df_all))
    print("Start:", df_all["end"].min())
    print("End:  ", df_all["end"].max())

    try:
        tmp_master.unlink()
    except OSError:
        pass


if __name__ == "__main__":
    main()
