#!/usr/bin/env python3
"""
Единый апдейтер master-датасета Si + CNY + FUTOI + OBSTATS.

Логика:
1) FO: инкрементальное обновление Si 5m через fo_tradestats_chain.py
   (всегда переписываем последний день; при timeout MOEX — сохраняем старый файл,
    master не пересобираем, скрипт завершается без traceback).
2) FX: инкрементальное обновление CNYRUB_TOM 5m через fx_5m_period.py
   (всегда переписываем последний день).
3) Собираем skeleton Si+CNY за полный период.
4) FUTOI: инкрементальный пересчёт через futoi_5m_full_from_master.py
   (переписываем последний день, не тянем каждый раз с 2020).
5) OBSTATS: инкрементальный пересчёт через obstats_5m_full_from_master.py
   (переписываем последний день, с корректным PYTHONPATH).
6) Собираем master_5m_si_cny_futoi_obstats_<start>_<end>.csv.

Запуск:
    cd ~/moex_bot
    source venv/bin/activate
    set -a; source .env; set +a
    python src/pipeline/update_master_si_cny_futoi_obstats.py --end 2025-11-27
"""

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo
import subprocess


TZ_MSK = ZoneInfo("Europe/Moscow")

ROOT = Path(__file__).resolve().parents[2]  # ~/moex_bot
DATA_FO = ROOT / "data" / "fo"
DATA_FX = ROOT / "data" / "fx"
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


def update_fo(
    start_fixed: date,
    end_target: date,
    min_from: date | None = None,
    bounded_tail_refresh: bool = False,
):
    """
    FO: инкрементальное обновление si_5m_2020-01-01_<END>.csv.

    Возвращает (path, ok_flag).

    Логика:
    - если файла нет: качаем с start_fixed..end_target;
    - если файл есть:
        * читаем его, берём last_day = max(end).date()
        * ВСЕГДА перекачиваем last_day..end_target
        * старые бары за last_day и дальше удаляем, подставляем свежий хвост.
    - если MOEX отвалился (timeout/No data):
        * если базового файла нет — аварийный выход;
        * если базовый файл есть — пишем WARN, возвращаем (старый путь, False).
    """
    DATA_FO.mkdir(parents=True, exist_ok=True)
    pattern = "data/fo/si_5m_2020-01-01_*.csv"
    latest = find_latest_file(pattern)

    df_old = None

    if latest is None:
        print("FO latest: нет файлов, качаем с нуля")
        from_date = start_fixed
    else:
        print("FO latest file:", latest)
        df_old = pd.read_csv(latest)
        df_old["end"] = pd.to_datetime(df_old["end"])
        last_day = df_old["end"].max().date()
        if end_target >= last_day:
            from_date = last_day
        else:
            from_date = end_target

    if bounded_tail_refresh and min_from is not None:
        from_date = min_from
    elif min_from and from_date < min_from:
        from_date = min_from

    if from_date > end_target:
        print("FO: from_date > end_target, обновление не требуется")
        # обновление не нужно, но базовый файл есть -> ok = True
        return latest, True

    print("FO update:", from_date, "..", end_target)

    fo_chain = find_script("fo_tradestats_chain.py")
    tail_name = (
        "si_5m_"
        + from_date.isoformat()
        + "_"
        + end_target.isoformat()
        + ".csv"
    )
    tail_path = ROOT / tail_name

    cmd = [
        sys.executable,
        str(fo_chain),
        "--base",
        "Si",
        "--from",
        from_date.isoformat(),
        "--till",
        end_target.isoformat(),
    ]

    try:
        run(cmd)
    except subprocess.CalledProcessError as e:
        if latest is None:
            print("ERROR: FO update failed и базового файла нет. Нечего сохранять.")
            raise SystemExit(1)
        print("WARN: FO update failed (MOEX timeout/No data). Keeping existing FO file.")
        return latest, False

    if not tail_path.exists():
        if latest is None:
            print("ERROR: FO хвостовой файл не создан и базового файла нет.")
            raise SystemExit(1)
        print("WARN: FO tail file not found. Keeping existing FO file.")
        return latest, False

    df_tail = pd.read_csv(tail_path)
    df_tail["end"] = pd.to_datetime(df_tail["end"])

    if df_old is None:
        df_all = df_tail
    else:
        mask = df_old["end"].dt.date < from_date
        df_base = df_old.loc[mask].copy()
        df_all = pd.concat([df_base, df_tail], ignore_index=True)

    df_all["end"] = pd.to_datetime(df_all["end"])
    df_all = df_all.sort_values("end").drop_duplicates("end", keep="last")

    out = DATA_FO / ("si_5m_2020-01-01_" + end_target.isoformat() + ".csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(out, index=False)

    print("FO updated:", out)
    print("Rows:", len(df_all))
    print("Start:", df_all["end"].min())
    print("End:  ", df_all["end"].max())
    return out, True


def update_fx(
    start_fixed: date,
    end_target: date,
    min_from: date | None = None,
    bounded_tail_refresh: bool = False,
) -> Path:
    """
    FX: инкрементальное обновление fx_5m_2020-01-01_<END>_cnyrub_tom.csv.

    Логика как у FO (переписываем последний день),
    но без специальной обработки timeout (пока что не было проблем).
    """
    DATA_FX.mkdir(parents=True, exist_ok=True)
    pattern = "data/fx/fx_5m_2020-01-01_*_cnyrub_tom.csv"
    latest = find_latest_file(pattern)

    df_old = None

    if latest is None:
        print("FX latest: нет файлов, качаем с нуля")
        from_date = start_fixed
    else:
        print("FX latest file:", latest)
        df_old = pd.read_csv(latest)
        df_old["end"] = pd.to_datetime(df_old["end"])
        last_day = df_old["end"].max().date()
        if end_target >= last_day:
            from_date = last_day
        else:
            from_date = end_target

    if bounded_tail_refresh and min_from is not None:
        from_date = min_from
    elif min_from and from_date < min_from:
        from_date = min_from

    if from_date > end_target:
        print("FX: from_date > end_target, обновление не требуется")
        return latest

    print("FX update:", from_date, "..", end_target)

    fx_period = find_script("fx_5m_period.py")
    tail_name = (
        "fx_5m_"
        + from_date.isoformat()
        + "_"
        + end_target.isoformat()
        + "_cnyrub_tom.csv"
    )
    tail_path = ROOT / tail_name

    env = os.environ.copy()
    fx_lib_dir = find_script("fx_lib_api.py").parent
    env["PYTHONPATH"] = str(fx_lib_dir)

    cmd = [
        sys.executable,
        str(fx_period),
        "--key",
        "CNYRUB",
        "--start",
        from_date.isoformat(),
        "--end",
        end_target.isoformat(),
        "--out",
        tail_name,
    ]
    run(cmd, env=env)

    if not tail_path.exists():
        raise FileNotFoundError("Хвостовой FX-файл не найден: " + str(tail_path))

    df_tail = pd.read_csv(tail_path)
    df_tail["end"] = pd.to_datetime(df_tail["end"])

    if df_old is None:
        df_all = df_tail
    else:
        mask = df_old["end"].dt.date < from_date
        df_base = df_old.loc[mask].copy()
        df_all = pd.concat([df_base, df_tail], ignore_index=True)

    df_all["end"] = pd.to_datetime(df_all["end"])
    df_all = df_all.sort_values("end").drop_duplicates("end", keep="last")

    out = DATA_FX / (
        "fx_5m_2020-01-01_" + end_target.isoformat() + "_cnyrub_tom.csv"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(out, index=False)

    print("FX updated:", out)
    print("Rows:", len(df_all))
    print("Start:", df_all["end"].min())
    print("End:  ", df_all["end"].max())
    return out


def build_skeleton(fo_path: Path, fx_path: Path) -> Path:
    """
    Собираем skeleton Si+CNY: si_cny_5m_2020-01-01_<END>.csv
    """
    DATA_MASTER.mkdir(parents=True, exist_ok=True)
    fo = pd.read_csv(fo_path)
    fx = pd.read_csv(fx_path)

    fo["end"] = pd.to_datetime(fo["end"])
    fx["end"] = pd.to_datetime(fx["end"])

    fo = fo.rename(
        columns={
            "open": "open_fo",
            "high": "high_fo",
            "low": "low_fo",
            "close": "close_fo",
            "volume": "volume_fo",
            "ticker": "ticker_fo",
        }
    )

    fx = fx.rename(
        columns={
            "open": "open_fx",
            "high": "high_fx",
            "low": "low_fx",
            "close": "close_fx",
            "volume": "volume_fx",
            "ticker": "ticker_fx",
        }
    )

    df = fo.merge(
        fx[["end", "open_fx", "high_fx", "low_fx", "close_fx", "volume_fx", "ticker_fx"]],
        on="end",
        how="left",
    )

    end_str = fo["end"].max().date().isoformat()
    out = DATA_MASTER / ("si_cny_5m_2020-01-01_" + end_str + ".csv")
    df.to_csv(out, index=False)

    print("Skeleton Si+CNY:", out)
    print("Columns:", list(df.columns))
    print("Start:", df["end"].min())
    print("End:  ", df["end"].max())
    print("Rows: ", len(df))
    return out


def build_futoi(
    master_path: Path,
    min_from: date | None = None,
    bounded_tail_refresh: bool = False,
) -> Path:
    """
    FUTOI: инкрементальный режим.

    Если master FUTOI отсутствует:
        - полный пересчёт через futoi_5m_full_from_master.py.
    Если есть:
        - читаем существующий futoi_si_5m_*.csv,
        - last_day = max(end).date(),
        - строим временный skeleton по master_path только с датами >= from_date,
        - прогоняем futoi_5m_full_from_master.py по этому skeleton,
        - склеиваем старый файл + новый хвост (с переписыванием последнего дня).
    """
    futoi_script = find_script("futoi_5m_full_from_master.py")

    futoi_existing = find_latest_file("data/master/futoi_si_5m_*.csv")

    sk = pd.read_csv(master_path)
    sk["end"] = pd.to_datetime(sk["end"])
    end_target = sk["end"].max().date()

    if futoi_existing is None:
        print("FUTOI: master не найден, делаем полный пересчёт")
        env_full = os.environ.copy()
        env_full["MASTER_PATH"] = str(master_path)
        env_full["FUTOI_ASSET"] = "si"
        run([sys.executable, str(futoi_script)], env=env_full)

        candidates = sorted(ROOT.glob("futoi_si_5m_*.csv"))
        if not candidates:
            raise FileNotFoundError(
                "Не найден futoi_si_5m_*.csv после полного пересчёта"
            )
        futoi_path = candidates[-1]
        dest = DATA_MASTER / futoi_path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(futoi_path.read_bytes())
        print("FUTOI file:", dest)
        return dest

    print("FUTOI existing master:", futoi_existing)
    df_old = pd.read_csv(futoi_existing)
    df_old["end"] = pd.to_datetime(df_old["end"])
    last_day = df_old["end"].max().date()
    print("FUTOI last_day:", last_day, "target_end:", end_target)

    if end_target >= last_day:
        from_date = last_day
    else:
        from_date = end_target

    if bounded_tail_refresh and min_from is not None:
        from_date = min_from
    elif min_from and from_date < min_from:
        from_date = min_from

    if from_date > end_target:
        print("FUTOI: from_date > end_target, обновление не требуется")
        return futoi_existing

    mask_tail = sk["end"].dt.date >= from_date
    sk_tail = sk.loc[mask_tail].copy()

    tmp_master = DATA_MASTER / (
        "_tmp_master_futoi_"
        + from_date.isoformat()
        + "_"
        + end_target.isoformat()
        + ".csv"
    )
    tmp_master.parent.mkdir(parents=True, exist_ok=True)
    sk_tail.to_csv(tmp_master, index=False)
    print("FUTOI tmp master:", tmp_master)

    env_tail = os.environ.copy()
    env_tail["MASTER_PATH"] = str(tmp_master)
    env_tail["FUTOI_ASSET"] = "si"
    run([sys.executable, str(futoi_script)], env=env_tail)

    candidates = sorted(ROOT.glob("futoi_si_5m_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            "Не найден futoi_si_5m_*.csv после инкрементального пересчёта"
        )
    futoi_tail_path = candidates[-1]
    print("FUTOI tail file:", futoi_tail_path)

    df_tail = pd.read_csv(futoi_tail_path)
    df_tail["end"] = pd.to_datetime(df_tail["end"])

    mask_old = df_old["end"].dt.date < from_date
    df_base = df_old.loc[mask_old].copy()
    df_all = pd.concat([df_base, df_tail], ignore_index=True)
    df_all = df_all.sort_values("end").drop_duplicates("end", keep="last")

    start_str = df_all["end"].min().date().isoformat()
    out_name = "futoi_si_5m_" + start_str + "_" + end_target.isoformat() + ".csv"
    dest = DATA_MASTER / out_name
    dest.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(dest, index=False)

    print("FUTOI master updated:", dest)
    print("Rows:", len(df_all))
    print("Start:", df_all["end"].min())
    print("End:  ", df_all["end"].max())

    try:
        tmp_master.unlink()
    except OSError:
        pass

    return dest


def build_obstats(
    fo_path: Path,
    min_from: date | None = None,
    bounded_tail_refresh: bool = False,
) -> Path:
    """
    OBSTATS: инкрементальный режим.

    Если master OBSTATS отсутствует:
        - полный пересчёт через obstats_5m_full_from_master.py по всему FO-master.
    Если есть:
        - читаем существующий obstats_si_5m_*.csv,
        - last_day = max(end).date(),
        - строим временный FO-skeleton только по датам >= from_date,
        - прогоняем obstats_5m_full_from_master.py по этому skeleton,
        - склеиваем старый файл + новый хвост (с переписыванием последнего дня).
    """
    obstats_script = find_script("obstats_5m_full_from_master.py")
    lib_path = find_script("lib_moex_api.py")
    lib_dir = lib_path.parent

    fo = pd.read_csv(fo_path)
    fo["end"] = pd.to_datetime(fo["end"])
    end_target = fo["end"].max().date()

    obstats_existing = find_latest_file("data/master/obstats_si_5m_*.csv")

    if obstats_existing is None:
        print("OBSTATS: master не найден, делаем полный пересчёт")
        env_full = os.environ.copy()
        env_full["MASTER_PATH"] = str(fo_path)
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
        return dest

    print("OBSTATS existing master:", obstats_existing)
    df_old = pd.read_csv(obstats_existing)
    df_old["end"] = pd.to_datetime(df_old["end"])
    last_day = df_old["end"].max().date()
    print("OBSTATS last_day:", last_day, "target_end:", end_target)

    if end_target < last_day:
        print("OBSTATS: end_target < last_day, обновление не требуется")
        return obstats_existing

    from_date = last_day
    if bounded_tail_refresh and min_from is not None:
        from_date = min_from
    elif min_from and from_date < min_from:
        from_date = min_from
    print("OBSTATS update from_date:", from_date)

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

    return dest


def build_master_all(
    skeleton_path: Path,
    futoi_path: Path,
    obstats_path: Path,
    out_master_path: Path | None = None,
) -> Path:
    """
    Итоговый master: merge skeleton + FUTOI + OBSTATS по end.
    """
    sk = pd.read_csv(skeleton_path)
    fu = pd.read_csv(futoi_path)
    ob = pd.read_csv(obstats_path)

    sk["end"] = pd.to_datetime(sk["end"])
    fu["end"] = pd.to_datetime(fu["end"])
    ob["end"] = pd.to_datetime(ob["end"])

    df = sk.merge(fu, on="end", how="left").merge(ob, on="end", how="left")

    start_str = df["end"].min().date().isoformat()
    end_str = df["end"].max().date().isoformat()

    out_name = "master_5m_si_cny_futoi_obstats_" + start_str + "_" + end_str + ".csv"
    out = out_master_path if out_master_path is not None else (DATA_MASTER / out_name)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    print("Merged master saved:", out)
    print("Rows:", len(df))
    print("Start:", df["end"].min())
    print("End:  ", df["end"].max())
    print("Columns:", list(df.columns))
    return out


def resolve_default_end() -> date:
    return date.today()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--end",
        help="Конечная дата YYYY-MM-DD (по умолчанию — сегодняшняя)",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        help="Нижняя граница пересчёта YYYY-MM-DD для bounded tail-refresh",
    )
    parser.add_argument(
        "--bounded-tail-refresh",
        action="store_true",
        help="Форсировать пересчёт хвоста с --from на всех слоях (FO/FX/FUTOI/OBSTATS)",
    )
    parser.add_argument(
        "--out-master-path",
        help="Явный путь для итогового master CSV",
    )
    args = parser.parse_args()

    start_fixed = date(2020, 1, 1)
    end_target = date.fromisoformat(args.end) if args.end else resolve_default_end()
    min_from = date.fromisoformat(args.from_date) if args.from_date else None
    out_master_path = Path(args.out_master_path) if args.out_master_path else None

    print("ROOT:", ROOT)
    print("Target end date:", end_target)

    fo_path, fo_ok = update_fo(
        start_fixed,
        end_target,
        min_from=min_from,
        bounded_tail_refresh=args.bounded_tail_refresh,
    )
    if not fo_ok:
        print("FO update failed (MOEX timeout/No data). Master не пересобирается.")
        return

    fx_path = update_fx(
        start_fixed,
        end_target,
        min_from=min_from,
        bounded_tail_refresh=args.bounded_tail_refresh,
    )
    skeleton_path = build_skeleton(fo_path, fx_path)
    futoi_path = build_futoi(
        skeleton_path,
        min_from=min_from,
        bounded_tail_refresh=args.bounded_tail_refresh,
    )
    obstats_path = build_obstats(
        fo_path,
        min_from=min_from,
        bounded_tail_refresh=args.bounded_tail_refresh,
    )
    build_master_all(
        skeleton_path,
        futoi_path,
        obstats_path,
        out_master_path=out_master_path,
    )


if __name__ == "__main__":
    main()
