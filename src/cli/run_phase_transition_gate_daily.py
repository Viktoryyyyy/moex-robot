from __future__ import annotations

import json
import os
from datetime import datetime, timezone


def _die(msg: str, code: int = 2) -> None:
    print("[CRIT] " + msg)
    raise SystemExit(code)


def _load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                s = raw.strip()
                if not s or s.startswith("#"):
                    continue
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = k.strip()
                v = v.strip()
                if not k:
                    continue
                if v.startswith("\"") and v.endswith("\"") and len(v) >= 2:
                    v = v[1:-1]
                if k not in os.environ:
                    os.environ[k] = v
    except Exception as e:
        _die("failed to load .env: err=" + str(e))


def _atomic_write_text(path: str, text: str) -> None:
    import tempfile
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=d or None)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _read_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        _die("failed to read json: path=" + path + " err=" + str(e))
    return {}


def _read_history_last_date(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        import csv
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = list(csv.DictReader(f))
        if not r:
            return ""
        last = r[-1].get("date") or ""
        return str(last).strip()
    except Exception as e:
        _die("failed to read history csv: path=" + path + " err=" + str(e))
    return ""


def main() -> None:
    _load_dotenv()
    if not os.getenv("MOEX_API_KEY"):
        _die("MOEX_API_KEY missing")
    cfg = "config/phase_transition_p10.json"
    out_prod = "data/gate/phase_transition_risk.json"
    out_payload = "data/gate/phase_transition_gate_payload.json"
    out_5m = "data/gate/si_5m_D-1.csv"
    out_day = "data/gate/day_metrics_D-1.csv"
    hist = "data/state/rel_range_history.csv"
    stamp = "data/gate/phase_transition_gate_daily.last_success.json"
    hist_before = _read_history_last_date(hist)
    try:
        from src.cli.daily_metrics_builder import main as build_daily_main
    except Exception as e:
        _die("cannot import daily_metrics_builder: err=" + str(e))
    try:
        from src.cli.phase_transition_gate import main as gate_main
    except Exception as e:
        _die("cannot import phase_transition_gate: err=" + str(e))
    import sys as _sys
    _argv0 = list(_sys.argv)
    try:
        _sys.argv = ["daily_metrics_builder", "--key", "Si", "--date", "D-1", "--out-5m", out_5m, "--out-day", out_day]
        build_daily_main()
    finally:
        _sys.argv = _argv0
    dm = _read_json(out_day) if out_day.endswith(".json") else None
    # out_day is CSV; read the single row without pandas
    try:
        import csv
        with open(out_day, "r", encoding="utf-8", newline="") as f:
            rr = list(csv.DictReader(f))
        if len(rr) != 1:
            _die("day_metrics must have exactly 1 row: got=" + str(len(rr)))
        asof = str((rr[0].get("date") or "")).strip()
        if not asof:
            _die("day_metrics missing date")
    except Exception as e:
        _die("failed to read day_metrics csv: err=" + str(e))
    _argv0 = list(_sys.argv)
    try:
        _sys.argv = ["phase_transition_gate", "--in-day", out_day, "--in-history", hist, "--config", cfg, "--out-json", out_payload, "--out-history", hist]
        gate_main()
    finally:
        _sys.argv = _argv0
    payload = _read_json(out_payload)
    risk = payload.get("phase_transition_risk")
    try:
        risk_i = int(risk)
    except Exception:
        _die("invalid risk in payload: " + str(risk))
    if risk_i not in (0, 1):
        _die("risk must be 0 or 1, got=" + str(risk_i))
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    prod = {"asof_date": asof, "phase_transition_risk": risk_i, "updated_at": updated_at}
    _atomic_write_text(out_prod, json.dumps(prod, ensure_ascii=False, indent=2) + "\n")
    _atomic_write_text(stamp, json.dumps({"asof_date": asof, "updated_at": updated_at}, ensure_ascii=False, indent=2) + "\n")
    hist_after = _read_history_last_date(hist)
    grew = 1 if (hist_before and hist_after and hist_after > hist_before) else 0
    if not hist_after:
        _die("rel_range_history.csv missing/empty after run")
    if hist_after != asof:
        _die("rel_range_history.csv last_date mismatch: hist_last=" + hist_after + " asof=" + asof)
    print("[GateDaily] status=PASS risk={r} asof={a} updated_at={u} history_last={h} history_grew={g}".format(r=risk_i, a=asof, u=updated_at, h=hist_after, g=grew))


if __name__ == "__main__":
    main()
