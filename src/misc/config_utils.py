#!/usr/bin/env python3
# coding: utf-8
import json
from pathlib import Path

CFG_PATH = Path(".state/config.json")
DEFAULTS = {
    "k": 1.15,
    "liq_threshold": 0.5
}

def load_config() -> dict:
    if CFG_PATH.exists():
        try:
            data = json.loads(CFG_PATH.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
    else:
        data = {}
    # merge defaults
    for k, v in DEFAULTS.items():
        data.setdefault(k, v)
    return data

def save_config(cfg: dict):
    CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
    # только известные ключи
    data = {k: cfg.get(k, DEFAULTS[k]) for k in DEFAULTS.keys()}
    CFG_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

