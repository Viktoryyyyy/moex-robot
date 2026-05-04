import hashlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo

TZ_MSK = ZoneInfo("Europe/Moscow")
DEFAULT_WHITELIST = ["SiM6", "SiU6", "SiU7", "SiZ6", "USDRUBF"]
DEFAULT_EXCLUDED = ["SiH7", "SiM7"]
SHORT_HISTORY_ALLOWED = {"SiU7"}


def today_msk():
    return datetime.now(TZ_MSK).date().isoformat()


def utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def stable_id(parts):
    return hashlib.sha256("|".join([str(x) for x in parts]).encode("utf-8")).hexdigest()[:24]


def parse_list(value, default):
    text = str(value or "").strip()
    if not text:
        return list(default)
    return [x.strip() for x in text.split(",") if x.strip()]


def print_json_line(key, value):
    print(key + ": " + json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))
