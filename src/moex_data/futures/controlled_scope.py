import json
from pathlib import Path

CONTROLLED_SCOPE = "controlled_batch_w_mm_mx"
DEFAULT_CONTROLLED_CONFIG = "configs/datasets/futures_controlled_batch_w_mm_mx_raw_scope_config.json"


def load_scope_config(root, config_path):
    rel = config_path or DEFAULT_CONTROLLED_CONFIG
    path = Path(rel)
    if not path.is_absolute():
        path = Path(root) / rel
    if not path.exists():
        raise FileNotFoundError("Missing controlled scope config: " + str(path))
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("universe_scope") != CONTROLLED_SCOPE:
        raise RuntimeError("Unsupported universe_scope")
    return data
