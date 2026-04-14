from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from src.moex_strategy_sdk.errors import StrategyRegistrationError


def resolve_external_pattern_artifact_path(*, locator_ref: str, environment_record: Mapping[str, object], format_kwargs: Mapping[str, object]) -> Path:
    if not isinstance(locator_ref, str) or not locator_ref:
        raise StrategyRegistrationError("locator_ref is required")
    artifact_root_refs = environment_record.get("artifact_root_refs")
    if not isinstance(artifact_root_refs, list) or len(artifact_root_refs) != 1:
        raise StrategyRegistrationError("runtime boundary requires exactly one artifact_root_ref")
    artifact_root_key = artifact_root_refs[0]
    if not isinstance(artifact_root_key, str) or not artifact_root_key:
        raise StrategyRegistrationError("invalid artifact_root_ref")
    artifact_root = os.environ.get(artifact_root_key)
    if not artifact_root:
        raise StrategyRegistrationError("missing required artifact root env var: " + artifact_root_key)
    return Path(artifact_root) / locator_ref.format(**format_kwargs)
