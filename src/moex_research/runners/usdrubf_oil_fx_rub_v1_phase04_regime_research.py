from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Final, Mapping

import numpy as np
import pandas as pd

from moex_research.features.oil_fx_rub_v1_features import prepare_identity_panel
from moex_research.runners import (
    usdrubf_oil_fx_rub_v1_pit_research_dataset as phase02b,
)
from moex_research.runners import (
    usdrubf_phase6_internal_modeling_dataset_builder as phase6_builder,
)

PROJECT: Final[str] = "MOEX_Bot"
TASK_ID: Final[str] = "STRAT_OIL_FX_RUB_V1_04_REGIME_RESEARCH"
CONTRACT_ID: Final[str] = "usdrubf_oil_fx_rub_v1_phase04_regime_research"
CONTRACT_VERSION: Final[str] = "1.0"
EXPECTED_IDENTITY_COUNT: Final[int] = 472
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"

WINDOW: Final[int] = 126
MIN_HISTORY: Final[int] = 63
HIGH_THRESHOLD: Final[float] = 0.75
LOW_THRESHOLD: Final[float] = 0.25
HORIZONS: Final[tuple[int, ...]] = (1, 3, 5, 10, 20)
CONFIRMATION_HORIZONS: Final[tuple[int, ...]] = (5, 10, 20)
BOOTSTRAP_SAMPLES: Final[int] = 1000
BOOTSTRAP_SEED: Final[int] = 20260916
BOOTSTRAP_BLOCK_LENGTH: Final[int] = 20
MIN_REGIME_OBSERVATIONS: Final[int] = 15
ROBUST_MIN_OBSERVATIONS: Final[int] = 20

EXPECTED_SHA256: Final[dict[str, str]] = {
    "phase6_modeling_dataset": "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00",
    "phase6_dataset_manifest": "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082",
    "brent_pit_acceptance_matrix": "78b60c9542fc08667267849b9ce03fdf161d2dc971fe99e1ab9d6a8d56266c43",
    "phase84a_gate_results": "aceaefb4d2e2a236539dd527c98464ddd1ea6bf5f1cdb8121662e1ce087f9c4c",
    "phase84a_input_identity": "3fa20b2daf45f196937064b5f1cc6a58b8009e544d6141e32a77d841d68b65ae",
}

DECLARED_OUTPUTS: Final[tuple[str, ...]] = (
    "input_identity_verification.json",
    "regime_observations.parquet",
    "regime_metrics.csv",
    "regime_temporal_stability.csv",
    "hypothesis_summary.json",
    "research_manifest.json",
    "gate_results.json",
)

REGIME_ORDER: Final[tuple[str, ...]] = (
    "high_high",
    "high_low",
    "low_high",
    "low_low",
    "neutral",
    "warmup",
)

_ALIAS_PATTERN = re.compile(r"(^|[/\\._-])(latest|current|autodetect)($|[/\\._-])", re.I)
_SHA40_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GLOB_CHARS = frozenset("*?[]")


class Phase04RegimeError(ValueError):
    """Fail-closed error for Oil/FX/RUB Phase 04 regime research."""


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Phase04RegimeError(f"invalid JSON evidence: {path}") from exc
    if not isinstance(payload, dict):
        raise Phase04RegimeError(f"JSON evidence must be an object: {path}")
    return payload


def _explicit_file(raw: object, flag: str, suffix: str) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase04RegimeError(f"{flag} must identify one explicit immutable file")
    path = Path(text)
    if path.suffix.lower() != suffix or not path.exists() or not path.is_file():
        raise Phase04RegimeError(f"{flag} file or suffix mismatch")
    return path


def _explicit_output_dir(raw: object) -> Path:
    text = str(raw).strip()
    if not text or any(char in text for char in _GLOB_CHARS) or _ALIAS_PATTERN.search(text):
        raise Phase04RegimeError("--output-dir must be explicit and immutable")
    path = Path(text)
    if path.exists():
        raise Phase04RegimeError("--output-dir must not pre-exist")
    return path


def _validate_contract(contract: Mapping[str, Any]) -> None:
    identity = contract.get("contract_identity")
    if not isinstance(identity, Mapping):
        raise Phase04RegimeError("contract_identity is required")
    expected = {
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "project": PROJECT,
        "task_id": TASK_ID,
    }
    for key, value in expected.items():
        if identity.get(key) != value:
            raise Phase04RegimeError(f"contract identity mismatch: {key}")

    scope = contract.get("scope")
    if not isinstance(scope, Mapping):
        raise Phase04RegimeError("scope is required")
    if scope.get("brent_only") is not True or scope.get("cnyrubf_allowed") is not False:
        raise Phase04RegimeError("Phase 04 must remain brent_only")
    if scope.get("oil_rub_product_allowed") is not False:
        raise Phase04RegimeError("Oil_RUB product must remain forbidden")
    if scope.get("target_day_data_allowed") is not False:
        raise Phase04RegimeError("target-day data must remain forbidden")

    methodology = contract.get("methodology")
    expected_method = {
        "rolling_window_sessions": WINDOW,
        "minimum_history_sessions": MIN_HISTORY,
        "high_threshold": HIGH_THRESHOLD,
        "low_threshold": LOW_THRESHOLD,
        "forward_return_horizons_sessions": list(HORIZONS),
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "bootstrap_block_length_sessions": BOOTSTRAP_BLOCK_LENGTH,
        "minimum_regime_observations": MIN_REGIME_OBSERVATIONS,
        "robust_minimum_observations": ROBUST_MIN_OBSERVATIONS,
        "no_threshold_optimization": True,
        "no_horizon_optimization": True,
    }
    if not isinstance(methodology, Mapping):
        raise Phase04RegimeError("methodology is required")
    for key, value in expected_method.items():
        if methodology.get(key) != value:
            raise Phase04RegimeError(f"methodology mismatch: {key}")

    if contract.get("runtime_artifacts") != list(DECLARED_OUTPUTS):
        raise Phase04RegimeError("runtime artifact inventory mismatch")

    authority = contract.get("authority_boundary")
    if not isinstance(authority, Mapping) or any(value is not False for value in authority.values()):
        raise Phase04RegimeError("authority boundary was widened")


def _validate_immutable_hashes(paths: Mapping[str, Path]) -> dict[str, str]:
    observed = {name: _sha256(path) for name, path in paths.items()}
    failed = [
        name for name, expected in EXPECTED_SHA256.items()
        if observed.get(name) != expected
    ]
    if failed:
        raise Phase04RegimeError("immutable input hash mismatch: " + ", ".join(sorted(failed)))
    return observed


def _validate_brent_evidence(gates: Mapping[str, Any]) -> None:
    if len(gates) != 9:
        raise Phase04RegimeError("Brent gate inventory must contain exactly G1-G9")
    for index, (key, value) in enumerate(gates.items(), start=1):
        if not str(key).startswith(f"G{index}_"):
            raise Phase04RegimeError("Brent gate order must be exactly G1-G9")
        if not isinstance(value, Mapping) or value.get("passed") is not True:
            raise Phase04RegimeError(f"Brent evidence failed at {key}")
    final = next(value for key, value in gates.items() if str(key).startswith("G9_"))
    if final.get("status") != "moex_brent_source_candidate_for_phase8_5":
        raise Phase04RegimeError("Brent source is not admitted")


def _normalize_dates(series: pd.Series, label: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        raise Phase04RegimeError(f"{label} contains invalid date")
    return parsed.dt.strftime("%Y-%m-%d").astype("string")


def _rolling_percentile(values: np.ndarray, window: int = WINDOW, min_history: int = MIN_HISTORY) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=float)
    for index in range(len(values)):
        start = max(0, index - window + 1)
        history = values[start:index + 1]
        history = history[np.isfinite(history)]
        if len(history) < min_history:
            continue
        current = values[index]
        if not np.isfinite(current):
            continue
        less = int(np.sum(history < current))
        equal = int(np.sum(history == current))
        result[index] = (less + 0.5 * equal) / len(history)
    return result


def _classify_regime(oil_pct: float, usd_pct: float) -> str:
    if not np.isfinite(oil_pct) or not np.isfinite(usd_pct):
        return "warmup"
    oil_high = oil_pct >= HIGH_THRESHOLD
    oil_low = oil_pct <= LOW_THRESHOLD
    usd_high = usd_pct >= HIGH_THRESHOLD
    usd_low = usd_pct <= LOW_THRESHOLD
    if oil_high and usd_high:
        return "high_high"
    if oil_high and usd_low:
        return "high_low"
    if oil_low and usd_high:
        return "low_high"
    if oil_low and usd_low:
        return "low_low"
    return "neutral"


def _prepare_observations(
    modeling_dataset: pd.DataFrame,
    source_panel: pd.DataFrame,
    brent_matrix: pd.DataFrame,
) -> pd.DataFrame:
    identities = prepare_identity_panel(modeling_dataset)
    required_brent = {
        "target_trade_date",
        "target_instrument_id",
        "prior_trade_date",
        "brent_trade_date",
        "brent_close",
        "brent_contract_code",
    }
    if not required_brent.issubset(brent_matrix.columns):
        raise Phase04RegimeError("Brent PIT matrix schema mismatch")

    brent = brent_matrix.copy()
    for column in ("target_trade_date", "prior_trade_date", "brent_trade_date"):
        brent[column] = _normalize_dates(brent[column], f"Brent {column}")
    brent["target_instrument_id"] = brent["target_instrument_id"].astype(str)
    if len(brent) != EXPECTED_IDENTITY_COUNT:
        raise Phase04RegimeError("Brent identity count mismatch")
    if not brent.loc[:, ["target_trade_date", "target_instrument_id"]].reset_index(drop=True).equals(
        identities.loc[:, ["target_trade_date", "target_instrument_id"]].astype(str).reset_index(drop=True)
    ):
        raise Phase04RegimeError("Brent identity/order mismatch")
    if not brent["prior_trade_date"].equals(identities["prior_trade_date"].astype("string")):
        raise Phase04RegimeError("Brent prior_trade_date mismatch")
    if not brent["brent_trade_date"].equals(brent["prior_trade_date"]):
        raise Phase04RegimeError("Brent close must be prior_trade_date close")

    phase02b.validate_phase6_source_panel_replay(modeling_dataset, source_panel)

    panel = phase6_builder._prepare_internal_d1_panel(source_panel)
    panel_dates = _normalize_dates(panel["trade_date"], "Phase6 source trade_date")
    panel_instruments = panel["instrument_id"].astype(str)
    if set(panel_instruments) != {EXPECTED_INSTRUMENT}:
        raise Phase04RegimeError("Phase6 source instrument mismatch")
    closes = pd.to_numeric(panel["close"], errors="coerce").to_numpy(float)
    if not np.isfinite(closes).all() or np.any(closes <= 0):
        raise Phase04RegimeError("Phase6 source close must be finite positive")
    date_to_index = {str(value): idx for idx, value in enumerate(panel_dates)}

    brent_close = pd.to_numeric(brent["brent_close"], errors="coerce").to_numpy(float)
    if not np.isfinite(brent_close).all() or np.any(brent_close <= 0):
        raise Phase04RegimeError("Brent close must be finite positive")

    usd_prior_close = np.full(EXPECTED_IDENTITY_COUNT, np.nan, dtype=float)
    for idx, prior_date in enumerate(identities["prior_trade_date"].astype(str)):
        panel_idx = date_to_index.get(prior_date)
        if panel_idx is None:
            raise Phase04RegimeError(f"Phase6 source missing prior_trade_date {prior_date}")
        usd_prior_close[idx] = closes[panel_idx]

    oil_pct = _rolling_percentile(brent_close)
    usd_pct = _rolling_percentile(usd_prior_close)
    regimes = [_classify_regime(a, b) for a, b in zip(oil_pct, usd_pct)]

    result = identities.loc[:, ["target_trade_date", "target_instrument_id", "prior_trade_date"]].copy()
    result["brent_contract_code"] = brent["brent_contract_code"].astype(str).to_numpy()
    result["brent_prior_close"] = brent_close
    result["usdrubf_prior_close"] = usd_prior_close
    result["brent_percentile_126"] = oil_pct
    result["usdrubf_percentile_126"] = usd_pct
    result["regime"] = regimes

    terminal_index = len(panel) - 1
    for horizon in HORIZONS:
        values = np.full(EXPECTED_IDENTITY_COUNT, np.nan, dtype=float)
        for row_idx, target_date in enumerate(identities["target_trade_date"].astype(str)):
            base_idx = date_to_index.get(target_date)
            if base_idx is None:
                raise Phase04RegimeError(f"Phase6 source missing target date {target_date}")
            endpoint = base_idx + horizon
            if endpoint >= terminal_index:
                continue
            values[row_idx] = closes[endpoint] / closes[base_idx] - 1.0
        result[f"fwd_usdrubf_close_return_{horizon}session"] = values
    return result


def _circular_block_indices(n: int, rng: np.random.Generator) -> np.ndarray:
    if n <= 0:
        return np.empty(0, dtype=int)
    block_count = int(math.ceil(n / BOOTSTRAP_BLOCK_LENGTH))
    starts = rng.integers(0, n, size=block_count)
    offsets = np.arange(BOOTSTRAP_BLOCK_LENGTH)
    return np.concatenate([(start + offsets) % n for start in starts])[:n]


def _bootstrap_regime_ci(
    observations: pd.DataFrame,
    *,
    regime: str,
    label: str,
    seed: int,
) -> tuple[float, float, float, float]:
    raw = pd.to_numeric(observations[label], errors="coerce").to_numpy(float)
    valid_mask = np.isfinite(raw)
    work = observations.loc[valid_mask, ["regime", label]].reset_index(drop=True)
    original_regime_count = int(work["regime"].eq(regime).sum())
    if original_regime_count < MIN_REGIME_OBSERVATIONS:
        return (float("nan"),) * 4

    rng = np.random.default_rng(seed)
    regime_means: list[float] = []
    differences: list[float] = []
    for _ in range(BOOTSTRAP_SAMPLES):
        idx = _circular_block_indices(len(work), rng)
        sample = work.iloc[idx]
        all_values = pd.to_numeric(sample[label], errors="coerce").to_numpy(float)
        regime_values = pd.to_numeric(
            sample.loc[sample["regime"].eq(regime), label], errors="coerce"
        ).to_numpy(float)
        regime_values = regime_values[np.isfinite(regime_values)]
        if len(regime_values) == 0:
            continue
        regime_mean = float(np.mean(regime_values))
        regime_means.append(regime_mean)
        differences.append(regime_mean - float(np.mean(all_values)))

    minimum_draws = max(100, BOOTSTRAP_SAMPLES // 2)
    if len(regime_means) < minimum_draws:
        return (float("nan"),) * 4
    mean_low, mean_high = np.quantile(np.asarray(regime_means), [0.025, 0.975])
    diff_low, diff_high = np.quantile(np.asarray(differences), [0.025, 0.975])
    return float(mean_low), float(mean_high), float(diff_low), float(diff_high)


def _with_fixed_calendar_segments(observations: pd.DataFrame) -> pd.DataFrame:
    work = observations.copy()
    dates = pd.to_datetime(work["target_trade_date"], errors="coerce")
    if dates.isna().any() or not dates.is_monotonic_increasing:
        raise Phase04RegimeError("target_trade_date must be valid/increasing for stability")
    start = dates.iloc[0]
    end = dates.iloc[-1]
    if end <= start:
        raise Phase04RegimeError("research calendar span must be positive")
    span = end - start
    cut1 = start + span / 3
    cut2 = start + span * 2 / 3
    segment = np.where(
        dates <= cut1,
        "early",
        np.where(dates <= cut2, "middle", "late"),
    )
    work["_calendar_segment"] = segment
    return work


def _calendar_segment_statistics(
    work: pd.DataFrame,
    *,
    regime: str,
    label: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    negative_mean_count = 0
    negative_diff_count = 0
    for segment in ("early", "middle", "late"):
        segment_frame = work.loc[work["_calendar_segment"].eq(segment)]
        all_values = pd.to_numeric(segment_frame[label], errors="coerce").to_numpy(float)
        all_values = all_values[np.isfinite(all_values)]
        regime_values = pd.to_numeric(
            segment_frame.loc[segment_frame["regime"].eq(regime), label],
            errors="coerce",
        ).to_numpy(float)
        regime_values = regime_values[np.isfinite(regime_values)]
        count = len(regime_values)
        mean = float(np.mean(regime_values)) if count else float("nan")
        hit = float(np.mean(regime_values < 0.0)) if count else float("nan")
        unconditional = float(np.mean(all_values)) if len(all_values) else float("nan")
        difference = (
            mean - unconditional
            if np.isfinite(mean) and np.isfinite(unconditional)
            else float("nan")
        )
        if np.isfinite(mean) and mean < 0.0:
            negative_mean_count += 1
        if np.isfinite(difference) and difference < 0.0:
            negative_diff_count += 1
        result[f"{segment}_count"] = count
        result[f"{segment}_mean"] = mean
        result[f"{segment}_short_hit_rate"] = hit
        result[f"{segment}_unconditional_mean"] = unconditional
        result[f"{segment}_mean_minus_unconditional"] = difference
    result["negative_mean_segment_count"] = negative_mean_count
    result["negative_difference_segment_count"] = negative_diff_count
    return result


def analyze_regimes(observations: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    work = _with_fixed_calendar_segments(observations)
    metrics: list[dict[str, Any]] = []
    stability: list[dict[str, Any]] = []
    seed_counter = 0

    for horizon in HORIZONS:
        label = f"fwd_usdrubf_close_return_{horizon}session"
        all_values = pd.to_numeric(work[label], errors="coerce").to_numpy(float)
        all_values = all_values[np.isfinite(all_values)]
        unconditional_mean = float(np.mean(all_values))

        for regime in REGIME_ORDER[:-1]:
            values = pd.to_numeric(
                work.loc[work["regime"].eq(regime), label],
                errors="coerce",
            ).to_numpy(float)
            values = values[np.isfinite(values)]
            n = len(values)
            mean = float(np.mean(values)) if n else float("nan")
            median = float(np.median(values)) if n else float("nan")
            short_hit = float(np.mean(values < 0.0)) if n else float("nan")
            diff = mean - unconditional_mean if np.isfinite(mean) else float("nan")
            mean_lo, mean_hi, diff_lo, diff_hi = _bootstrap_regime_ci(
                work,
                regime=regime,
                label=label,
                seed=BOOTSTRAP_SEED + seed_counter,
            )
            seed_counter += 1

            segment_stats = _calendar_segment_statistics(
                work, regime=regime, label=label
            )
            negative_mean_segments = int(segment_stats["negative_mean_segment_count"])
            negative_diff_segments = int(
                segment_stats["negative_difference_segment_count"]
            )

            if (
                regime == "high_high"
                and n >= ROBUST_MIN_OBSERVATIONS
                and np.isfinite(mean_hi)
                and mean_hi < 0.0
                and short_hit >= 0.60
                and negative_mean_segments == 3
            ):
                h5_evidence = "robust"
            elif (
                regime == "high_high"
                and n >= MIN_REGIME_OBSERVATIONS
                and mean < 0.0
                and short_hit > 0.55
                and negative_mean_segments >= 2
            ):
                h5_evidence = "suggestive"
            else:
                h5_evidence = "not_supported"

            if (
                regime == "high_high"
                and n >= ROBUST_MIN_OBSERVATIONS
                and mean < 0.0
                and np.isfinite(diff_hi)
                and diff_hi < 0.0
                and negative_diff_segments == 3
            ):
                h6_evidence = "robust"
            elif (
                regime == "high_high"
                and n >= MIN_REGIME_OBSERVATIONS
                and mean < 0.0
                and diff < 0.0
                and negative_diff_segments >= 2
            ):
                h6_evidence = "suggestive"
            else:
                h6_evidence = "not_supported"

            metrics.append(
                {
                    "regime": regime,
                    "horizon_sessions": horizon,
                    "valid_count": n,
                    "mean_forward_return": mean,
                    "median_forward_return": median,
                    "short_hit_rate": short_hit,
                    "mean_ci95_low": mean_lo,
                    "mean_ci95_high": mean_hi,
                    "unconditional_mean_return": unconditional_mean,
                    "mean_minus_unconditional": diff,
                    "difference_ci95_low": diff_lo,
                    "difference_ci95_high": diff_hi,
                    "h5_evidence_status": h5_evidence,
                    "h6_evidence_status": h6_evidence,
                }
            )
            stability.append(
                {
                    "regime": regime,
                    "horizon_sessions": horizon,
                    **segment_stats,
                }
            )

    metrics_df = pd.DataFrame(metrics)
    stability_df = pd.DataFrame(stability)

    hh = metrics_df.loc[metrics_df["regime"].eq("high_high")].set_index(
        "horizon_sessions"
    )
    confirming = hh.loc[list(CONFIRMATION_HORIZONS)]

    h5_robust_count = int(confirming["h5_evidence_status"].eq("robust").sum())
    h5_supported_count = int(
        confirming["h5_evidence_status"].isin(["robust", "suggestive"]).sum()
    )
    h5_negative_count = int(confirming["mean_forward_return"].lt(0.0).sum())
    if h5_robust_count >= 2:
        h5_status = "supported_robust"
    elif (
        h5_supported_count >= 2
        and h5_negative_count == len(CONFIRMATION_HORIZONS)
    ):
        h5_status = "supported_suggestive"
    else:
        h5_status = "not_supported_in_phase04"

    h6_robust_count = int(confirming["h6_evidence_status"].eq("robust").sum())
    h6_supported_count = int(
        confirming["h6_evidence_status"].isin(["robust", "suggestive"]).sum()
    )
    h6_diff_negative_count = int(
        confirming["mean_minus_unconditional"].lt(0.0).sum()
    )
    if h6_robust_count >= 2:
        h6_status = "supported_robust"
    elif (
        h6_supported_count >= 2
        and h6_diff_negative_count == len(CONFIRMATION_HORIZONS)
    ):
        h6_status = "supported_suggestive"
    else:
        h6_status = "not_supported_in_phase04"

    summary = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "analysis_scope": "brent_usdrubf_level_regimes_H5_H6",
        "exploratory_not_causal": True,
        "regime_definition": {
            "window_sessions": WINDOW,
            "min_history_sessions": MIN_HISTORY,
            "high_threshold": HIGH_THRESHOLD,
            "low_threshold": LOW_THRESHOLD,
            "primary_regime": "high_high",
            "primary_direction": "negative USDRUBF forward return",
            "temporal_stability_partition": "fixed chronological thirds of full research period",
            "bootstrap": {
                "method": "date_aligned_circular_block",
                "samples": BOOTSTRAP_SAMPLES,
                "block_length_sessions": BOOTSTRAP_BLOCK_LENGTH,
                "seed": BOOTSTRAP_SEED,
            },
        },
        "H5_divergence_catch_up": {
            "status": h5_status,
            "confirmation_horizons_sessions": list(CONFIRMATION_HORIZONS),
            "robust_confirmation_count": h5_robust_count,
            "supported_confirmation_count": h5_supported_count,
            "negative_mean_confirmation_count": h5_negative_count,
            "details": [
                {
                    "horizon_sessions": int(h),
                    "valid_count": int(hh.loc[h, "valid_count"]),
                    "mean_forward_return": float(
                        hh.loc[h, "mean_forward_return"]
                    ),
                    "short_hit_rate": float(hh.loc[h, "short_hit_rate"]),
                    "mean_ci95_low": float(hh.loc[h, "mean_ci95_low"]),
                    "mean_ci95_high": float(hh.loc[h, "mean_ci95_high"]),
                    "evidence_status": str(
                        hh.loc[h, "h5_evidence_status"]
                    ),
                }
                for h in HORIZONS
            ],
        },
        "H6_regime_dependency": {
            "status": h6_status,
            "confirmation_horizons_sessions": list(CONFIRMATION_HORIZONS),
            "robust_confirmation_count": h6_robust_count,
            "supported_confirmation_count": h6_supported_count,
            "negative_vs_unconditional_confirmation_count": h6_diff_negative_count,
            "details": [
                {
                    "horizon_sessions": int(h),
                    "mean_minus_unconditional": float(
                        hh.loc[h, "mean_minus_unconditional"]
                    ),
                    "difference_ci95_low": float(
                        hh.loc[h, "difference_ci95_low"]
                    ),
                    "difference_ci95_high": float(
                        hh.loc[h, "difference_ci95_high"]
                    ),
                    "evidence_status": str(
                        hh.loc[h, "h6_evidence_status"]
                    ),
                }
                for h in HORIZONS
            ],
        },
        "interpretation_boundary": (
            "This phase tests fixed, past-only level regimes. It does not "
            "establish causality, a tradable threshold, a position size, "
            "or a production signal."
        ),
        "model_fit_performed": False,
        "trading_rule_design_performed": False,
    }
    return metrics_df, stability_df, summary


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m moex_research.runners.usdrubf_oil_fx_rub_v1_phase04_regime_research"
    )
    for flag in (
        "--contract-path",
        "--phase6-modeling-dataset-path",
        "--phase6-dataset-manifest-path",
        "--phase6-source-panel-path",
        "--brent-matrix-path",
        "--brent-gate-results-path",
        "--brent-input-identity-path",
        "--output-dir",
        "--run-id",
        "--git-commit-sha",
    ):
        parser.add_argument(flag, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    contract_path = _explicit_file(args.contract_path, "--contract-path", ".json")
    phase6_modeling = _explicit_file(args.phase6_modeling_dataset_path, "--phase6-modeling-dataset-path", ".parquet")
    phase6_manifest = _explicit_file(args.phase6_dataset_manifest_path, "--phase6-dataset-manifest-path", ".json")
    phase6_source = _explicit_file(args.phase6_source_panel_path, "--phase6-source-panel-path", ".parquet")
    brent_matrix_path = _explicit_file(args.brent_matrix_path, "--brent-matrix-path", ".parquet")
    brent_gates_path = _explicit_file(args.brent_gate_results_path, "--brent-gate-results-path", ".json")
    brent_identity_path = _explicit_file(args.brent_input_identity_path, "--brent-input-identity-path", ".json")
    output_dir = _explicit_output_dir(args.output_dir)
    run_id = str(args.run_id).strip()
    git_sha = str(args.git_commit_sha).strip().lower()
    if not run_id or _ALIAS_PATTERN.search(run_id):
        raise Phase04RegimeError("--run-id must be explicit and immutable")
    if not _SHA40_PATTERN.fullmatch(git_sha):
        raise Phase04RegimeError("--git-commit-sha must be exactly 40 lowercase hex characters")

    contract = _read_json(contract_path)
    _validate_contract(contract)
    immutable_paths = {
        "phase6_modeling_dataset": phase6_modeling,
        "phase6_dataset_manifest": phase6_manifest,
        "brent_pit_acceptance_matrix": brent_matrix_path,
        "phase84a_gate_results": brent_gates_path,
        "phase84a_input_identity": brent_identity_path,
    }
    observed_hashes = _validate_immutable_hashes(immutable_paths)

    modeling = pd.read_parquet(phase6_modeling)
    source_panel = pd.read_parquet(phase6_source)
    brent_matrix = pd.read_parquet(brent_matrix_path)
    brent_gates = _read_json(brent_gates_path)
    brent_identity = _read_json(brent_identity_path)
    _validate_brent_evidence(brent_gates)
    if not brent_identity:
        raise Phase04RegimeError("Brent input identity evidence must be non-empty")

    observations = _prepare_observations(modeling, source_panel, brent_matrix)
    metrics, stability, summary = analyze_regimes(observations)

    gates = {
        "G1_phase6_lineage": {"passed": True},
        "G2_brent_admission": {"passed": True, "status": "moex_brent_source_candidate_for_phase8_5"},
        "G3_pit_levels": {"passed": True, "source_date": "prior_trade_date"},
        "G4_percentile_pit": {"passed": True, "window_sessions": WINDOW, "min_history_sessions": MIN_HISTORY},
        "G5_regime_fixed": {"passed": True, "high_threshold": HIGH_THRESHOLD, "low_threshold": LOW_THRESHOLD},
        "G6_labels": {"passed": True, "horizons_sessions": list(HORIZONS), "terminal_endpoint_policy": "structural_null"},
        "G7_no_oil_rub_leakage": {"passed": True, "oil_rub_product_constructed": False},
        "G8_research_only": {
            "passed": True,
            "network_access_performed": False,
            "model_fit_performed": False,
            "trading_rule_design_performed": False,
            "trading_action_performed": False,
        },
    }
    failed = [key for key, value in gates.items() if value.get("passed") is not True]
    gates["G9_final"] = {
        "passed": not failed,
        "failed_gates": failed,
        "status": "oil_fx_rub_phase04_regime_research_complete" if not failed else "blocked",
    }

    manifest = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": run_id,
        "git_commit_sha": git_sha,
        "identity_count": EXPECTED_IDENTITY_COUNT,
        "input_sha256": observed_hashes,
        "regime_counts": {key: int(value) for key, value in observations["regime"].value_counts().to_dict().items()},
        "runtime_artifacts": list(DECLARED_OUTPUTS),
        "network_access_performed": False,
        "upstream_artifact_mutation_performed": False,
        "model_fit_performed": False,
        "trading_rule_design_performed": False,
        "trading_action_performed": False,
    }
    identity_verification = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "eligible_identity_count": EXPECTED_IDENTITY_COUNT,
        "first_target_trade_date": str(observations["target_trade_date"].iloc[0]),
        "last_target_trade_date": str(observations["target_trade_date"].iloc[-1]),
        "instrument_id": EXPECTED_INSTRUMENT,
        "phase6_lineage_replayed": True,
        "brent_immutable_artifacts_verified": True,
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    observations.to_parquet(output_dir / "regime_observations.parquet", index=False)
    metrics.to_csv(output_dir / "regime_metrics.csv", index=False)
    stability.to_csv(output_dir / "regime_temporal_stability.csv", index=False)
    (output_dir / "input_identity_verification.json").write_text(
        json.dumps(identity_verification, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "hypothesis_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "research_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "gate_results.json").write_text(
        json.dumps(gates, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if tuple(sorted(path.name for path in output_dir.iterdir())) != tuple(sorted(DECLARED_OUTPUTS)):
        raise Phase04RegimeError("output artifact inventory mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
