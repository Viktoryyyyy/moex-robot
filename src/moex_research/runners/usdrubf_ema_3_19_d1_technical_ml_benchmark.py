from __future__ import annotations
import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Mapping, Sequence
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, average_precision_score, balanced_accuracy_score, brier_score_loss, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from src.moex_research.runners.usdrubf_ema_3_19_d1_rule_gate_benchmark import EXPERIMENT_ID as M4B_EXPERIMENT_ID, RULE_NAMES as FIXED_RULE_NAMES, SOURCE_EXPERIMENT_ID as M4A_EXPERIMENT_ID, _validate_quality_report as _validate_m4a_quality_report, build_analysis_frame, build_rule_masks
EXPERIMENT_ID: Final = 'usdrubf_ema_3_19_d1_technical_ml_benchmark_v1'
PRODUCER: Final = 'src.moex_research.runners.usdrubf_ema_3_19_d1_technical_ml_benchmark'
INSTRUMENT_ID: Final = 'usdrubf'
PRIMARY_TARGET: Final = 'h2_allow_trade'
PRIMARY_RETURN: Final = 'h2_signed_return'
FEATURE_GROUPS: Final[dict[str, dict[str, tuple[str, ...] | bool]]] = {'direction_only': {'numeric': (), 'categorical': ('cross_dir',), 'candidate': False}, 'momentum': {'numeric': ('dir_roc_10', 'dir_rsi_centered', 'dir_stoch_k_centered', 'dir_stoch_d_centered'), 'categorical': ('cross_dir',), 'candidate': True}, 'trend': {'numeric': ('adx_14', 'dir_di_spread', 'dir_macd_hist'), 'categorical': ('cross_dir',), 'candidate': True}, 'volatility': {'numeric': ('atr_14_pct', 'dir_bb_position', 'bb_bandwidth_20_2'), 'categorical': ('cross_dir',), 'candidate': True}, 'full_technical': {'numeric': ('dir_roc_10', 'dir_rsi_centered', 'dir_stoch_k_centered', 'dir_stoch_d_centered', 'adx_14', 'dir_di_spread', 'dir_macd_hist', 'atr_14_pct', 'dir_bb_position', 'bb_bandwidth_20_2'), 'categorical': ('cross_dir',), 'candidate': True}}
FEATURE_GROUP_NAMES: Final = tuple(FEATURE_GROUPS)
MODEL_CANDIDATES: Final = tuple((name for name, definition in FEATURE_GROUPS.items() if bool(definition['candidate'])))
MINIMUM_ELIGIBLE_EVENTS: Final = 50
MINIMUM_INITIAL_TRAIN_EVENTS: Final = 32
TEST_BLOCK_EVENTS: Final = 8
PURGE_D1_SESSIONS: Final = 3
MODEL_C: Final = 1.0
MODEL_RANDOM_STATE: Final = 319
CLASSIFICATION_THRESHOLD: Final = 0.5
CALIBRATION_BINS: Final = 5
PERMUTATION_SEED: Final = 319
PERMUTATION_REPETITIONS: Final = 1000
MINIMUM_OOS_EVENTS: Final = 24
MINIMUM_RETAINED_EVENTS: Final = 8
MINIMUM_RETENTION_RATE: Final = 0.2
MAXIMUM_RETENTION_RATE: Final = 0.6
MINIMUM_ROC_AUC: Final = 0.55
MAX_ADJUSTED_P_VALUE: Final = 0.1
MAX_POSITIVE_FOLD_CONTRIBUTION: Final = 0.7
OUTPUT_RUN_METADATA: Final = 'run_metadata.json'
OUTPUT_PREDICTIONS: Final = 'm4c_oos_predictions.csv'
OUTPUT_MODEL_METRICS: Final = 'm4c_model_metrics.csv'
OUTPUT_FIXED_RULE_METRICS: Final = 'm4c_fixed_rule_metrics.csv'
OUTPUT_FOLD_METRICS: Final = 'm4c_fold_metrics.csv'
OUTPUT_DIRECTION_METRICS: Final = 'm4c_direction_metrics.csv'
OUTPUT_COEFFICIENTS: Final = 'm4c_coefficients.csv'
OUTPUT_CALIBRATION: Final = 'm4c_calibration_bins.csv'
OUTPUT_PERMUTATION: Final = 'm4c_permutation_control.json'
OUTPUT_QUALITY: Final = 'm4c_quality_report.json'
OUTPUT_DECISION: Final = 'm4c_decision.json'
DECLARED_OUTPUT_FILES: Final = (OUTPUT_RUN_METADATA, OUTPUT_PREDICTIONS, OUTPUT_MODEL_METRICS, OUTPUT_FIXED_RULE_METRICS, OUTPUT_FOLD_METRICS, OUTPUT_DIRECTION_METRICS, OUTPUT_COEFFICIENTS, OUTPUT_CALIBRATION, OUTPUT_PERMUTATION, OUTPUT_QUALITY, OUTPUT_DECISION)
_ALIAS_TOKENS: Final = ('latest', 'current', 'autodetect')
_GLOB_CHARACTERS: Final = frozenset('*?[]')
_SHA_RE: Final = re.compile('^[0-9a-fA-F]{40}$')
_TOKEN_SPLIT_RE: Final = re.compile('[^A-Za-z0-9]+')

@dataclass(frozen=True)
class WalkForwardFold:
    fold: int
    train_positions: tuple[int, ...]
    test_positions: tuple[int, ...]
    first_test_session_index: int
    maximum_train_label_completion_index: int

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run the frozen USDRUBF D1 EMA(3/19) technical-feature logistic walk-forward benchmark.')
    parser.add_argument('--indicator-context-path', required=True)
    parser.add_argument('--labels-path', required=True)
    parser.add_argument('--quality-report-path', required=True)
    parser.add_argument('--m4b-decision-path', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--git-commit-sha', required=True)
    return parser

def _alias_tokens(path_value: str) -> list[str]:
    components = [part.lower() for part in re.split('[\\/]+', path_value) if part and part not in {'.', '..'}]
    stem_tokens = set(_TOKEN_SPLIT_RE.split(Path(components[-1]).stem)) if components else set()
    tokens = set(components) | {token.lower() for token in stem_tokens if token}
    return [token for token in _ALIAS_TOKENS if token in tokens]

def _validate_explicit_input(raw_value: str, argument_name: str, allowed_suffixes: set[str], parser: argparse.ArgumentParser) -> Path:
    raw_path = raw_value.strip()
    if not raw_path:
        parser.error(f'{argument_name} must be non-empty')
    if any((character in raw_path for character in _GLOB_CHARACTERS)):
        parser.error(f'{argument_name} must reference one explicit file and must not contain glob syntax')
    candidates = [raw_path]
    try:
        resolved = str(Path(raw_path).expanduser().resolve(strict=False))
    except (OSError, RuntimeError):
        resolved = str(Path(raw_path).expanduser().absolute())
    if resolved != raw_path:
        candidates.append(resolved)
    aliases = sorted({token for candidate in candidates for token in _alias_tokens(candidate)})
    if aliases:
        parser.error(f'{argument_name} must not use mutable alias token(s): ' + ', '.join(aliases))
    path = Path(raw_path)
    if not path.exists():
        parser.error(f'{argument_name} must reference an existing file')
    if not path.is_file():
        parser.error(f'{argument_name} must reference a file')
    if path.suffix.lower() not in allowed_suffixes:
        parser.error(f'{argument_name} must end with ' + ' or '.join(sorted(allowed_suffixes)))
    return path

def _validate_cli_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> tuple[Path, Path, Path, Path, Path, str, str]:
    context_path = _validate_explicit_input(str(args.indicator_context_path), '--indicator-context-path', {'.csv', '.parquet'}, parser)
    labels_path = _validate_explicit_input(str(args.labels_path), '--labels-path', {'.csv', '.parquet'}, parser)
    quality_path = _validate_explicit_input(str(args.quality_report_path), '--quality-report-path', {'.json'}, parser)
    m4b_decision_path = _validate_explicit_input(str(args.m4b_decision_path), '--m4b-decision-path', {'.json'}, parser)
    if len({context_path.resolve(), labels_path.resolve(), quality_path.resolve(), m4b_decision_path.resolve()}) != 4:
        parser.error('the four input artifact paths must be distinct')
    output_raw = str(args.output_dir).strip()
    if not output_raw:
        parser.error('--output-dir must be non-empty')
    output_dir = Path(output_raw)
    if output_dir.exists() and (not output_dir.is_dir()):
        parser.error('--output-dir must reference a directory')
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error('--output-dir must be absent or empty to prevent stale evidence')
    run_id = str(args.run_id).strip()
    if not run_id:
        parser.error('--run-id must be non-empty')
    git_sha = str(args.git_commit_sha).strip().lower()
    if not _SHA_RE.fullmatch(git_sha):
        parser.error('--git-commit-sha must be an explicit 40-character hexadecimal commit SHA')
    return (context_path, labels_path, quality_path, m4b_decision_path, output_dir, run_id, git_sha)

def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def _read_table(path: Path, artifact_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path) if path.suffix.lower() == '.csv' else pd.read_parquet(path)
    except Exception as exc:
        raise ValueError(f'failed to read {artifact_name}: {exc}') from exc

def _read_json(path: Path, artifact_name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f'failed to read {artifact_name}: {exc}') from exc
    if not isinstance(payload, dict):
        raise ValueError(f'{artifact_name} must contain a JSON object')
    return payload

def _json_safe(value: Any) -> Any:
    if value is pd.NA:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value

def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')

def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False, lineterminator='\n', float_format='%.12g')

def _output_paths(output_dir: Path) -> dict[str, Path]:
    root = output_dir.resolve()
    paths = {filename: root / filename for filename in DECLARED_OUTPUT_FILES}
    if any((path.resolve().parent != root for path in paths.values())):
        raise ValueError('declared output path escaped output directory')
    return paths

def _validate_m4b_decision(payload: Mapping[str, Any]) -> None:
    if payload.get('experiment_id') != M4B_EXPERIMENT_ID:
        raise ValueError(f'M4B decision experiment_id must equal {M4B_EXPERIMENT_ID!r}')
    if payload.get('result_status') != 'rule_gate_not_supported':
        raise ValueError('M4C requires the canonical M4B result rule_gate_not_supported')
    if payload.get('selected_rule') is not None:
        raise ValueError('M4C requires M4B selected_rule to be null')
    for field in ('model_training_performed', 'threshold_sweep_performed', 'post_hoc_rule_search_performed', 'runtime_or_trading_action_performed', 'strategy_promotion_allowed'):
        if payload.get(field) is not False:
            raise ValueError(f'M4B decision must state {field}=false')

def prepare_analysis_frame(indicator_context: pd.DataFrame, labels: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = build_analysis_frame(indicator_context, labels)
    frame = frame.copy()
    frame['dir_stoch_k_centered'] = frame['direction'] * (frame['stoch_k_14'] - 50.0)
    frame['dir_stoch_d_centered'] = frame['direction'] * (frame['stoch_d_3'] - 50.0)
    frame['_source_index'] = np.arange(len(frame), dtype=int)
    eligible = frame.loc[frame['indicator_ready'].astype(bool) & frame[PRIMARY_RETURN].notna()].copy()
    target = pd.to_numeric(eligible[PRIMARY_TARGET], errors='coerce')
    if target.isna().any() or not target.isin([0, 1]).all():
        raise ValueError('observed H2 rows require h2_allow_trade values 0 or 1')
    expected = pd.to_numeric(eligible[PRIMARY_RETURN], errors='coerce').gt(0.0).astype(int)
    if not target.astype(int).equals(expected):
        raise ValueError('h2_allow_trade must equal int(h2_signed_return > 0)')
    eligible['target_value'] = target.astype(int)
    if len(eligible) < MINIMUM_ELIGIBLE_EVENTS:
        raise ValueError(f'eligible H2 indicator-ready event count {len(eligible)} is below {MINIMUM_ELIGIBLE_EVENTS}')
    if eligible['target_value'].nunique() < 2:
        raise ValueError('eligible H2 target contains only one class')
    required_features = {column for definition in FEATURE_GROUPS.values() for key in ('numeric', 'categorical') for column in definition[key]}
    missing = sorted(required_features - set(eligible.columns))
    if missing:
        raise ValueError('analysis frame is missing model feature columns: ' + ', '.join(missing))
    return (frame.reset_index(drop=True), eligible.reset_index(drop=True))

def build_walk_forward_folds(frame: pd.DataFrame) -> list[WalkForwardFold]:
    if len(frame) <= MINIMUM_INITIAL_TRAIN_EVENTS:
        raise ValueError('eligible target does not contain enough rows for walk-forward evaluation')
    initial_test_position = MINIMUM_INITIAL_TRAIN_EVENTS
    while initial_test_position < len(frame):
        first_test_session = int(frame.iloc[initial_test_position]['event_session_index'])
        train_positions = tuple((position for position in range(initial_test_position) if int(frame.iloc[position]['event_session_index']) + PURGE_D1_SESSIONS < first_test_session))
        classes = frame.iloc[list(train_positions)]['target_value'].nunique() if train_positions else 0
        if len(train_positions) >= MINIMUM_INITIAL_TRAIN_EVENTS and classes == 2:
            break
        initial_test_position += 1
    else:
        raise ValueError('no valid initial walk-forward fold remains after the H2 purge')
    folds: list[WalkForwardFold] = []
    fold_number = 1
    test_start = initial_test_position
    while test_start < len(frame):
        test_stop = min(test_start + TEST_BLOCK_EVENTS, len(frame))
        first_test_session = int(frame.iloc[test_start]['event_session_index'])
        train_positions = tuple((position for position in range(test_start) if int(frame.iloc[position]['event_session_index']) + PURGE_D1_SESSIONS < first_test_session))
        if len(train_positions) < MINIMUM_INITIAL_TRAIN_EVENTS:
            raise ValueError('walk-forward fold violates minimum training count after purge')
        train = frame.iloc[list(train_positions)]
        if train['target_value'].nunique() < 2:
            raise ValueError('walk-forward training fold contains only one target class')
        maximum_completion = int((train['event_session_index'].astype(int) + PURGE_D1_SESSIONS).max())
        if maximum_completion >= first_test_session:
            raise ValueError('walk-forward purge invariant failed')
        folds.append(WalkForwardFold(fold=fold_number, train_positions=train_positions, test_positions=tuple(range(test_start, test_stop)), first_test_session_index=first_test_session, maximum_train_label_completion_index=maximum_completion))
        fold_number += 1
        test_start = test_stop
    if not folds:
        raise ValueError('walk-forward protocol produced zero OOS folds')
    return folds

def _feature_columns(group_name: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if group_name not in FEATURE_GROUPS:
        raise ValueError(f'unknown feature group: {group_name}')
    definition = FEATURE_GROUPS[group_name]
    return (tuple(definition['numeric']), tuple(definition['categorical']))

def build_model_pipeline(group_name: str) -> Pipeline:
    numeric, categorical = _feature_columns(group_name)
    transformers: list[tuple[str, Any, list[str]]] = []
    if numeric:
        transformers.append(('numeric', Pipeline(steps=[('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())]), list(numeric)))
    if categorical:
        transformers.append(('categorical', OneHotEncoder(categories=[['cross_down', 'cross_up']], handle_unknown='error'), list(categorical)))
    preprocessor = ColumnTransformer(transformers=transformers, remainder='drop')
    model = LogisticRegression(C=MODEL_C, class_weight='balanced', solver='liblinear', max_iter=1000, random_state=MODEL_RANDOM_STATE)
    return Pipeline(steps=[('preprocessor', preprocessor), ('model', model)])

def _positive_probability(pipeline: Pipeline, frame: pd.DataFrame, group_name: str) -> np.ndarray:
    numeric, categorical = _feature_columns(group_name)
    probabilities = pipeline.predict_proba(frame[list(numeric + categorical)])
    classes = list(pipeline.named_steps['model'].classes_)
    if 1 not in classes:
        raise ValueError('fitted model does not expose positive class')
    return probabilities[:, classes.index(1)]

def _fit_predict_fold(train: pd.DataFrame, test: pd.DataFrame, group_name: str, *, target_override: np.ndarray | None=None) -> tuple[Pipeline, np.ndarray]:
    numeric, categorical = _feature_columns(group_name)
    columns = list(numeric + categorical)
    target = train['target_value'].astype(int).to_numpy() if target_override is None else np.asarray(target_override, dtype=int)
    if len(target) != len(train) or np.unique(target).size < 2:
        raise ValueError('training target override must preserve two classes and row count')
    pipeline = build_model_pipeline(group_name)
    pipeline.fit(train[columns], target)
    return (pipeline, _positive_probability(pipeline, test, group_name))

def _coefficient_rows(pipeline: Pipeline, *, group_name: str, fold: int) -> list[dict[str, Any]]:
    preprocessor = pipeline.named_steps['preprocessor']
    model = pipeline.named_steps['model']
    names = [str(name) for name in preprocessor.get_feature_names_out()]
    coefficients = model.coef_[0]
    if len(names) != len(coefficients):
        raise ValueError('transformed feature count does not match coefficient count')
    rows = [{'experiment_id': EXPERIMENT_ID, 'feature_group': group_name, 'fold': int(fold), 'feature': feature, 'coefficient': float(value), 'coefficient_scope': 'fold_training_only'} for feature, value in zip(names, coefficients, strict=True)]
    rows.append({'experiment_id': EXPERIMENT_ID, 'feature_group': group_name, 'fold': int(fold), 'feature': '__intercept__', 'coefficient': float(model.intercept_[0]), 'coefficient_scope': 'fold_training_only'})
    return rows

def _probability_metrics(predictions: pd.DataFrame) -> dict[str, Any]:
    if predictions.empty:
        return {'oos_events': 0, 'positive_rate': None, 'roc_auc': None, 'pr_auc': None, 'brier_score': None, 'fold_prevalence_baseline_brier': None, 'brier_improvement_vs_fold_prevalence': None, 'accuracy': None, 'balanced_accuracy': None, 'precision': None, 'recall': None, 'f1': None, 'ece_5_bins': None}
    y = predictions['target_value'].astype(int).to_numpy()
    probability = predictions['probability'].astype(float).to_numpy()
    predicted = (probability >= CLASSIFICATION_THRESHOLD).astype(int)
    baseline_probability = predictions['baseline_probability'].astype(float).to_numpy()
    single_class = np.unique(y).size < 2
    model_brier = float(brier_score_loss(y, probability))
    baseline_brier = float(brier_score_loss(y, baseline_probability))
    return {'oos_events': int(len(predictions)), 'positive_rate': float(np.mean(y)), 'roc_auc': None if single_class else float(roc_auc_score(y, probability)), 'pr_auc': None if single_class else float(average_precision_score(y, probability)), 'brier_score': model_brier, 'fold_prevalence_baseline_brier': baseline_brier, 'brier_improvement_vs_fold_prevalence': baseline_brier - model_brier, 'accuracy': float(accuracy_score(y, predicted)), 'balanced_accuracy': None if single_class else float(balanced_accuracy_score(y, predicted)), 'precision': float(precision_score(y, predicted, zero_division=0)), 'recall': float(recall_score(y, predicted, zero_division=0)), 'f1': float(f1_score(y, predicted, zero_division=0)), 'ece_5_bins': _expected_calibration_error(y, probability, CALIBRATION_BINS)}

def _expected_calibration_error(y_true: np.ndarray, probability: np.ndarray, bins: int) -> float:
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = len(y_true)
    if total == 0:
        return float('nan')
    value = 0.0
    for index in range(bins):
        lower = edges[index]
        upper = edges[index + 1]
        mask = (probability >= lower) & (probability <= upper) if index == bins - 1 else (probability >= lower) & (probability < upper)
        if not mask.any():
            continue
        value += float(mask.mean()) * abs(float(probability[mask].mean()) - float(y_true[mask].mean()))
    return float(value)

def build_calibration_bins(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    edges = np.linspace(0.0, 1.0, CALIBRATION_BINS + 1)
    for group_name in FEATURE_GROUP_NAMES:
        group = predictions.loc[predictions['feature_group'].eq(group_name)]
        probability = group['probability'].astype(float).to_numpy()
        y = group['target_value'].astype(int).to_numpy()
        for index in range(CALIBRATION_BINS):
            lower = edges[index]
            upper = edges[index + 1]
            mask = (probability >= lower) & (probability <= upper) if index == CALIBRATION_BINS - 1 else (probability >= lower) & (probability < upper)
            count = int(mask.sum())
            mean_probability = None if count == 0 else float(probability[mask].mean())
            observed_rate = None if count == 0 else float(y[mask].mean())
            rows.append({'experiment_id': EXPERIMENT_ID, 'feature_group': group_name, 'bin_index': index, 'lower_bound': float(lower), 'upper_bound': float(upper), 'event_count': count, 'mean_probability': mean_probability, 'observed_positive_rate': observed_rate, 'absolute_calibration_gap': None if mean_probability is None or observed_rate is None else abs(mean_probability - observed_rate)})
    return pd.DataFrame(rows)

def _policy_metrics(returns: pd.Series, accepted: pd.Series | np.ndarray) -> dict[str, Any]:
    values = pd.to_numeric(returns, errors='coerce').to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError('policy metric input requires observed finite H2 returns')
    mask = np.asarray(accepted, dtype=bool)
    if len(mask) != len(values):
        raise ValueError('policy mask length does not match return count')
    retained = values[mask]
    return {'accepted_events': int(mask.sum()), 'acceptance_rate': float(mask.mean()) if len(mask) else None, 'retained_h2_mean_signed_return': None if len(retained) == 0 else float(np.mean(retained)), 'retained_h2_median_signed_return': None if len(retained) == 0 else float(np.median(retained)), 'retained_h2_win_rate': None if len(retained) == 0 else float(np.mean(retained > 0.0)), 'policy_h2_mean_return_per_signal': float(np.mean(values * mask.astype(float)))}

def evaluate_models(eligible: pd.DataFrame, folds: Sequence[WalkForwardFold]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    prediction_rows: list[dict[str, Any]] = []
    fold_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    for group_name in FEATURE_GROUP_NAMES:
        for fold in folds:
            train = eligible.iloc[list(fold.train_positions)].copy()
            test = eligible.iloc[list(fold.test_positions)].copy()
            pipeline, probability = _fit_predict_fold(train, test, group_name)
            prevalence = float(train['target_value'].mean())
            majority_class = int(prevalence >= 0.5)
            for offset, (_, event) in enumerate(test.iterrows()):
                prediction_rows.append({'experiment_id': EXPERIMENT_ID, 'instrument_id': str(event['instrument_id']), 'end': pd.Timestamp(event['end']).isoformat(), 'cross_dir': str(event['cross_dir']), 'event_session_index': int(event['event_session_index']), 'source_index': int(event['_source_index']), 'feature_group': group_name, 'fold': int(fold.fold), 'target_value': int(event['target_value']), 'h2_signed_return': float(event[PRIMARY_RETURN]), 'probability': float(probability[offset]), 'predicted_class': int(probability[offset] >= CLASSIFICATION_THRESHOLD), 'baseline_probability': prevalence, 'baseline_class': majority_class})
            fold_predictions = pd.DataFrame(prediction_rows[-len(test):])
            probability_metrics = _probability_metrics(fold_predictions)
            policy = _policy_metrics(fold_predictions['h2_signed_return'], fold_predictions['probability'].ge(CLASSIFICATION_THRESHOLD))
            no_gate = _policy_metrics(fold_predictions['h2_signed_return'], np.ones(len(fold_predictions), dtype=bool))
            fold_rows.append({'experiment_id': EXPERIMENT_ID, 'feature_group': group_name, 'fold': int(fold.fold), 'train_events': int(len(train)), 'test_events': int(len(test)), 'train_start': pd.Timestamp(train.iloc[0]['end']).isoformat(), 'train_end': pd.Timestamp(train.iloc[-1]['end']).isoformat(), 'test_start': pd.Timestamp(test.iloc[0]['end']).isoformat(), 'test_end': pd.Timestamp(test.iloc[-1]['end']).isoformat(), 'first_test_session_index': fold.first_test_session_index, 'maximum_train_label_completion_index': fold.maximum_train_label_completion_index, 'purge_d1_sessions': PURGE_D1_SESSIONS, 'train_prevalence': prevalence, **probability_metrics, **policy, 'no_gate_h2_mean_return_per_signal': no_gate['policy_h2_mean_return_per_signal'], 'policy_h2_uplift_vs_no_gate': policy['policy_h2_mean_return_per_signal'] - no_gate['policy_h2_mean_return_per_signal']})
            coefficient_rows.extend(_coefficient_rows(pipeline, group_name=group_name, fold=fold.fold))
    return (pd.DataFrame(prediction_rows), pd.DataFrame(fold_rows), pd.DataFrame(coefficient_rows))

def _oos_reference(predictions: pd.DataFrame) -> pd.DataFrame:
    direction = predictions.loc[predictions['feature_group'].eq('direction_only')].copy()
    if direction.empty:
        raise ValueError('direction_only OOS predictions are missing')
    key_columns = ['instrument_id', 'end', 'cross_dir', 'event_session_index', 'source_index', 'fold', 'target_value', 'h2_signed_return']
    reference = direction[key_columns].sort_values(['fold', 'event_session_index']).reset_index(drop=True)
    for group_name in FEATURE_GROUP_NAMES:
        group = predictions.loc[predictions['feature_group'].eq(group_name), key_columns].sort_values(['fold', 'event_session_index']).reset_index(drop=True)
        if not reference.equals(group):
            raise ValueError('all feature groups must use the identical OOS event set and folds')
    return reference

def build_fixed_rule_metrics(full_frame: pd.DataFrame, oos_reference: pd.DataFrame) -> pd.DataFrame:
    masks = build_rule_masks(full_frame)
    source_indices = oos_reference['source_index'].astype(int).to_numpy()
    returns = oos_reference['h2_signed_return']
    rows: list[dict[str, Any]] = []
    for rule_name in FIXED_RULE_NAMES:
        accepted = masks[rule_name].iloc[source_indices].to_numpy(dtype=bool)
        metrics = _policy_metrics(returns, accepted)
        rows.append({'experiment_id': EXPERIMENT_ID, 'rule_name': rule_name, 'is_candidate_rule': rule_name != 'no_gate', **metrics})
    return pd.DataFrame(rows)

def build_model_metrics(predictions: pd.DataFrame, fold_metrics: pd.DataFrame, fixed_rule_metrics: pd.DataFrame) -> pd.DataFrame:
    reference = _oos_reference(predictions)
    no_gate = _policy_metrics(reference['h2_signed_return'], np.ones(len(reference), dtype=bool))
    rows: list[dict[str, Any]] = []
    for group_name in FEATURE_GROUP_NAMES:
        group = predictions.loc[predictions['feature_group'].eq(group_name)].copy()
        probability = _probability_metrics(group)
        policy = _policy_metrics(group['h2_signed_return'], group['probability'].ge(CLASSIFICATION_THRESHOLD))
        group_folds = fold_metrics.loc[fold_metrics['feature_group'].eq(group_name)].copy()
        positive = pd.to_numeric(group_folds['policy_h2_uplift_vs_no_gate'], errors='coerce').clip(lower=0.0) * pd.to_numeric(group_folds['test_events'], errors='coerce')
        positive_total = float(positive.sum())
        maximum_share = None if positive_total <= 0.0 else float(positive.max() / positive_total)
        rows.append({'experiment_id': EXPERIMENT_ID, 'feature_group': group_name, 'is_candidate_model': group_name in MODEL_CANDIDATES, **probability, **policy, 'no_gate_h2_mean_signed_return': no_gate['retained_h2_mean_signed_return'], 'no_gate_h2_median_signed_return': no_gate['retained_h2_median_signed_return'], 'no_gate_h2_win_rate': no_gate['retained_h2_win_rate'], 'no_gate_h2_mean_return_per_signal': no_gate['policy_h2_mean_return_per_signal'], 'retained_h2_mean_uplift_vs_no_gate': None if policy['retained_h2_mean_signed_return'] is None else policy['retained_h2_mean_signed_return'] - no_gate['retained_h2_mean_signed_return'], 'retained_h2_median_uplift_vs_no_gate': None if policy['retained_h2_median_signed_return'] is None else policy['retained_h2_median_signed_return'] - no_gate['retained_h2_median_signed_return'], 'retained_h2_win_rate_uplift_vs_no_gate': None if policy['retained_h2_win_rate'] is None else policy['retained_h2_win_rate'] - no_gate['retained_h2_win_rate'], 'policy_h2_uplift_vs_no_gate': policy['policy_h2_mean_return_per_signal'] - no_gate['policy_h2_mean_return_per_signal'], 'positive_policy_uplift_folds': int((group_folds['policy_h2_uplift_vs_no_gate'] > 0.0).sum()), 'maximum_positive_fold_contribution_share': maximum_share})
    metrics = pd.DataFrame(rows)
    best_fixed = fixed_rule_metrics.loc[fixed_rule_metrics['is_candidate_rule'].astype(bool)].sort_values(['policy_h2_mean_return_per_signal', 'rule_name'], ascending=[False, True]).iloc[0]
    metrics['best_fixed_rule'] = str(best_fixed['rule_name'])
    metrics['best_fixed_policy_h2_mean_return_per_signal'] = float(best_fixed['policy_h2_mean_return_per_signal'])
    return metrics

def build_direction_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_name in FEATURE_GROUP_NAMES:
        for cross_dir in ('cross_up', 'cross_down'):
            subset = predictions.loc[predictions['feature_group'].eq(group_name) & predictions['cross_dir'].eq(cross_dir)].copy()
            rows.append({'experiment_id': EXPERIMENT_ID, 'feature_group': group_name, 'cross_dir': cross_dir, **_probability_metrics(subset), **_policy_metrics(subset['h2_signed_return'], subset['probability'].ge(CLASSIFICATION_THRESHOLD))})
    return pd.DataFrame(rows)

def _null_summary(values: np.ndarray) -> dict[str, Any]:
    finite = values[np.isfinite(values)]
    if len(finite) == 0:
        return {'valid_repetitions': 0, 'mean': None, 'std': None, 'q90': None, 'q95': None, 'q99': None}
    return {'valid_repetitions': int(len(finite)), 'mean': float(np.mean(finite)), 'std': None if len(finite) < 2 else float(np.std(finite, ddof=1)), 'q90': float(np.quantile(finite, 0.9)), 'q95': float(np.quantile(finite, 0.95)), 'q99': float(np.quantile(finite, 0.99))}

def _p_value(null: np.ndarray, observed: float | None) -> float | None:
    if observed is None or not np.isfinite(observed):
        return None
    finite = null[np.isfinite(null)]
    if len(finite) == 0:
        return None
    return float((1 + np.count_nonzero(finite >= observed)) / (len(finite) + 1))

def compute_permutation_control(eligible: pd.DataFrame, folds: Sequence[WalkForwardFold], observed_model_metrics: pd.DataFrame, *, seed: int=PERMUTATION_SEED, repetitions: int | None=None) -> dict[str, Any]:
    repetitions = PERMUTATION_REPETITIONS if repetitions is None else int(repetitions)
    if repetitions <= 0:
        raise ValueError('permutation repetitions must be positive')
    oos_positions = [position for fold in folds for position in fold.test_positions]
    oos = eligible.iloc[oos_positions].copy()
    y_oos = oos['target_value'].astype(int).to_numpy()
    h2_oos = pd.to_numeric(oos[PRIMARY_RETURN], errors='coerce').to_numpy(dtype=float)
    if np.unique(y_oos).size < 2:
        raise ValueError('permutation control requires two classes in aggregate OOS labels')
    no_gate_mean = float(np.mean(h2_oos))
    rng = np.random.default_rng(seed)
    auc_null = {group_name: np.full(repetitions, np.nan, dtype=float) for group_name in MODEL_CANDIDATES}
    policy_null = {group_name: np.full(repetitions, np.nan, dtype=float) for group_name in MODEL_CANDIDATES}
    max_auc = np.full(repetitions, np.nan, dtype=float)
    max_policy = np.full(repetitions, np.nan, dtype=float)
    for repetition in range(repetitions):
        auc_values: list[float] = []
        policy_values: list[float] = []
        for group_name in MODEL_CANDIDATES:
            probabilities: list[np.ndarray] = []
            for fold in folds:
                train = eligible.iloc[list(fold.train_positions)].copy()
                test = eligible.iloc[list(fold.test_positions)].copy()
                shuffled = rng.permutation(train['target_value'].astype(int).to_numpy())
                _, fold_probability = _fit_predict_fold(train, test, group_name, target_override=shuffled)
                probabilities.append(fold_probability)
            probability = np.concatenate(probabilities)
            auc_uplift = float(roc_auc_score(y_oos, probability) - 0.5)
            policy_uplift = float(np.mean(h2_oos * (probability >= CLASSIFICATION_THRESHOLD).astype(float)) - no_gate_mean)
            auc_null[group_name][repetition] = auc_uplift
            policy_null[group_name][repetition] = policy_uplift
            auc_values.append(auc_uplift)
            policy_values.append(policy_uplift)
        max_auc[repetition] = max(auc_values)
        max_policy[repetition] = max(policy_values)
    candidates: dict[str, Any] = {}
    for group_name in MODEL_CANDIDATES:
        row = observed_model_metrics.loc[observed_model_metrics['feature_group'].eq(group_name)]
        if len(row) != 1:
            raise ValueError(f'expected one observed model metric row for {group_name}')
        observed_auc = row.iloc[0]['roc_auc']
        observed_auc_uplift = None if pd.isna(observed_auc) else float(observed_auc - 0.5)
        observed_policy_uplift = float(row.iloc[0]['policy_h2_uplift_vs_no_gate'])
        candidates[group_name] = {'observed_roc_auc_uplift_vs_0_5': observed_auc_uplift, 'observed_policy_h2_uplift_vs_no_gate': observed_policy_uplift, 'roc_auc_uplift': {'unadjusted_p_value': _p_value(auc_null[group_name], observed_auc_uplift), 'max_stat_adjusted_p_value': _p_value(max_auc, observed_auc_uplift), 'null_summary': _null_summary(auc_null[group_name])}, 'policy_h2_uplift': {'unadjusted_p_value': _p_value(policy_null[group_name], observed_policy_uplift), 'max_stat_adjusted_p_value': _p_value(max_policy, observed_policy_uplift), 'null_summary': _null_summary(policy_null[group_name])}}
    return {'experiment_id': EXPERIMENT_ID, 'seed': int(seed), 'repetitions': int(repetitions), 'null': 'training labels permuted independently inside each walk-forward fold', 'oos_labels_unchanged': True, 'candidate_groups': list(MODEL_CANDIDATES), 'multiple_comparison_adjustment': 'one-sided max-statistic across the four candidate technical feature groups', 'max_statistic_null': {'roc_auc_uplift_vs_0_5': _null_summary(max_auc), 'policy_h2_uplift_vs_no_gate': _null_summary(max_policy)}, 'candidates': candidates}

def build_decision(model_metrics: pd.DataFrame, fixed_rule_metrics: pd.DataFrame, permutation: Mapping[str, Any], *, run_id: str, git_commit_sha: str) -> dict[str, Any]:
    direction_row = model_metrics.loc[model_metrics['feature_group'].eq('direction_only')].iloc[0]
    best_fixed = fixed_rule_metrics.loc[fixed_rule_metrics['is_candidate_rule'].astype(bool)].sort_values(['policy_h2_mean_return_per_signal', 'rule_name'], ascending=[False, True]).iloc[0]

    def greater(left: Any, right: Any) -> bool:
        if left is None or right is None or pd.isna(left) or pd.isna(right):
            return False
        return bool(float(left) > float(right))
    candidate_decisions: dict[str, Any] = {}
    supported_groups: list[str] = []
    for group_name in MODEL_CANDIDATES:
        row = model_metrics.loc[model_metrics['feature_group'].eq(group_name)].iloc[0]
        control = permutation['candidates'][group_name]
        auc_p = control['roc_auc_uplift']['max_stat_adjusted_p_value']
        policy_p = control['policy_h2_uplift']['max_stat_adjusted_p_value']
        conditions = {'minimum_oos_events': int(row['oos_events']) >= MINIMUM_OOS_EVENTS, 'minimum_retained_events': int(row['accepted_events']) >= MINIMUM_RETAINED_EVENTS, 'retention_rate_within_limits': bool(MINIMUM_RETENTION_RATE <= float(row['acceptance_rate']) <= MAXIMUM_RETENTION_RATE), 'roc_auc_gt_0_55': bool(not pd.isna(row['roc_auc']) and float(row['roc_auc']) > MINIMUM_ROC_AUC), 'brier_better_than_fold_prevalence': bool(float(row['brier_score']) < float(row['fold_prevalence_baseline_brier'])), 'brier_better_than_direction_only': bool(float(row['brier_score']) < float(direction_row['brier_score'])), 'retained_h2_mean_gt_no_gate': greater(row['retained_h2_mean_signed_return'], row['no_gate_h2_mean_signed_return']), 'retained_h2_median_gt_no_gate': greater(row['retained_h2_median_signed_return'], row['no_gate_h2_median_signed_return']), 'retained_h2_win_rate_gt_no_gate': greater(row['retained_h2_win_rate'], row['no_gate_h2_win_rate']), 'policy_h2_mean_gt_no_gate': bool(row['policy_h2_mean_return_per_signal'] > row['no_gate_h2_mean_return_per_signal']), 'policy_h2_mean_gt_best_fixed_rule': bool(row['policy_h2_mean_return_per_signal'] > best_fixed['policy_h2_mean_return_per_signal']), 'policy_h2_mean_gt_direction_only': bool(row['policy_h2_mean_return_per_signal'] > direction_row['policy_h2_mean_return_per_signal']), 'positive_policy_uplift_in_at_least_two_folds': int(row['positive_policy_uplift_folds']) >= 2, 'no_single_fold_over_70pct_positive_uplift': bool(not pd.isna(row['maximum_positive_fold_contribution_share']) and float(row['maximum_positive_fold_contribution_share']) <= MAX_POSITIVE_FOLD_CONTRIBUTION), 'max_stat_adjusted_auc_p_le_0_10': bool(auc_p is not None and float(auc_p) <= MAX_ADJUSTED_P_VALUE), 'max_stat_adjusted_policy_p_le_0_10': bool(policy_p is not None and float(policy_p) <= MAX_ADJUSTED_P_VALUE)}
        supported = all(conditions.values())
        if supported:
            supported_groups.append(group_name)
        candidate_decisions[group_name] = {'supported': supported, 'conditions': conditions, 'oos_events': int(row['oos_events']), 'retained_events': int(row['accepted_events']), 'retention_rate': float(row['acceptance_rate']), 'roc_auc': None if pd.isna(row['roc_auc']) else float(row['roc_auc']), 'brier_score': float(row['brier_score']), 'policy_h2_mean_return_per_signal': float(row['policy_h2_mean_return_per_signal']), 'max_stat_adjusted_auc_p_value': auc_p, 'max_stat_adjusted_policy_p_value': policy_p}
    if supported_groups:
        selected_rows = model_metrics.loc[model_metrics['feature_group'].isin(supported_groups)].sort_values(['policy_h2_mean_return_per_signal', 'brier_score', 'feature_group'], ascending=[False, True, True])
        selected_group = str(selected_rows.iloc[0]['feature_group'])
        result_status = 'technical_ml_supported_for_next_research_phase'
        selection_basis = 'highest OOS policy mean, then lowest Brier, among fully supported groups'
    else:
        selected_group = None
        result_status = 'technical_ml_not_supported'
        selection_basis = None
    return {'experiment_id': EXPERIMENT_ID, 'run_id': run_id, 'git_commit_sha': git_commit_sha, 'result_status': result_status, 'fallback_result': 'technical_ml_not_supported', 'selected_feature_group': selected_group, 'selection_basis': selection_basis, 'primary_target': PRIMARY_TARGET, 'classification_threshold': CLASSIFICATION_THRESHOLD, 'best_fixed_rule_on_oos_policy_metric': {'rule_name': str(best_fixed['rule_name']), 'accepted_events': int(best_fixed['accepted_events']), 'acceptance_rate': float(best_fixed['acceptance_rate']), 'policy_h2_mean_return_per_signal': float(best_fixed['policy_h2_mean_return_per_signal'])}, 'direction_only_negative_control': {'roc_auc': None if pd.isna(direction_row['roc_auc']) else float(direction_row['roc_auc']), 'brier_score': float(direction_row['brier_score']), 'policy_h2_mean_return_per_signal': float(direction_row['policy_h2_mean_return_per_signal'])}, 'candidate_decisions': candidate_decisions, 'persistent_model_artifact_emitted': False, 'hyperparameter_search_performed': False, 'threshold_tuning_performed': False, 'post_hoc_feature_selection_performed': False, 'model_promotion_allowed': False, 'strategy_promotion_allowed': False, 'runtime_or_trading_action_performed': False}

def _input_records(paths: Mapping[str, Path]) -> dict[str, dict[str, str]]:
    return {name: {'path': str(path), 'sha256': _sha256_file(path)} for name, path in paths.items()}

def run_research_package(*, indicator_context_path: Path, labels_path: Path, quality_report_path: Path, m4b_decision_path: Path, output_dir: Path, run_id: str, git_commit_sha: str) -> dict[str, Any]:
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise ValueError('output_dir must be absent or empty to prevent stale evidence')
    started_at = _utc_now_iso()
    paths = {'indicator_context': indicator_context_path, 'labels': labels_path, 'quality_report': quality_report_path, 'm4b_decision': m4b_decision_path}
    inputs = _input_records(paths)
    context = _read_table(indicator_context_path, 'M4A indicator context')
    labels = _read_table(labels_path, 'M4A multi-horizon labels')
    source_quality = _read_json(quality_report_path, 'M4A quality report')
    m4b_decision = _read_json(m4b_decision_path, 'M4B decision')
    full_frame, eligible = prepare_analysis_frame(context, labels)
    _validate_m4a_quality_report(source_quality, full_frame)
    _validate_m4b_decision(m4b_decision)
    folds = build_walk_forward_folds(eligible)
    predictions, fold_metrics, coefficients = evaluate_models(eligible, folds)
    oos_reference = _oos_reference(predictions)
    fixed_rule_metrics = build_fixed_rule_metrics(full_frame, oos_reference)
    model_metrics = build_model_metrics(predictions, fold_metrics, fixed_rule_metrics)
    direction_metrics = build_direction_metrics(predictions)
    calibration = build_calibration_bins(predictions)
    permutation = compute_permutation_control(eligible, folds, model_metrics)
    decision = build_decision(model_metrics, fixed_rule_metrics, permutation, run_id=run_id, git_commit_sha=git_commit_sha)
    metadata = {'experiment_id': EXPERIMENT_ID, 'producer': PRODUCER, 'run_id': run_id, 'git_commit_sha': git_commit_sha, 'started_at': started_at, 'completed_at': _utc_now_iso(), 'instrument_id': INSTRUMENT_ID, 'source_experiment_id': M4A_EXPERIMENT_ID, 'm4b_experiment_id': M4B_EXPERIMENT_ID, 'inputs': inputs, 'feature_groups': FEATURE_GROUPS, 'model': {'estimator': 'sklearn.linear_model.LogisticRegression', 'C': MODEL_C, 'class_weight': 'balanced', 'solver': 'liblinear', 'random_state': MODEL_RANDOM_STATE, 'probability_mode': 'native logistic probability', 'post_hoc_calibrator': None}, 'validation_protocol': {'mode': 'expanding_walk_forward', 'shuffle': False, 'minimum_initial_train_events_after_purge': MINIMUM_INITIAL_TRAIN_EVENTS, 'test_block_events': TEST_BLOCK_EVENTS, 'purge_d1_sessions': PURGE_D1_SESSIONS, 'classification_threshold': CLASSIFICATION_THRESHOLD, 'threshold_tuning': False, 'hyperparameter_search': False, 'preprocessing_fit_scope': 'training fold only'}, 'negative_controls': {'direction_only_feature_group': True, 'training_label_permutations': PERMUTATION_REPETITIONS, 'permutation_seed': PERMUTATION_SEED, 'max_statistic_adjustment': True}, 'outputs': list(DECLARED_OUTPUT_FILES), 'result_status': decision['result_status'], 'persistent_model_artifact_emitted': False}
    quality = {'experiment_id': EXPERIMENT_ID, 'run_id': run_id, 'git_commit_sha': git_commit_sha, 'input_artifacts': inputs, 'counts': {'full_event_rows': int(len(full_frame)), 'indicator_ready_h2_eligible_rows': int(len(eligible)), 'oos_events': int(len(oos_reference)), 'walk_forward_folds': int(len(folds)), 'prediction_rows': int(len(predictions)), 'model_metric_rows': int(len(model_metrics)), 'fixed_rule_metric_rows': int(len(fixed_rule_metrics)), 'fold_metric_rows': int(len(fold_metrics)), 'direction_metric_rows': int(len(direction_metrics)), 'calibration_rows': int(len(calibration)), 'coefficient_rows': int(len(coefficients))}, 'source_validation': {'m4a_quality_report_validated': True, 'm4b_result_status': m4b_decision['result_status'], 'm4b_selected_rule': m4b_decision['selected_rule']}, 'anti_leakage': {'feature_source': 'M4A signal-time indicator context only', 'model_feature_groups': FEATURE_GROUPS, 'label_or_future_fields_used_as_features': [], 'h2_target_used_only_for_fold_training_and_oos_evaluation': True, 'fold_local_imputation': True, 'fold_local_scaling': True, 'fold_local_encoding': True, 'h2_purge_d1_sessions': PURGE_D1_SESSIONS, 'all_fold_purge_invariants_pass': bool((fold_metrics['maximum_train_label_completion_index'] < fold_metrics['first_test_session_index']).all()), 'shuffle': False, 'threshold_tuning': False, 'hyperparameter_search': False, 'post_hoc_feature_selection': False}, 'negative_controls': {'direction_only': True, 'permutation_seed': PERMUTATION_SEED, 'permutation_repetitions': PERMUTATION_REPETITIONS, 'training_labels_only_permuted': True, 'oos_labels_unchanged': True}, 'declared_outputs': list(DECLARED_OUTPUT_FILES), 'result_status': decision['result_status'], 'persistent_model_artifact_emitted': False, 'runtime_or_trading_logic_present': False}
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = _output_paths(output_dir)
    _write_json(output_paths[OUTPUT_RUN_METADATA], metadata)
    _write_csv(output_paths[OUTPUT_PREDICTIONS], predictions)
    _write_csv(output_paths[OUTPUT_MODEL_METRICS], model_metrics)
    _write_csv(output_paths[OUTPUT_FIXED_RULE_METRICS], fixed_rule_metrics)
    _write_csv(output_paths[OUTPUT_FOLD_METRICS], fold_metrics)
    _write_csv(output_paths[OUTPUT_DIRECTION_METRICS], direction_metrics)
    _write_csv(output_paths[OUTPUT_COEFFICIENTS], coefficients)
    _write_csv(output_paths[OUTPUT_CALIBRATION], calibration)
    _write_json(output_paths[OUTPUT_PERMUTATION], permutation)
    _write_json(output_paths[OUTPUT_QUALITY], quality)
    _write_json(output_paths[OUTPUT_DECISION], decision)
    return {'metadata': metadata, 'quality_report': quality, 'decision': decision, 'output_paths': {name: str(path) for name, path in output_paths.items()}}

def main(argv: Sequence[str] | None=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    context_path, labels_path, quality_path, m4b_decision_path, output_dir, run_id, git_sha = _validate_cli_args(args, parser)
    run_research_package(indicator_context_path=context_path, labels_path=labels_path, quality_report_path=quality_path, m4b_decision_path=m4b_decision_path, output_dir=output_dir, run_id=run_id, git_commit_sha=git_sha)
    return 0
if __name__ == '__main__':
    raise SystemExit(main())
