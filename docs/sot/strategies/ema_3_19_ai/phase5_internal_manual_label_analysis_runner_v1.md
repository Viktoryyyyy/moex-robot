# Phase 5 internal manual label analysis runner v1

Status: checked-in runner only  
Project: MOEX Bot  
Lane: `ema_3_19_ai`  
Task: `ema_3_19_ai_market_phase_phase_5_0_internal_d1_panel_manual_label_analysis_runner`

## Purpose

This artifact defines a deterministic internal-only CLI runner for Phase 5 descriptive analysis of USDRUBF / Si D1 market phases.

The runner joins the existing internal D1 panel with checked-in manual B / S / OUT labels and produces descriptive analysis artifacts when it is executed later under a separate PM L2 server approval.

This PR only checks in the runner, tests, and documentation. It does not execute the runner and does not materialize analysis outputs.

## Authority boundary

Execution and artifact materialization require a separate PM L2 server approval after this PR is merged into `main`.

This PR does not authorize:

- server commands;
- server output writes;
- external data ingestion;
- provider API calls;
- model fitting;
- prediction;
- trading or broker actions;
- runtime or systemd changes;
- generated data committed to the repository.

Merge authority remains `PM_L2_ONLY`.

## CLI entrypoint

```text
python -m moex_research.runners.usdrubf_phase5_internal_manual_label_analysis
```

Required input and output flags:

```text
--panel-path
--panel-manifest-path
--label-contract-path
--output-dir
--run-id
```

Required safety gates:

```text
--internal-d1-only
--no-external-data
--no-model-fitting
--no-prediction
--no-trading
```

The runner fails before reading inputs or writing outputs if any safety gate is missing.

## Inputs

The runner expects:

1. Existing internal D1 panel parquet from the approved Phase 3.4 build.
2. Existing D1 panel manifest JSON.
3. Checked-in manual label contract JSON.
4. The checked-in manual label materializer:
   `src/moex_research/labels/usdrubf_d1_manual_phase_labels.py`.

The runner does not fetch market data and does not call external providers.

## Outputs when executed later

The runner writes only these required artifacts to `--output-dir`:

```text
manifest.json
analysis_report.md
phase_summary.csv
transition_counts.csv
boundary_window_summary.csv
joined_panel_preview.csv
```

The runner does not write `joined_panel.parquet` by default.

A full joined parquet export is intentionally not part of this PR. If PM L2 later wants that artifact, it requires an explicit future approval and scope.

## Analysis contents

The generated report and CSV artifacts cover:

- row counts by phase;
- date coverage by phase;
- OHLC, return, range, EMA 3/19 context, and optional volume/value/trade-count summaries by phase;
- transition counts across B / S / OUT;
- behavior around manual phase boundaries;
- a preview of the joined D1 panel and labels.

## Label and EMA semantics

Manual labels are the label source for Phase 5 descriptive analysis. They are manual research labels and are not EMA-derived.

EMA 3/19 is computed only from the internal D1 close series as baseline/context. EMA 3/19 is not the label source, not a model, not a prediction, and not trading advice.

Manual labels remain offline research labels. They must not become runtime features, assistant-visible runtime state, broker instructions, or production trading signals.

## Refusal boundaries

The runner refuses or avoids these scopes:

- missing safety flags;
- input panels already containing manual label or target-like fields;
- non-B/S/OUT manual label contract values;
- external data;
- provider/network calls;
- model fitting;
- prediction generation;
- trading or broker execution.

## Current PR boundary

This PR creates only:

```text
src/moex_research/runners/usdrubf_phase5_internal_manual_label_analysis.py
tests/ema_3_19_ai/test_phase5_internal_manual_label_analysis_runner.py
docs/sot/strategies/ema_3_19_ai/phase5_internal_manual_label_analysis_runner_v1.md
```

No server analysis is run. No repository data artifacts are created. No existing D1 panel builder, D1 panel output, manual label contract, or manual label materializer is modified.
