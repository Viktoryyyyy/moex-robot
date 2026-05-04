# futures_daily_refresh_scheduler_contract

status: implemented_contract
project: MOEX Bot
scope: Slice 1 daily data refresh scheduler
artifact_class: external_operational_contract
format: markdown
schema_version: futures_daily_refresh_scheduler.v1

purpose: Operational scheduler contract for unattended Slice 1 daily futures data refresh. The scheduler must call the canonical daily refresh orchestrator and must not duplicate loader, FUTOI, or D1 builder logic.

canonical_entrypoint: src/moex_data/futures/daily_refresh_runner.py
scheduler_type: systemd_timer
server_unit_path: /etc/systemd/system/moex-futures-daily-refresh.service
server_timer_path: /etc/systemd/system/moex-futures-daily-refresh.timer
working_directory: /home/trader/moex_bot/moex-robot
python_executable: /home/trader/moex_bot/venv/bin/python
environment_contract:
- MOEX_DATA_ROOT=/home/trader/moex_bot/data

scheduled_command:
```text
/home/trader/moex_bot/venv/bin/python src/moex_data/futures/daily_refresh_runner.py --snapshot-date 2026-04-29 --data-root /home/trader/moex_bot/data
```

mandatory_arguments:
- --snapshot-date 2026-04-29
- --data-root /home/trader/moex_bot/data

implicit_defaults_allowed:
- --run-date may default to the runner's current Moscow-date default until a separate scheduling/date-semantics contract replaces it.
- --whitelist may default to the Slice 1 accepted whitelist in src/moex_data/futures/slice1_common.py.
- --excluded may default to the Slice 1 excluded list in src/moex_data/futures/slice1_common.py.

forbidden_scheduler_behavior:
- do not call raw_5m_loader.py directly from the scheduler.
- do not call futoi_raw_loader.py directly from the scheduler.
- do not call derived_d1_ohlcv_builder.py directly from the scheduler.
- do not change MOEX_DATA_ROOT from /home/trader/moex_bot/data.
- do not remove or replace --snapshot-date 2026-04-29 until registry refresh is separately automated.
- do not add continuous series generation.
- do not expand to all futures.
- do not call strategy, runtime trading, or research entrypoints.

systemd_service_contract:
```text
[Unit]
Description=MOEX Bot Slice 1 futures daily data refresh
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=trader
WorkingDirectory=/home/trader/moex_bot/moex-robot
Environment=MOEX_DATA_ROOT=/home/trader/moex_bot/data
ExecStart=/home/trader/moex_bot/venv/bin/python src/moex_data/futures/daily_refresh_runner.py --snapshot-date 2026-04-29 --data-root /home/trader/moex_bot/data
```

systemd_timer_contract:
```text
[Unit]
Description=Run MOEX Bot Slice 1 futures daily data refresh

[Timer]
OnCalendar=*-*-* 23:45:00
Persistent=true
Unit=moex-futures-daily-refresh.service

[Install]
WantedBy=timers.target
```

manual_validation_command:
```text
sudo systemctl daemon-reload && sudo systemctl start moex-futures-daily-refresh.service && sudo systemctl status --no-pager moex-futures-daily-refresh.service
```

enable_command:
```text
sudo systemctl enable --now moex-futures-daily-refresh.timer && systemctl list-timers --all --no-pager | grep moex-futures-daily-refresh
```

log_or_status_observation:
- systemctl status --no-pager moex-futures-daily-refresh.service
- systemctl status --no-pager moex-futures-daily-refresh.timer
- journalctl -u moex-futures-daily-refresh.service --no-pager -n 200
- ${MOEX_DATA_ROOT}/futures/runs/daily_refresh/run_date={run_date}/manifest.json

success_criteria:
- systemd service unit exists at server_unit_path.
- systemd timer unit exists at server_timer_path.
- timer is enabled and active.
- manual service start exits with code 0.
- daily_refresh_runner.py writes a manifest under ${MOEX_DATA_ROOT}/futures/runs/daily_refresh/run_date={run_date}/manifest.json.
- manifest schema_version equals futures_daily_data_refresh_manifest.v1.
- manifest daily_refresh_result_verdict equals pass.
- manifest artifact_validation_status equals pass.
- child_component_status contains raw_5m_loader, futoi_raw_loader, and derived_d1_ohlcv_builder.

fail_closed_contract:
- The scheduler delegates failure semantics to daily_refresh_runner.py.
- The service must fail non-zero if daily_refresh_runner.py fails non-zero.
- The timer must not call later components independently after a runner failure.
- Failed runs must be observable via systemd status or journalctl.
- The daily refresh manifest blockers field is the canonical data-refresh failure explanation when a manifest is written.

operational_notes:
- The timer schedule is server-local time.
- The schedule is intentionally limited to Slice 1 daily refresh only.
- Registry snapshot automation is out of scope; --snapshot-date 2026-04-29 is intentionally fixed by this contract.
