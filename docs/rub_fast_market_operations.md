# Independent fast market cache

The opt-in reader overlays synchronized market/OI and recomputes basis from the same generation. It never fetches quotes. The heavy snapshot identity, age and slow source components remain separate. Training and trade execution are not enabled.

The user timer collects every 30 seconds with a 25-second process deadline. Generation lifetime is 60 seconds from collection start; existing read-time source age and quality gates still apply independently. A failed attempt publishes FAILED, which revokes fast factual authority immediately. Missing, malformed, future or expired enabled cache fails closed without falling back to the heavy quotes. Atomic publication preserves the previous complete file if publication fails; that file expires at its original deadline. SHA-256 detects accidental corruption, not authenticity.

Deploy after tests and merge, as trader:
```sh
loginctl enable-linger trader
mkdir -p ~/.config/systemd/user
cp ops/systemd/user/moex-rub-fast-market.* ~/.config/systemd/user/
systemctl --user daemon-reload
PYTHONPATH=src:. /home/trader/moex_bot/venv/bin/python -m moex_data.rub_fast_market --enable
systemctl --user enable --now moex-rub-fast-market.timer
```

Run from /home/trader/moex_bot/moex-robot with the normal MOEX environment. Restart the factual API and MCP service to load the reader. Verify several automatic generations, API fast_market_read, actual source ages, unchanged slow source identity, and loginctl Linger=yes. Inspect failures using journalctl --user -u moex-rub-fast-market.service.

Rollback: stop and disable the user timer, then remove only the regular enabled marker at $MOEX_DATA_ROOT/state/rub_intelligence/chat_analysis_snapshot/fast_market/enabled. The reader resumes the legacy heavy snapshot with its existing source-age gate. Keep current.json for diagnosis. No sudoers changes are needed.
