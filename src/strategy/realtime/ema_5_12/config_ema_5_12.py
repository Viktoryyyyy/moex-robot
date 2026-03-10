from pathlib import Path

# Core instrument settings
FO_KEY = "Si"  # key for resolving current Si future via existing FO API

# EMA parameters
EMA_FAST_WINDOW = 5
EMA_SLOW_WINDOW = 12

# Commission and execution modelling
# Commission is set as 2 points per completed trade (entry+exit).
# Exact usage will be defined in executor_ema_5_12.py, но базовое
# значение зафиксировано здесь.
COMMISSION_PTS_PER_TRADE = 2.0

# Fallback slippage if 2nd level in orderbook is not available.
EXEC_FALLBACK_SLIPPAGE_PTS = 2.0

# Timezone identifier for Moscow Exchange
MSK_TZ = "Europe/Moscow"

# Paths (relative to project root, assuming cwd = ~/moex_bot)
SIGNALS_DIR = Path("data") / "signals"
STATE_PATH = Path("config") / "ema_5_12_state.json"

# Daily regime file (R1: trading today is decided by yesterday's regime).
# Expected lightweight CSV with columns:
#   TRADEDATE, regime_day_ema_5_12_D5000
