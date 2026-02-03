# Grid Trading Bot + GUI (private)

**Disclaimer:** this project is intentionally **redacted/trimmed**. Trading with any bot involves a risk of losing funds.  
This is **not** investment advice. Use it on a demo account or with minimal position sizes.

## TL;DR
A grid trading bot + a PyQt5 GUI panel for managing per-ticker parameters.  
The project stores configurations and runtime state locally in the `data/` folder and keeps logs and an event/order history.

---

## Project structure

├─ bot.py
├─ trading_api.py
├─ misc.py
├─ trade_db.py            # SQLite branch (fills + reports)
├─ Settings.py
├─ UI.py                  # GUI logic
├─ qUI.py                 # auto-generated UI (pyuic5)
├─ main.ui                # Qt Designer source
├─ config.py              # local config (not tracked)
├─ run_bot.bat
├─ run_panel.bat
├─ setup.bat
├─ update.bat
├─ installPython.txt
└─ data/                  # local runtime data (not tracked)
   ├─ API.txt             # local keys/credentials
   ├─ couples.txt         # local tickers/params
   ├─ settings.txt        # local runtime state
   ├─ logs/
   │  ├─ error_log.jsonl
   │  └─ operation_log.jsonl
   ├─ orders/             # optional JSON order cards
   └─ clearing/           # optional snapshots/service files
## Quick start (Windows)

### 1) Install Python
Python **3.11** is recommended.  
Notes: `installPython.txt`.

### 2) Install dependencies
Run:
- setup.bat

### 3) Run

GUI:
- run_panel.bat (Win)
- run_panel.sh (Lin)(option)

Bot:
- run_bot.bat (Windows)
- run_bot.sh (Lin)(option)

The GUI updates the ticker configuration (data/couples.txt) and stores user actions via Settings.py.  
The bot reads the configuration and executes the trading logic.

## Configuration & state

### Ticker configs: `data/couples.txt`
The GUI writes per-ticker strategy parameters (typical set):

- `enable` (ON/OFF)
- `symbol` (ticker)
- `side` (long/short)
- `size` (order size in lots)
- `step_orders` (grid step in instrument price ticks)
- `quantity_orders` (number of grid levels)
- `TP` (take-profit step in price ticks)

Additional fields (limits/stop_loss/special modes) depend on the specific project branch/version.

### Runtime state: `data/settings.txt`
Used by the bot to keep internal state between iterations:
- which levels/orders are already placed
- which events have already been processed
- internal flags and caches

---

## Logs & diagnostics

- `data/logs/error_log.jsonl` — errors  
- `data/logs/operation_log.jsonl` — events/operations  

If order history is enabled:
- `data/orders/*.json` — “order cards” and related events

---

## Architecture (very brief)

- `bot.py` — main domain logic: grid, position management, limits, reactions to fills/restores
- `trading_api.py` — broker interface and low-level operations (orders/positions/price-step quantization)
- `Settings.py` + `data/` — local file-based storage for configs and state (convenient for debugging)
- `UI.py` / `qUI.py` — GUI for parameter management and manual commands

---

## SQLite branch/version (order history)

There is an alternative version where order history and statuses are stored not as JSON files (data/orders/*.json), but in a SQLite database (data.db).  
The goal is to have a single source of truth for all created/filled/canceled orders and easy queries (by ticker, status, period, PnL, causal events).

### Why SQLite instead of JSON
- Fast queries: “all FILLED in a week”, “orders without TP”, “partial fills”
- Less filesystem noise and lower risk of “broken JSON” after a crash
- Easier reporting/statistics without scanning folders

### Where the database lives
The DB is typically stored in data/, for example:
- data/data.db

### What is stored in the DB (conceptually)
Typical entities:
- orders: all orders (created/active/closed)
- fills/executions: fills (including partials)
- events/log: lifecycle events (create, cancel, modify, errors)
- positions/snapshots: position snapshots for recovery/analytics