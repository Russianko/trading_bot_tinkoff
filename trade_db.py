import sqlite3
import pathlib
import threading
import datetime as _dt
from decimal import Decimal
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict



_DB_PATH = pathlib.Path("data") / "trades.db"
_lock = threading.Lock()
_SCHEMA_OK = False


# --- Отчёт по произвольному окну времени (по UTC ts) ---

def get_full_daily_report_window(start_utc: _dt.datetime,
                                 end_utc: _dt.datetime):
    """
    Возвращает агрегированный отчёт по сделкам за окно [start_utc; end_utc):
    группировка по (symbol, strategy_side).

    start_utc / end_utc — naive datetime в UTC.
    """
    global _SCHEMA_OK
    if not _SCHEMA_OK:
        init_db()

    # приводим к строкам формата, как в ts (ISO + 'Z')
    start_str = start_utc.replace(tzinfo=None).isoformat() + "Z"
    end_str   = end_utc.replace(tzinfo=None).isoformat() + "Z"

    with _lock, sqlite3.connect(_DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                symbol,
                strategy_side,
                COUNT(*)                                       AS trades_count,
                SUM(CASE WHEN dir = 'BUY'  THEN 1   ELSE 0 END) AS buy_trades,
                SUM(CASE WHEN dir = 'SELL' THEN 1   ELSE 0 END) AS sell_trades,
                SUM(CASE WHEN dir = 'BUY'  THEN qty ELSE 0 END) AS buy_qty,
                SUM(CASE WHEN dir = 'SELL' THEN qty ELSE 0 END) AS sell_qty,
                SUM(CASE WHEN dir = 'BUY'  THEN qty*price ELSE 0 END) AS buy_amount,
                SUM(CASE WHEN dir = 'SELL' THEN qty*price ELSE 0 END) AS sell_amount,
                SUM(commission) AS commission_sum
            FROM trade_fills
            WHERE ts >= ? AND ts < ?
            GROUP BY symbol, strategy_side
            ORDER BY symbol, strategy_side;
        """, (start_str, end_str))
        rows = cur.fetchall()

    report = []
    for (symbol, strategy_side,
         trades_count, buy_trades, sell_trades,
         buy_qty, sell_qty,
         buy_amount, sell_amount,
         commission_sum) in rows:

        buy_amount      = float(buy_amount or 0.0)
        sell_amount     = float(sell_amount or 0.0)
        commission_sum  = float(commission_sum or 0.0)
        strategy_side_s = (strategy_side or "").lower()

        # Для long: profit = sell - buy
        # Для short: profit = buy - sell
        if strategy_side_s == "short":
            pnl = buy_amount - sell_amount
        else:
            pnl = sell_amount - buy_amount

        report.append({
            "symbol":         symbol,
            "strategy_side":  strategy_side or "",
            "trades_count":   int(trades_count or 0),
            "buy_trades":     int(buy_trades or 0),
            "sell_trades":    int(sell_trades or 0),
            "buy_qty":        int(buy_qty or 0),
            "sell_qty":       int(sell_qty or 0),
            "buy_amount":     buy_amount,
            "sell_amount":    sell_amount,
            "commission":     commission_sum,
            "pnl":            pnl,
        })

    return report



def get_full_daily_report(
    trade_date: str,
    symbol_multipliers: Dict[str, float] | None = None,
) -> List[Dict[str, Any]]:
    """
    Расширенный отчёт по сделкам за день.

    trade_date: 'YYYY-MM-DD'
    symbol_multipliers: опционально словарь {symbol: multiplier},
        если по инструменту PnL нужно домножать (стоимость пункта, размер контракта и т.п.).
        Если не указан, считаем PnL как (разница цен * qty).

    ВАЖНО: PnL считается ТОЛЬКО по сделкам внутри этого дня (локальный FIFO).
    Если позиция переезжает через ночь, этот отчёт покажет PnL только по тем
    открытию/закрытиям, которые попали в указанный trade_date.
    """

    global _SCHEMA_OK
    if not _SCHEMA_OK:
        init_db()

    symbol_multipliers = symbol_multipliers or {}

    with _lock, sqlite3.connect(_DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                symbol,
                strategy_side,
                dir,
                kind,
                qty,
                price,
                commission,
                ts
            FROM trade_fills
            WHERE trade_date = ?
            ORDER BY symbol, ts, id
            """,
            (trade_date,),
        )
        rows = cur.fetchall()

    # агрегаты по инструментам
    stats = {}

    # Для PnL: локальный FIFO-стек по каждому символу
    # structure: per-symbol: list of {"qty_left": int, "price": float}
    # отдельно для long/short логика ниже
    open_lots: Dict[str, List[Dict[str, float]]] = defaultdict(list)

    for symbol, strategy_side, dir_, kind, qty, price, commission, ts in rows:
        strategy_side = (strategy_side or "long").lower()
        dir_ = (dir_ or "").upper()
        qty = int(qty)
        price = float(price)
        commission = float(commission or 0.0)

        st = stats.setdefault(symbol, {
            "symbol": symbol,
            "strategy_side": strategy_side,
            "buy_trades": 0,
            "sell_trades": 0,
            "buy_qty": 0,
            "sell_qty": 0,
            "commission": 0.0,
            "realized_pnl": 0.0,   # до комиссий
            "net_pnl": 0.0,        # после комиссий (заполним в конце)
        })

        # счётчики сделок и объёма
        if dir_ == "BUY":
            st["buy_trades"] += 1
            st["buy_qty"] += qty
        elif dir_ == "SELL":
            st["sell_trades"] += 1
            st["sell_qty"] += qty

        st["commission"] += commission

        # --- PnL: упрощённая модель FIFO внутри дня ---

        # Для long-стратегии:
        #   BUY  -> открытие/наращивание позиции
        #   SELL -> закрытие/частичное закрытие -> считаем PnL
        #
        # Для short-стратегии:
        #   SELL -> открытие/наращивание шорта
        #   BUY  -> закрытие/частичное закрытие шорта -> считаем PnL

        lots = open_lots[symbol]

        if strategy_side == "long":
            if dir_ == "BUY":
                # Открытие / увеличение лонга
                lots.append({"qty_left": qty, "price": price})
            elif dir_ == "SELL":
                # Закрытие части лонга — считаем PnL по FIFO
                qty_to_close = qty
                while qty_to_close > 0 and lots:
                    lot = lots[0]
                    take = min(qty_to_close, lot["qty_left"])
                    # прибыль: продали дороже покупки
                    pnl_piece = (price - lot["price"]) * take
                    st["realized_pnl"] += pnl_piece

                    lot["qty_left"] -= take
                    qty_to_close -= take

                    if lot["qty_left"] <= 0:
                        lots.pop(0)

        else:  # strategy_side == "short"
            if dir_ == "SELL":
                # Открытие / увеличение шорта
                lots.append({"qty_left": qty, "price": price})
            elif dir_ == "BUY":
                # Закрытие части шорта — считаем PnL по FIFO
                qty_to_close = qty
                while qty_to_close > 0 and lots:
                    lot = lots[0]
                    take = min(qty_to_close, lot["qty_left"])
                    # прибыль: продали дороже, выкупили дешевле
                    pnl_piece = (lot["price"] - price) * take
                    st["realized_pnl"] += pnl_piece

                    lot["qty_left"] -= take
                    qty_to_close -= take

                    if lot["qty_left"] <= 0:
                        lots.pop(0)

    # применяем мультипликаторы и считаем net_pnl
    result = []
    for symbol, st in stats.items():
        mult = float(symbol_multipliers.get(symbol, 1.0))
        realized = st["realized_pnl"] * mult
        commission_sum = st["commission"]
        st["realized_pnl"] = realized
        st["net_pnl"] = realized - commission_sum
        result.append(st)

    # можно отсортировать по символу или по net_pnl
    result.sort(key=lambda x: x["symbol"])
    return result


def init_db():
    """
    Инициализация / миграция схемы trade_fills.
    Приводим всё к единому виду с колонками:
      ts, trade_date, symbol, strategy_side, dir, kind, qty, price, commission, order_id
    """
    global _SCHEMA_OK

    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with _lock, sqlite3.connect(_DB_PATH) as conn:
        cur = conn.cursor()

        # есть ли вообще таблица trade_fills
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_fills'")
        exists = cur.fetchone() is not None

        if not exists:
            # свежая база — сразу создаём новую схему
            cur.execute("""
                CREATE TABLE trade_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,           -- полное время ISO
                    trade_date TEXT NOT NULL,   -- YYYY-MM-DD
                    symbol TEXT NOT NULL,
                    strategy_side TEXT NOT NULL, -- 'long' / 'short'
                    dir TEXT NOT NULL,           -- 'BUY' / 'SELL'
                    kind TEXT NOT NULL,          -- OPEN:MARKET, TP:FROM_MARKET и т.п.
                    qty INTEGER NOT NULL,
                    price REAL NOT NULL,
                    commission REAL NOT NULL DEFAULT 0.0,
                    order_id TEXT
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_date ON trade_fills(trade_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON trade_fills(symbol, trade_date);")
            conn.commit()
            _SCHEMA_OK = True
            return

        # таблица есть — смотрим, какие в ней сейчас колонки
        cur.execute("PRAGMA table_info(trade_fills)")
        cols = [r[1] for r in cur.fetchall()]

        # уже новая схема — ничего не делаем
        if "ts" in cols and "dir" in cols:
            _SCHEMA_OK = True
            return

        # старая prod-схема: ts_utc + trade_dir (как было раньше)
        if "ts_utc" in cols and "trade_dir" in cols and "ts" not in cols and "dir" not in cols:
            # миграция: переименовываем, создаём новую, переливаем данные с маппингом колонок
            cur.execute("ALTER TABLE trade_fills RENAME TO trade_fills_old;")

            cur.execute("""
                CREATE TABLE trade_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy_side TEXT NOT NULL,
                    dir TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    price REAL NOT NULL,
                    commission REAL NOT NULL DEFAULT 0.0,
                    order_id TEXT
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_date ON trade_fills(trade_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON trade_fills(symbol, trade_date);")

            # переносим данные: ts_utc -> ts, trade_dir -> dir
            cur.execute("""
                INSERT INTO trade_fills
                    (ts, trade_date, symbol, strategy_side, dir, kind, qty, price, commission, order_id)
                SELECT
                    ts_utc,
                    trade_date,
                    symbol,
                    strategy_side,
                    trade_dir,
                    kind,
                    qty,
                    price,
                    commission,
                    order_id
                FROM trade_fills_old;
            """)

            cur.execute("DROP TABLE trade_fills_old;")
            conn.commit()
            _SCHEMA_OK = True
            return

        # если схема вообще какая-то левая — дропаем и создаём "по-новому"
        cur.execute("DROP TABLE trade_fills;")
        cur.execute("""
            CREATE TABLE trade_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                strategy_side TEXT NOT NULL,
                dir TEXT NOT NULL,
                kind TEXT NOT NULL,
                qty INTEGER NOT NULL,
                price REAL NOT NULL,
                commission REAL NOT NULL DEFAULT 0.0,
                order_id TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_date ON trade_fills(trade_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON trade_fills(symbol, trade_date);")
        conn.commit()
        _SCHEMA_OK = True


def get_daily_report(trade_date: str):
    """
    trade_date: 'YYYY-MM-DD' (например, _now_msk().date().isoformat())
    Возвращает список словарей по инструментам.
    """
    with _lock, sqlite3.connect(_DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                symbol,
                SUM(CASE WHEN dir='BUY'  THEN 1 ELSE 0 END) as buy_trades,
                SUM(CASE WHEN dir='SELL' THEN 1 ELSE 0 END) as sell_trades,
                SUM(commission) as commission_sum,
                SUM(CASE WHEN dir='BUY'  THEN qty ELSE 0 END) -
                SUM(CASE WHEN dir='SELL' THEN qty ELSE 0 END) as volume_diff
            FROM trade_fills
            WHERE trade_date = ?
            GROUP BY symbol
            ORDER BY symbol;
            """,
            (trade_date,),
        )
        rows = cur.fetchall()

    result = []
    for symbol, buy_tr, sell_tr, comm, vol_diff in rows:
        result.append({
            "symbol": symbol,
            "buy_trades": int(buy_tr or 0),
            "sell_trades": int(sell_tr or 0),
            "commission": float(comm or 0.0),
            "volume_diff": int(vol_diff or 0),
        })
    return result


def send_daily_report_via_misc() -> None:
    """
    Автоматический отчёт за окно [19:30 МСК предыдущего дня; 19:30 МСК текущего дня).
    Печатает в консоль через misc.send_msg().
    """
    import misc  # локальный импорт, чтобы не словить циклический

    # текущее время
    now_utc = _dt.datetime.utcnow()
    msk_offset = _dt.timedelta(hours=3)
    now_msk = now_utc + msk_offset

    # "сегодняшние" 19:30 МСК
    anchor_msk = _dt.datetime.combine(now_msk.date(), _dt.time(19, 30))

    # если ещё не дошли до 19:30 — считаем, что это конец предыдущего окна
    if now_msk < anchor_msk:
        end_msk = anchor_msk
        start_msk = anchor_msk - _dt.timedelta(days=1)
    else:
        start_msk = anchor_msk
        end_msk = anchor_msk + _dt.timedelta(days=1)

    start_utc = start_msk - msk_offset
    end_utc = end_msk - msk_offset

    report = get_full_daily_report_window(start_utc, end_utc)

    if not report:
        misc.send_msg(
            f"Отчёт за период "
            f"{start_msk.strftime('%Y-%m-%d %H:%M')}–{end_msk.strftime('%Y-%m-%d %H:%M')} МСК: сделок нет."
        )
        return

    lines = [
        f"Отчёт по сделкам за период "
        f"{start_msk.strftime('%Y-%m-%d %H:%M')}–{end_msk.strftime('%Y-%m-%d %H:%M')} МСК:"
    ]
    for r in report:
        lines.append(
            f"{r['symbol']} [{r['strategy_side'] or '-'}]: "
            f"сделки={r['trades_count']}, "
            f"BUY={r['buy_trades']} ({r['buy_qty']} л.), "
            f"SELL={r['sell_trades']} ({r['sell_qty']} л.), "
            f"комиссия={r['commission']:.2f}, "
            f"P&L={r['pnl']:.2f}"
        )

    misc.send_msg("\n".join(lines))


def _calc_trade_dir(strategy_side: str, kind: str) -> str:
    """
    Возвращает направление сделки 'BUY' / 'SELL' в зависимости от стороны стратегии и вида ордера.
    """
    side = (strategy_side or "long").lower()
    k = (kind or "").upper()
    if k.startswith("OPEN"):
        return "BUY" if side == "long" else "SELL"
    if "TP" in k:
        return "SELL" if side == "long" else "BUY"
    return "BUY" if side == "long" else "SELL"


def log_trade_fill_to_db(
        *,
        order_id: str,
        symbol: str,
        strategy_side: str,
        kind: str,
        qty: int,
        price: float,
        commission: float = 0.0,
        ts: str | None = None,
):
    """
    Логируем ИМЕННО ФАКТ исполнения сделки (fill).
    Вызывается один раз, когда ордер переходит в состояние FILLED.
    """
    global _SCHEMA_OK
    if not _SCHEMA_OK:
        init_db()

    if ts is None:
        now = _dt.datetime.utcnow()
    else:
        # поддержим формат "...Z"
        now = _dt.datetime.fromisoformat(ts.replace("Z", ""))

    trade_date = now.date().isoformat()
    dir_ = _calc_trade_dir(strategy_side, kind)

    with _lock, sqlite3.connect(_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO trade_fills
                (ts, trade_date, symbol, strategy_side, dir, kind, qty, price, commission, order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now.isoformat() + "Z",
                trade_date,
                symbol,
                strategy_side.lower(),
                dir_,
                kind,
                int(qty),
                float(price),
                float(commission),
                str(order_id) if order_id is not None else None,
            ),
        )
        conn.commit()