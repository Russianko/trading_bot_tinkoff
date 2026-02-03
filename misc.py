import trading_api
import datetime
import time
import Settings
import json
import os
from tinkoff.invest import Client, OperationType, OperationState            # ← нужен для чтения портфеля
from config import TOKEN                   # ← твой токен (уже есть в config.py)


# --- Информация об инструментах (как и было) ---
figi = trading_api.get_figi()

# --- Директории логов ---
log_dir = "data/logs"
os.makedirs(log_dir, exist_ok=True)

# Файлы логов (JSON Lines — по строке на событие)
error_log_file_path = os.path.join(log_dir, "error_log.jsonl")
operation_log_file_path = os.path.join(log_dir, "operation_log.jsonl")


# --- Комиссии брокера/биржи для расчёта отчёта ---

COMMISSION_RATE = float(os.environ.get("TINKOFF_COMMISSION_RATE", "0.00035"))

# ======================
#   БАЗОВЫЕ ЛОГИ
# ======================

def log_error(details):
    """Пишем ошибку в error_log.jsonl (одна JSON-строка на событие)."""
    log_entry = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "type": "error",
        "details": details
    }
    try:
        with open(error_log_file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Ошибка при записи error-лога: {str(e)}")


def log_operation(details, op_name: str | None = None):
    """
    Универсальный лог операций в operation_log.jsonl.
    details: dict или str. Имя операции — через op_name.
    """
    payload = details if isinstance(details, dict) else {"message": str(details)}
    log_entry = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "type": "operation",
        "op_name": op_name,
        "details": payload
    }
    try:
        with open(operation_log_file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Ошибка при записи operation-лога: {str(e)}")


# ======================
#   УТИЛИТЫ
# ======================

def getSymbols():
    res = []
    for s in figi.keys():
        res.append(s)
    res.sort()
    return ["-"] + res


def send_msg(*msgs):
    msgn = ""
    for m in msgs:
        msgn += str(m) + " "
    dt = str(datetime.datetime.today()).split('.')[0]

    msg = ""
    msgn = msgn.split("\n")
    for m in msgn:
        msg += f"[{dt}] {m}"
        if len(msgn) > 1 and msgn.index(m) + 1 != len(msgn):
            msg += "\n"

    Settings.saveLog(msg)
    print(msg)


def get_mp(x):
    x = int(x)
    res = "1"
    if x == 0:
        return 1
    else:
        for i in range(x):
            res += "0"
    return float(res)


def get_p(x):
    x = int(x)
    if x != 0:
        res = "0."
        for i in range(x - 1):
            res += "0"
        res += "1"
        res = float(res)
    else:
        res = 1
    return res


def transformationPrice(price, x):
    if x == 0:
        return int(price)
    price = str(price).replace(",", ".")
    try:
        full = price.split(".")[0]
        drob = price.split(".")[1]
        price = f"{full}.{drob[:x]}"
    except:
        pass
    return float(price)


def make_instant_report() -> str:
    """Моментальный отчёт: сегодняшняя торговая активность + оценка портфеля и VM на сейчас."""
    day_utc = datetime.datetime.utcnow().date().isoformat()
    return make_session_daily_report(day_utc)


def ToPriceStep(price, step):
    return round(round(price / step) * step, 5)


def WithPrice(price, TP, fi):
    x = fi["min_price"]
    step = fi["step"]
    return float(round(price + TP, x))


def WithoutPrice(price, prec, fi):
    x = fi["min_price"]
    step = fi["step"]
    return float(round(price - step * prec, x))
    # return price - step * prec


def del_order_of_orders(orders, order_id):
    norders = []
    for order in orders:
        if str(order["order_id"]) != str(order_id):
            norders.append(order)
    return norders


def del_order_list(orders, order_id):
    res = []
    for order in orders:
        if str(order["order_id"]) != str(order_id):   # <-- сравниваем как строки
            res.append(order)
    return res


def search_max_ord(orders):
    max_order = orders[0]
    for order in orders:
        if max_order["price"] < order["price"]:
            max_order = order
    return max_order


def search_min_ord(orders):
    min_order = orders[0]
    for order in orders:
        if min_order["price"] > order["price"]:
            min_order = order
    return min_order


def get_new_prices(ticker, price, step, orders):
    prices = []
    for ord in orders:
        prices.append(ord["price"])
    prices.sort()

    prices_new = []
    if price > prices[-1]:
        print(1)
        price_last = price
        for i in range(len(prices)):
            pr = WithoutPrice(price_last, step, figi[ticker])
            price_last = pr
            prices_new.append(pr)
    elif price < prices[0]:
        print(2)
        price_last = price
        for i in range(len(prices)):
            pr = WithPrice(price_last, step, figi[ticker])
            price_last = pr
            prices_new.append(pr)
    else:
        price_last_buy = price
        price_last_sell = price
        for i in range(len(prices) - 1):
            if round(prices[i + 1] - prices[i], figi[ticker]["min_price"]) > round(step * figi[ticker]["step"], figi[ticker]["min_price"]):
                pr = WithPrice(prices[i], step, figi[ticker])
                prices.append(pr)
                break

        for prc in prices:
            if price > prc:
                pr = WithPrice(price_last_buy, step, figi[ticker])
                price_last_buy = pr
                prices_new.append(pr)
            elif price < prc:
                pr = WithoutPrice(price_last_sell, step, figi[ticker])
                price_last_sell = pr
                prices_new.append(pr)

    prices_new.sort()
    return prices_new


def get_max_order(orders):
    r_order = []
    for order in orders:
        if not r_order or r_order["price"] < order["price"]:
            r_order = order
    return r_order


def get_min_order(orders):
    r_order = []
    for order in orders:
        if not r_order or r_order["price"] > order["price"]:
            r_order = order
    return r_order


# ======================
#   ИСТОРИЯ ОРДЕРОВ (по order_id)
# ======================

# ====== Вырезано ======

def orderlog_event(order_id: str, symbol: str,
                   status: str, op_name: str,
                   message: str = "", extra: dict | None = None):
    """
    Добавляет произвольное событие в историю ордера.
    status: например WORKING / FILLED / CANCELED / REPOSTED и т.д.
    """
    path = orderlog_path(order_id)
    data = _json_read(path, {})
    if not data:
        # если карточки нет — создадим заготовку, чтобы ничего не потерять
        data = {
            "order_id": str(order_id),
            "symbol": symbol,
            "created_at": datetime.datetime.utcnow().isoformat(),
            "finished_at": None,
            "status": status,
            "events": []
        }

    evt = {
        "ts": datetime.datetime.utcnow().isoformat(),
        "status": status,
        "op_name": op_name,
        "current_price": read_price(symbol),
        "message": message
    }
    if extra:
        evt["extra"] = extra

    data["status"] = status
    data["events"].append(evt)
    _json_write_atomic(path, data)


def orderlog_finish(order_id: str, final_status: str):
    """Фиксируем момент окончания жизни ордера (исполнен/отменён)."""
    path = orderlog_path(order_id)
    data = _json_read(path, {})
    if not data:
        return
    finished = datetime.datetime.utcnow().isoformat()
    data["status"] = final_status
    data["finished_at"] = finished
    data["events"].append({
        "ts": finished,
        "status": final_status,
        "op_name": "FINALIZE",
        "message": f"Завершение ордера ({final_status})"
    })
    _json_write_atomic(path, data)


# ==== Хелперы для TP и уникальности цен ====

def _quant(price: float, step: float) -> float:
    return round(round(float(price) / float(step)) * float(step), 10)


def _lot_mult(ticker: str) -> int:
    """Множитель лота инструмента (штук в лоте). Для акций на MOEX часто 10, для фьючерсов – контрактный множитель.
    Если данных нет — считаем 1."""
    try:
        return int(trading_api.get_figi()[ticker].get("lot") or 1)
    except Exception:
        try:
            return int(figi[ticker].get("lot") or 1)
        except Exception:
            return 1




def is_price_level_free(symbol: str, price: float, settings: dict) -> bool:
    """
    True — если на уровне 'price' (± 1 шаг) НЕТ активных open/close ордеров.
    Проверяем локальные структуры settings[symbol]["orders_open"/"orders_close"].
    """
    step = get_step(symbol)
    p_new = _quant(price, step)
    sym = settings.get(symbol, {})
    for dct_name in ("orders_open", "orders_close"):
        for o in sym.get(dct_name, {}).values():
            p_old = _quant(o["price"], step)
            # один и тот же уровень для BUY/SELL — запрещаем
            if abs(p_new - p_old) <= step + 1e-12:
                return False
    return True


def is_price_level_free_broker(symbol: str, price: float) -> bool:
    """
    True — если на уровне 'price' (± 1 шаг) НЕТ активных заявок у брокера.
    symbol тут — тикер, как в figi.
    """
    step = get_step(symbol)
    p_new = _quant(price, step)
    try:
        active = trading_api.get_orders(symbol)  # действующие заявки по инструменту
    except Exception as e:
        send_msg(f"{symbol}: не удалось получить активные заявки для проверки: {e}")
        # в сомнительном случае лучше НЕ ставить дубль
        return True

    for o in active:
        p_old = _quant(o["price"], step)
        if abs(p_new - p_old) <= step + 1e-12:
            return False
    return True


def is_price_level_free_combined(symbol: str, price: float, settings: dict) -> bool:
    """
    Комбинированная проверка:
    1) локальные структуры settings (open/close),
    2) активные заявки у брокера.
    """
    return is_price_level_free(symbol, price, settings) and is_price_level_free_broker(symbol, price)


def get_entry_price_from_json(order_id: str, fallback: float | None = None) -> float | None:
    """
    Ищем вход (fill) конкретного OPEN-ордера по его order_id в data/orders/<id>.json.
    Предпочтительно берём extra.fill_price из события FILLED,
    иначе берём order_price из корня.
    """
    path = orderlog_path(order_id)
    data = _json_read(path, {})
    if not data:
        return fallback
    # 1) попробуем найти событие FILLED c extra.fill_price
    for evt in reversed(data.get("events", [])):
        if evt.get("status") == "FILLED":
            extra = evt.get("extra") or {}
            fp = extra.get("fill_price")
            if fp is not None:
                try:
                    return float(fp)
                except Exception:
                    pass
    # 2) иначе fallback на исходную цену ордера
    try:
        return float(data.get("order_price"))
    except Exception:
        return fallback




def read_jsonl(path, limit=None):
    rows = []
    with open(path, encoding="utf-8") as f:
        if limit is None:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        else:
            # читаем «с хвоста» эффективно
            from collections import deque
            dq = deque(maxlen=limit)
            for line in f:
                if line.strip():
                    dq.append(json.loads(line))
            rows = list(dq)
    return rows


def check_and_build_sell_grid(symbol, ticker, couple, settings, price_s, figi):
    """
    SELL-grid ВВЕРХ (независимо от базовой торговой сетки):
    если рыночная цена выше средней по портфелю на sell_trigger и в портфеле есть лоты,
    выставляем лимитные продажи «лестницей» вверх на ВЕСЬ текущий объём портфеля
    пачками размера couple["size"] с шагом couple["step_orders"].
    """
    try:
        # 0) позиция в портфеле и средняя
        with Client(TOKEN) as client:
            pf = client.operations.get_portfolio(account_id=trading_api.account_id)
        pos = next((p for p in pf.positions if p.figi == figi[ticker]["figi"]), None)
        if not pos:
            return

        avg_price = trading_api.convert_float(pos.average_position_price)
        lots_portfolio = int(trading_api.convert_float(pos.quantity_lots))
        if lots_portfolio <= 0:
            return  # нечего продавать (для long); для short логика SELL-grid не применяется

        # 1) триггер по порогу
        raw_thr = couple.get("sell_trigger", None)
        if raw_thr in (None, ""):
            return  # SELL-grid отключён
        try:
            threshold = float(raw_thr)
            if threshold <= 0:
                return
        except Exception:
            return

        if (price_s - avg_price) <= threshold:
            return

        send_msg(f"{symbol}: avg={avg_price}, last={price_s}, thr={threshold} -> строим независимую SELL-сетку")

        # 2) параметры сетки (из базовой конфигурации)
        pack_size = int(couple["size"])
        step_mult = float(couple["step_orders"])

        # 3) сколько SELL уже висит у брокера (чтобы не перепродать)
        already_sell = 0
        try:
            active = trading_api.get_orders(ticker)
            for o in active:
                try:
                except Exception:
                    pass
        except Exception as e:
            # если брокер недоступен — fallback: считаем SELL-лотами все локальные TP для long-стороны
            send_msg(f"{symbol}: get_orders недоступен при SELL-grid: {e} — используем локальные TP как already_sell")
            side = (couple.get("side") or "").lower()
            if side == "long":
                for o in settings.get(symbol, {}).get("orders_close", {}).values():
                    already_sell += int(float(o.get("size", 0)))

        # 4) сколько ещё нужно поставить, чтобы покрыть ВЕСЬ портфель (но не больше)


        # 5) стартовая цена и уже поставленные нами уровни SELL-grid


        def _quant(p: float) -> float:
            return round(round(float(p) / float(figi[symbol]["step"])) * float(figi[symbol]["step"]), decimals)

        # Собираем наши уже поставленные SELL-grid уровни (по локальным структурам)
        sg_prices = set()
        existed_sell_grid_lots = 0
        for o in settings.get(symbol, {}).get("orders_close", {}).values():
            if o.get("tag") == "sell_grid":  # только наши SELL-grid
                sg_prices.add(_quant(o["price"]))
                existed_sell_grid_lots += int(float(o.get("size", 0)))

        # Если часть SELL-grid уже стоит — продолжаем от наивысшего нашего уровня + шаг,
        # иначе — стартуем от текущей рыночной цены.
        start_price = max(sg_prices) if sg_prices else _quant(price_s)

        # 6) строим уровни вверх: + step_orders * step инструмента каждый раз
        remaining = lots_to_cover
        level_price = start_price

        while remaining > 0:
            # следующий уровень
            level_price = _quant(WithPrice(level_price, step_mult, figi[symbol]))

            # не дублируем ТОЛЬКО собственные SELL-grid уровни
            if _quant(level_price) in sg_prices:
                continue

            size_here = pack_size if remaining >= pack_size else remaining

            try:
                ord_res = trading_api.short_limit(ticker, size_here, level_price)
                orderlog_init(str(ord_res["order_id"]), ticker, "short",
                              size_here, level_price, figi[symbol]["step"], "SELL_GRID")
                o = {"order_id": ord_res["order_id"], "price": level_price,
                     "size": size_here, "type": "tp", "tag": "sell_grid"}
                settings.setdefault(symbol, {})
                settings[symbol].setdefault("orders_close", {})[o["order_id"]] = o
                settings[symbol].setdefault("orders", []).append(o)
                sg_prices.add(_quant(level_price))
                remaining -= size_here
                send_msg(f"{symbol}: SELL-grid {size_here} @ {level_price} (осталось {remaining})")
            except Exception as e:
                # Не останавливаем построение всей сетки; просто лог и идём дальше по уровням
                send_msg(f"{symbol}: предупреждение при постановке SELL {size_here} @ {level_price}: {e}")
                # пробуем следующий уровень, не уменьшая remaining
                continue

        Settings.saveSettings(settings)

    except Exception as e:
        send_msg(f"{symbol}: ошибка в check_and_build_sell_grid: {e}")


def tail_operations(n=100):
    return read_jsonl(operation_log_file_path, limit=n)


def tail_errors(n=100):
    return read_jsonl(error_log_file_path, limit=n)


def format_no_tp_report(symbol: str, positions_no_tp: dict) -> str:
    """
    Короткий отчёт по позициям без TP для символа.
    positions_no_tp: {order_id: {"entry": float, "size": float, "side": str,
                                  "suggested_tp": float, "deferred_at": epoch}}
    """
    if not positions_no_tp:
        return f"{symbol}: при остановке — позиций без TP не обнаружено."

    lines = [f"{symbol}: при остановке — позиции БЕЗ TP ({len(positions_no_tp)} шт.):"]
    now = time.time()
    for oid, data in positions_no_tp.items():
        entry = data.get("entry")
        size = data.get("size")
        side = data.get("side")
        tp   = data.get("suggested_tp")
        age_s = int(now - float(data.get("deferred_at", now)))
        age   = f"{age_s//3600}h {age_s%3600//60}m {age_s%60}s" if age_s else "0s"
        lines.append(f"  • {oid}: entry={entry}, size={size}, side={side}, TP={tp}, age={age}")
    return "\n".join(lines)


# ======================
#   DAILY REPORT
# ======================

from collections import defaultdict
import threading

MOSCOW_UTC_OFFSET = 3  # МСК = UTC+3

def _utc_today_str():
    return datetime.datetime.utcnow().date().isoformat()

def _is_ts_on_day(ts_iso: str, day_utc: str) -> bool:
    # ожидаем ts вида 'YYYY-MM-DDThh:mm:ss' (как мы пишем в orderlog_* и log_operation)
    return str(ts_iso).startswith(day_utc)

def _iter_day_filled_events(day_utc: str):
    """
    Бежим по data/orders/*.json и собираем события FILLED за указанный день (UTC).
    На основании side + op_name определяем BUY/SELL и сумму сделки.
    """
    if not os.path.isdir(ORDERS_DIR):
        return
    for fname in os.listdir(ORDERS_DIR):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(ORDERS_DIR, fname)
        try:
            data = _json_read(path, {})
            symbol = data.get("symbol")
            side   = (data.get("side") or "").lower()   # 'long' | 'short'
            size   = float(data.get("size") or 0)
            order_price_root = float(data.get("order_price") or 0)
            events = data.get("events", [])
            for evt in events:
                if evt.get("status") != "FILLED":
                    continue
                ts  = evt.get("ts", "")
                if not _is_ts_on_day(ts, day_utc):
                    continue
                opn = (evt.get("op_name") or "").upper()  # 'OPEN->FILLED' | 'TP->FILLED'
                # цена исполнения: при OPEN мы писали extra.fill_price; иначе fallback на order_price
                fill = evt.get("extra", {}).get("fill_price")
                try:
                    price = float(fill if fill is not None else evt.get("order_price", order_price_root))
                except Exception:
                    price = order_price_root
                # Классификация сделки на BUY/SELL:
                if "OPEN" in opn:
                    trade = "BUY" if side == "long" else "SELL"
                elif "TP" in opn:
                    trade = "SELL" if side == "long" else "BUY"
                else:
                    # На всякий случай – если придёт другой тег.
                    # Считаем OPEN как выше.
                    trade = "BUY" if side == "long" else "SELL"
                yield {
                    "symbol": symbol,
                    "trade": trade,      # 'BUY'|'SELL'
                    "side": side,        # 'long'|'short'
                    "size": float(size),
                    "price": float(price),
                    "ts": ts,
                    "op_name": opn,
                }
        except Exception:
            continue

def _get_portfolio_snapshot():
    """
    Возвращает
      pf_positions: {
        ticker: {
          "lots": float,           # лоты в портфеле
          "avg_price": float,      # средняя цена входа
          "last_price": float,     # текущая рыночная
          "market_value": float,   # оценка позиции (руб)
          "mtm": float,            # открытая P/L сейчас (руб) = (last-avg)*lots*lot_mult
          "expected_yield": float|None,  # если брокер отдает
          "instrument_type": str,  # для фьючерсов будет 'futures' / 'instrument_type_futures' и т.п.
          "lot_mult": int          # множитель лота
        }
      }
    """
    pf_positions = {}
    try:
        with Client(TOKEN) as client:
            pf = client.operations.get_portfolio(account_id=trading_api.account_id)

        fmap = {v["figi"]: k for k, v in figi.items()}  # figi -> тикер

        for p in pf.positions:
            ticker = fmap.get(p.figi, p.figi)

            lots = float(trading_api.convert_float(getattr(p, "quantity_lots", 0)) or 0)
            if abs(lots) < 1e-9:
                continue

            avgp = float(trading_api.convert_float(getattr(p, "average_position_price", 0)) or 0)
            last = float(trading_api.get_price(ticker))

            lot_mult = _lot_mult(ticker)
            mtm = (last - avgp) * lots * lot_mult
            mv  = last * lots * lot_mult

            exp_yield = getattr(p, "expected_yield", None)
            try:
                exp_yield = float(trading_api.convert_float(exp_yield)) if exp_yield is not None else None
            except Exception:
                exp_yield = None

            # для фьючерсов у позиции есть var_margin
            try:
                varm = float(trading_api.convert_float(getattr(p, "var_margin", 0)) or 0)
            except Exception:
                varm = 0.0

            pf_positions[str(ticker)] = {
                "lots": lots,
                "avg_price": avgp,
                "last_price": last,
                "market_value": mv,
                "mtm": mtm,
                "expected_yield": exp_yield,
                "instrument_type": str(getattr(p, "instrument_type", "")).lower(),
                "lot_mult": lot_mult,
                "var_margin": varm,  # ← новое поле
            }
    except Exception as e:
        send_msg(f"(portfolio snapshot error) {e}")
    return pf_positions

def _compute_day_agg(day_utc: str):
    """
    Считаем агрегаты за день по FILLED-сделкам из карточек ордеров.
    Возвращает totals, per_symbol:
      totals = {buy_cnt, sell_cnt, buy_lots, sell_lots, gross, commission, net, margin_generated}
      per_symbol[sym] — аналогично помелочно.
    'margin_generated' — оборот short-OPEN (SELL) за день (условная маржа за сессию).
    """
    def _blank():
        return dict(buy_cnt=0, sell_cnt=0, buy_lots=0.0, sell_lots=0.0,
                    gross=0.0, commission=0.0, net=0.0, margin_generated=0.0)

    totals = _blank()
    totals["vm_futures"] = 0.0  # ← итог фактической VM за день
    per_sym = defaultdict(_blank)

    for e in _iter_day_filled_events(day_utc):
        sym, trade, side, size, price = e["symbol"], e["trade"], e["side"], float(e["size"]), float(e["price"])
        value = size * price
        fee   = value * COMMISSION_RATE

        if trade == "BUY":
            totals["buy_cnt"]  += 1
            totals["buy_lots"] += size
            totals["gross"]    -= value
            per_sym[sym]["buy_cnt"]  += 1
            per_sym[sym]["buy_lots"] += size
            per_sym[sym]["gross"]    -= value
        else:
            totals["sell_cnt"]  += 1
            totals["sell_lots"] += size
            totals["gross"]     += value
            per_sym[sym]["sell_cnt"]  += 1
            per_sym[sym]["sell_lots"] += size
            per_sym[sym]["gross"]     += value

        totals["commission"]         += fee
        per_sym[sym]["commission"]   += fee

        # "маржа" за сессию: оборот коротких OPEN-SELL
        if side == "short" and trade == "SELL" and "OPEN" in e["op_name"]:
            totals["margin_generated"]       += value
            per_sym[sym]["margin_generated"] += value

    totals["net"] = totals["gross"] - totals["commission"]
    for sym, r in per_sym.items():
        r["net"] = r["gross"] - r["commission"]

    # --- фактическая вариационная маржа за календарный день (МСК) ---
    try:
        # 1) границы дня в МСК → UTC
        MSK = datetime.timezone(datetime.timedelta(hours=3))
        d = datetime.date.fromisoformat(day_utc)
        start_msk = datetime.datetime(d.year, d.month, d.day, tzinfo=MSK)
        end_msk = start_msk + datetime.timedelta(days=1)
        start_utc = start_msk.astimezone(datetime.timezone.utc)
        end_utc = end_msk.astimezone(datetime.timezone.utc)

        vm_sum, cursor, found = 0.0, None, 0

        # Локальные хелперы (чтобы не плодить новые глобальные функции)
        def _is_vm(item) -> bool:
            # 1) по enum/числу
            try:
                ot = getattr(item, "operation_type")
                if ot in (
                        OperationType.OPERATION_TYPE_ACCRUING_VARMARGIN,
                        OperationType.OPERATION_TYPE_WRITING_OFF_VARMARGIN,
                ):
                    return True
            except Exception:
                pass
            # 2) по строковому представлению типа
            t = str(getattr(item, "operation_type", "")).upper()
            if "VARMARGIN" in t:
                return True
            # 3) по человекочитаемому названию
            name = (
                    str(getattr(item, "name", "") or "")
                    + " " + str(getattr(item, "title", "") or "")
                    + " " + str(getattr(item, "description", "") or "")
            ).upper()
            return ("VARMARGIN" in name) or ("ВАРИАЦИОНН" in name)

        def _amount(item) -> float:
            for fld in ("payment", "money", "cash"):
                val = getattr(item, fld, None)
                if val is not None:
                    try:
                        return float(trading_api.convert_float(val) or 0.0)
                    except Exception:
                        pass
            return 0.0


        totals["vm_futures"] = vm_sum
        # Диагностика в лог — поможет, если снова увидите 0
        log_operation({
            "kind": "VM_SUMMARY",
            "day_msk": day_utc,
            "period_utc": [start_utc.isoformat(), end_utc.isoformat()],
            "vm_sum": vm_sum,
            "ops_found": found
        }, op_name="VM_SUMMARY")

    except Exception as e:
        log_error({"where": "vm_fetch", "err": str(e)})

def make_session_daily_report(day_utc: str | None = None) -> str:
    day_utc = day_utc or _utc_today_str()
    totals, per_sym = _compute_day_agg(day_utc)

    header = f"📊 Отчёт за {day_utc}"
    lines = [header, "-" * len(header)]
    lines.append(f"Куплено: {totals['buy_cnt']} орд. / {totals['buy_lots']:.2f} лотов")
    lines.append(f"Продано: {totals['sell_cnt']} орд. / {totals['sell_lots']:.2f} лотов")
    lines.append(f"Денежный поток (gross): {'+' if totals['gross']>=0 else ''}{totals['gross']:.2f} ₽")
    lines.append(f"Комиссии: {totals['commission']:.2f} ₽")
    lines.append(f"Итог за день (net): {'+' if totals['net']>=0 else ''}{totals['net']:.2f} ₽")
    # новая строка: реализация VM за день по данным Operations
    lines.append(
        f"Вариационная маржа (факт, фьючерсы): {'+' if totals.get('vm_futures', 0) >= 0 else ''}{totals.get('vm_futures', 0):.2f} ₽")
    lines.append(f"Сгенерированная маржа (short OPEN): {totals['margin_generated']:.2f} ₽")

    # Портфель на сейчас
    pf = _get_portfolio_snapshot()
    total_mv  = sum(v["market_value"] for v in pf.values()) if pf else 0.0
    total_mtm = sum(v["mtm"] for v in pf.values()) if pf else 0.0
    vm_total = sum(float(v.get("var_margin") or 0.0) for v in pf.values()
                   if "fut" in (v.get("instrument_type", "")))
    lines.append(f"Вариационная маржа (оценка, фьючерсы): {'+' if vm_total >= 0 else ''}{vm_total:,.2f} ₽".replace(",", " "))

    lines.append(f"Текущая стоимость портфеля: {total_mv:,.2f} ₽".replace(",", " "))
    lines.append(f"Открытая P/L (М2М) по портфелю: {'+' if total_mtm>=0 else ''}{total_mtm:,.2f} ₽".replace(",", " "))

    # Сколько из инструментов бота в портфеле
    try:
        couples = Settings.getCouples()
        bot_syms = set(couples.keys())
    except Exception:
        bot_syms = set()
    instruments_count = len([s for s in pf.keys() if s in bot_syms and abs(pf[s]["lots"]) > 0])
    lines.append(f"Инструментов в портфеле из списка бота: {instruments_count}")

    if pf:
        lines.append("")
        lines.append("Средние цены, лоты и P/L по портфелю (на сейчас):")
        for sym in sorted(pf.keys()):
            d = pf[sym]
            avgp, last = d["avg_price"], d["last_price"]
            lots, mv, mtm = d["lots"], d["market_value"], d["mtm"]
            it = d.get("instrument_type","")
            vm_val = d.get("var_margin", 0.0) if "fut" in it else None
            vm_hint = f", VM≈{vm_val:.2f}" if vm_val is not None else ""
            lines.append(
                f"  • {sym}: avg={avgp:.4f}, last={last:.4f}, lots={lots:.2f}, "
                f"MV={mv:.2f}, MTM={mtm:+.2f}{vm_hint}"
            )

    # Детализация по инструментам
    if per_sym:
        lines.append("")
        lines.append("Детализация по инструментам:")
        for sym in sorted(per_sym.keys()):
            r = per_sym[sym]
            lines.append(
                f"  • {sym}: BUY {r['buy_cnt']}/{r['buy_lots']:.2f}, "
                f"SELL {r['sell_cnt']}/{r['sell_lots']:.2f}, "
                f"gross {r['gross']:.2f} ₽, fee {r['commission']:.2f} ₽, net {r['net']:.2f} ₽, "
                f"margin_gen {r['margin_generated']:.2f} ₽"
            )

    text = "\n".join(lines)
    return _box(text) if '_box' in globals() else text

def emit_session_daily_report_to_logs(day_utc: str | None = None):
    day_utc = day_utc or _utc_today_str()
    res = _compute_day_agg(day_utc)
    if not isinstance(res, tuple) or len(res) != 2:
        totals = dict(buy_cnt=0, sell_cnt=0, buy_lots=0.0, sell_lots=0.0, cashflow=0.0)
        per_sym = {}
    else:
        totals, per_sym = res


# --- Планировщик на 19:30 МСК (16:30 UTC) ---
_report_thread = None
_report_guard  = set()

def _report_loop(hour_utc: int, minute_utc: int):
    """
    Не блокирует главный поток. Раз в минуту проверяет время и запускает отчёт
    один раз на минуту (guard по ключу YYYY-MM-DD HH:MM).
    """
    while True:
        try:
            now = datetime.datetime.utcnow()
            key = now.strftime("%Y-%m-%d %H:%M")
            if now.hour == int(hour_utc) and now.minute == int(minute_utc):
                if key not in _report_guard:
                    _report_guard.add(key)
                    emit_session_daily_report_to_logs(day_utc=now.date().isoformat())
            # чистим guard редко
            if len(_report_guard) > 2000:
                _report_guard.clear()
            time.sleep(1)  # секунда — чтобы не промахнуться по минуте
        except Exception as e:
            try:
                send_msg(f"(report scheduler error) {e}")
            except Exception:
                pass
            time.sleep(5)

def start_daily_report_scheduler(hour_utc: int = 16, minute_utc: int = 30):
    """
    Запускает фоновой поток для ежедневного отчёта.
    Для 19:30 МСК вызываем start_daily_report_scheduler(16, 30).
    """
    global _report_thread
    if _report_thread and _report_thread.is_alive():
        return
    _report_thread = threading.Thread(target=_report_loop, args=(hour_utc, minute_utc), daemon=True)
    _report_thread.start()


# ==== Clearing хэлперы (post-clearing snapshot & reconciliation) ====
CLEARING_DIR = "data/clearing"
os.makedirs(CLEARING_DIR, exist_ok=True)

def clearing_snapshot_broker_orders(ticker: str, symbol: str, phase: str = "post_clearing"):
    ts_iso = datetime.datetime.utcnow().isoformat() + "Z"
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(CLEARING_DIR, f"{symbol}_{phase}_{ts}.json")
    try:
        active = trading_api.get_orders(ticker)
    except Exception as e:
        send_msg(f"{symbol}: не удалось получить активные заявки для слепка: {e}")
        active = []
    snap = {"ts": ts_iso, "symbol": symbol, "phase": phase, "orders": active}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    send_msg(f"{symbol}: слепок заявок сохранён {path} (count={len(active)})")
    return active

def _levels_signature_from_settings(symbol: str, settings: dict, decimals: int | None = None):
    if decimals is None:
        try: decimals = int(figi[symbol]["min_price"])
        except Exception: decimals = 2
    sig = set()
    for o in settings.get(symbol, {}).get("orders", []):
        try:
            sig.add((str(o["type"]), round(float(o["price"]), decimals), float(o["size"])))
        except Exception:
            pass
    return sig

def _levels_signature_from_active(active: list, couple: dict, symbol: str):
    try: decimals = int(figi[symbol]["min_price"])
    except Exception: decimals = 2
    def _typ(direction: int) -> str:
        side = str(couple.get("side","")).lower()
        return "open" if (side=="long" and int(direction)==1) or (side=="short" and int(direction)==2) else "tp"
    sig = set()
    for o in active or []:
        try:
            if int(o.get("order_type",1)) != 1:
                continue
            price = round(float(o["price"]), decimals)
            size  = float(o.get("lots_requested") or o.get("lots") or o.get("quantity") or 0)
            typ   = _typ(int(o["direction"]))
            sig.add((typ, price, size))
        except Exception:
            pass
    return sig

def mirror_broker_active_to_settings(active: list, couple: dict, settings: dict, symbol: str):
    settings.setdefault(symbol, {})
    settings[symbol]["orders_open"]  = {}
    settings[symbol]["orders_close"] = {}
    settings[symbol]["orders"]       = []
    def _push(st):
        if st["type"] == "open":
            settings[symbol]["orders_open"][str(st["order_id"])] = st
        else:
            settings[symbol]["orders_close"][str(st["order_id"])] = st
        settings[symbol]["orders"].append(st)
    side = str(couple.get("side","")).lower()
    for o in active or []:
        try:
            if int(o.get("order_type",1)) != 1:
                continue
            price = float(o["price"])
            size  = float(o.get("lots_requested") or o.get("lots") or o.get("quantity") or 0)
            dirn  = int(o["direction"])
            typ   = ("open" if (side=="long" and dirn==1) or (side=="short" and dirn==2) else "tp")
            st    = {"order_id": str(o["order_id"]), "price": price, "size": size, "type": typ}
            # защита от дублей локально
            if is_price_level_free(symbol, price, settings):
                _push(st)
        except Exception as e:
            send_msg(f"{symbol}: пропуск заявки при зеркалке: {e}")

def refill_missing_opens(symbol: str, ticker: str, couple: dict, settings: dict):
    try:
        desired = int(couple.get("quantity_orders") or couple.get("orders_count") or couple.get("grid_levels") or 0)
    except Exception:
        desired = 0
    if desired <= 0:
        return 0
    settings.setdefault(symbol, {})
    open_list = list(settings[symbol].get("orders_open", {}).values())
    if len(open_list) >= desired:
        return 0
    step_mult = float(couple.get("step_orders") or 1)
    lot_size  = int(couple.get("size") or couple.get("package") or couple.get("LOT") or 1)
    side = str(couple.get("side","")).lower()
    if not open_list:
        return 0
    if side == "long":
        anchor = min(o["price"] for o in open_list)
        def _next(p): return WithoutPrice(p, step_mult, figi[symbol])
        place = trading_api.long_limit
    else:
        anchor = max(o["price"] for o in open_list)
        def _next(p): return WithPrice(p, step_mult, figi[symbol])
        place = trading_api.short_limit
    try: decimals = int(figi[symbol]["min_price"])
    except Exception: decimals = 2
    placed = 0
    price_next = _next(anchor)
    while len(open_list) + placed < desired:
        price_cand = round(float(price_next), decimals)
        # не дублируем ни локально, ни у брокера
        if is_price_level_free_combined(ticker, price_cand, settings):
            try:
                ord_res = place(ticker, lot_size, price_cand)
                orderlog_init(str(ord_res["order_id"]), ticker, side, lot_size,
                              price_cand, figi[symbol]["step"], "OPEN:REFILL")
                o = {"order_id": ord_res["order_id"], "price": price_cand, "size": lot_size, "type": "open"}
                settings[symbol].setdefault("orders_open", {})[str(ord_res["order_id"])] = o
                settings[symbol].setdefault("orders", []).append(o)
                send_msg(f"{symbol}: дозаполнен OPEN {lot_size} @ {price_cand}")
                placed += 1
            except Exception as e:
                send_msg(f"{symbol}: не удалось дозаполнить OPEN {lot_size} @ {price_cand}: {e}")
        price_next = _next(price_next)
    Settings.saveSettings(settings)
    return placed

def handle_clearing_exit(symbol: str, ticker: str, couple: dict, settings: dict):
    """
    Вызывается сразу после выхода из клиринга.
    1) сохраняем слепок активных заявок брокера,
    2) сверяем с локальными уровнями,
    3) если отличия есть — зеркалим брокера в settings и дозаполняем недостающие OPEN,
    4) если отличий нет — ничего не меняем.
    """
    active  = clearing_snapshot_broker_orders(ticker, symbol, phase="post_clearing")
    try: decimals = int(figi[symbol]["min_price"])
    except Exception: decimals = 2
    sig_old = _levels_signature_from_settings(symbol, settings, decimals)
    sig_new = _levels_signature_from_active(active, couple, symbol)


if __name__ == '__main__':
    pass
    print(send_msg(1231231))