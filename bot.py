import time
import trading_api
import misc
import Settings
from config import *
from sys import exc_info
from traceback import extract_tb
from decimal import Decimal
from misc import check_and_build_sell_grid
import datetime as _dt
from uuid import uuid4


def _now_msk():
    return _dt.datetime.utcnow() + _dt.timedelta(hours=3)

# окна (МСК)
_PRE_MID      = _dt.time(13, 57)
_RESTORE_MID  = _dt.time(14, 5)
_PRE_EVE      = _dt.time(18, 42)
_RESTORE_EVE  = _dt.time(19, 0)

# ночной перерыв MOEX: 23:55–09:01
_PRE_NIGHT        = _dt.time(23, 43)  # делаем снимок и отмены за ~2 минуты до 23:45
_NIGHT_START      = _dt.time(23, 45)
_RESTORE_MORNING  = _dt.time(9, 3)


def _order_side(o) -> str:
    """
    Приводит сторону ордера к 'BUY' или 'SELL'.
    Поддерживает side/direction/operation, числовые 1/2, а также dir=±1 и короткие B/S.
    """
    s = o.get("side") or o.get("direction") or o.get("operation")
    if s is not None:
        # числовые коды Tinkoff: 1 = BUY, 2 = SELL
        try:
            v = int(s)
            if v == 1:
                return "BUY"
            if v == 2:
                return "SELL"
        except Exception:
            pass
        s = str(s).upper()
        if s in ("B", "BUY", "PURCHASE", "BUY_LIMIT", "BUY_MARKET"):
            return "BUY"
        if s in ("S", "SELL", "SELL_LIMIT", "SELL_MARKET"):
            return "SELL"
    d = o.get("dir")
    if d is not None:
        try:
            v = int(d)
            return "BUY" if v > 0 else ("SELL" if v < 0 else "")
        except Exception:
            return ""
    return ""


def _broker_pending_open(symbol: str, side: str, ticker: str = None) -> int:
    tkr = ticker or symbol
    try:
        open_orders = trading_api.get_orders(tkr) or []
    except Exception:
        open_orders = []

    want_side = "BUY" if (side or "").lower() == "long" else "SELL"

    def _remaining_lots(o):
        try:
            lr = o.get("lots_requested", None)
            if lr is not None:
                le = o.get("lots_executed", 0)
                return max(0, int(float(lr)) - int(float(le or 0)))
        except Exception:
            pass
        v = o.get("lots") or o.get("size") or o.get("qty") or o.get("quantity") or 0
        try:
            return int(float(v))
        except Exception:
            return 0

    def _is_same_ticker(o):
        for k in ("ticker", "symbol", "instrument", "figi"):
            if k in o and o[k]:
                return str(o[k]) in {tkr, symbol}
        return True

    total = 0
    for o in open_orders:
        try:
            typ = (str(o.get("type", "LIMIT")) or "").upper()
            if any(x in typ for x in ("STOP", "SL", "TP", "TRAIL")):
                continue
            if not _is_same_ticker(o):
                continue
            if _order_side(o) != want_side:
                continue
            total += _remaining_lots(o)
        except Exception:
            pass
    return max(0, total)


def _broker_held_ss(symbol: str, side: str, ticker: str = None) -> int:
    """Фактическая позиция по стороне стратегии, из брокера."""
    key = ticker or symbol
    qty = 0
    try:
        get_qty = (getattr(trading_api, 'get_portfolio_position_qty', None)
                   or getattr(trading_api, 'get_position_lots', None)
                   or getattr(trading_api, 'get_position_qty', None)
                   or getattr(trading_api, 'portfolio_position_qty', None))
        if get_qty:
            q = get_qty(key)  # ВАЖНО: используем реальный тикер
            if q is not None:
                qty = int(float(q))

        # запасной путь, если позиция не нашлась выше
        if qty == 0:
            get_bal = getattr(trading_api, 'balance_ticker', None)
            if get_bal:
                bal = get_bal(key)
                # balance_ticker у нас возвращает число (лоты)
                try:
                    qty = int(float(bal))
                except Exception:
                    # на всякий — поддержим dict-формат
                    if isinstance(bal, dict):
                        for k in ("lots", "qty", "quantity"):
                            if k in bal:
                                qty = int(float(bal[k]))
                                break
    except Exception:
        qty = 0

    side = (side or "").lower()
    return max(0, qty) if side == "long" else max(0, -qty)



def _cut_far_open_to_limit(symbol: str, couple: dict, limit: int):
    """
    Если held_ss + pending_open_broker > limit — снимаем «дальние от рынка» OPEN
    до укладки в лимит. Основано ТОЛЬКО на данных брокера.
    """
    if not (limit and limit > 0):
        return

    side = (couple.get("side") or "").lower()
    ticker = couple.get("symbol") or symbol
    held_ss = _broker_held_ss(symbol, side, ticker=ticker)
    pending = _broker_pending_open(symbol, side, ticker=ticker)
    excess = (held_ss + pending) - limit
    if excess <= 0 or pending <= 0:
        return

    # получаем живые ордера у брокера и сортируем от «самых дальних» к «ближайшим»
    try:
        orders = trading_api.get_orders(ticker)  # <-- берём по реальному тикеру
    except Exception:
        return

    want_side = "BUY" if side == "long" else "SELL"
    # для long самые дальние BUY — с самой НИЗКОЙ ценой; ближе к рынку — выше цена
    # для short самые дальние SELL — с самой ВЫСОКОЙ ценой; ближе к рынку — ниже цена
    def _key(o):
        p = float(o.get("price") or 0)
        return (p if side == "long" else -p, str(o.get("order_id")))

    # оставляем только OPEN-направление
    opens = []
    for o in orders:
        if _order_side(o) != want_side:
            continue
        typ = (str(o.get("type", "LIMIT")) or "").upper()
        if any(x in typ for x in ("STOP", "SL", "TP", "TRAIL")):
            continue
        opens.append(o)
    opens.sort(key=_key)  # от дальних к ближним

    for o in opens:
        if excess <= 0:
            break
        try:
            oid = str(o["order_id"])
            trading_api.cancel_order(oid)

            # считаем, сколько реально освободили из лимита: requested − executed
            freed = 0
            lr = o.get("lots_requested", None)
            if lr is not None:
                le = o.get("lots_executed", 0)
                freed = max(0, int(float(lr)) - int(float(le or 0)))
            else:
                freed = int(float(o.get("lots") or o.get("size") or o.get("qty") or 0))

            excess -= max(0, freed)
            misc.send_msg(f"{symbol}: снял дальний OPEN {o.get('price')} {freed} (сокращаем до лимита)")
        except Exception as e:
            misc.send_msg(f"{symbol}: не удалось снять {o.get('order_id')}: {e}")


def _headroom_from_broker(symbol: str, couple: dict, limit: int) -> tuple[int,int,int,int]:
    side = couple.get("side") or "long"
    ticker = couple.get("symbol") or symbol

    held_ss = _broker_held_ss(symbol, side, ticker=ticker)
    pob = _broker_pending_open(symbol, side, ticker=ticker)

    # used оставляем как held + pending — это удобно для логов/диагностики
    used = held_ss + pob

    if limit and limit > 0:
        # ОСТАТОК ДЛЯ НОВЫХ OPEN = лимит − фактическая позиция
        headroom = max(0, int(limit) - int(held_ss))
    else:
        headroom = 1_000_000_000

    return held_ss, pob, used, headroom


def _grid_target_lots(couple: dict) -> int:
    """Сколько лотов всего должна занимать сетка по настройкам (включая базовый слот)."""
    size = max(1, int(float(couple.get("size") or 1)))
    qty_orders = int(couple.get("quantity_orders") or 0)
    return size * qty_orders


def _grid_slots_for_new_layer(symbol: str, couple: dict, portfolio_limit: int) -> int:
    """
    Считает, сколько слотов новой сетки можно добавить:
    - одна сетка = size * quantity_orders лотов
    - не выходим за лимит портфеля
    """
    size = max(1, int(float(couple.get("size") or 1)))
    grid_lots = _grid_target_lots(couple)  # размер ОДНОЙ сетки в лотах

    # фактическая позиция по стороне стратегии
    held_ss, _, _, _ = _headroom_from_broker(symbol, couple, portfolio_limit)

    if portfolio_limit and portfolio_limit > 0:
        # максимум лотов, которые вообще ещё можно добавить
        max_new_lots = max(0, int(portfolio_limit) - int(held_ss))
    else:
        # портфельный лимит не задан -> хотя бы одну сетку разрешаем
        max_new_lots = grid_lots

    # новая сетка не может быть больше, чем одна "полная" сетка
    new_grid_lots = min(grid_lots, max_new_lots)

    return max(0, int(new_grid_lots // size))


def _grid_cap_slots_to_add(symbol: str, couple: dict, limit: int) -> int:
    size = max(1, int(float(couple.get("size") or 1)))
    held_ss, _, _, _ = _headroom_from_broker(symbol, couple, limit)

    # «крыша»: портфельный лимит, иначе — размер сетки по конфигу
    cap_lots = int(limit) if (limit and limit > 0) else _grid_target_lots(couple)

    deficit_lots = max(0, cap_lots - int(held_ss))  # ключевая правка: минус ТОЛЬКО held
    return int(deficit_lots // size)


def tp_sell_grid_if_avg_lower(symbol: str, couple: dict):
    """
        Условие: если средняя цена позиции в портфеле ниже текущей цены рынка на заданный порог,
        построить SELL-сетку ВВЕРХ (лимитные ордера на продажу выше рынка) и продать текущее количество
        позиции, разбив на пакеты, используя текущий шаг сетки и размер пакета.
        Пустой порог (нет/пустая строка/некорректное значение) — SELL-сетка не строится.
        """
    # --- 1) avg и last из trading_api (берём по существующим именам)
    get_avg = (getattr(trading_api, 'get_portfolio_avg_price', None)
               or getattr(trading_api, 'get_avg_price_from_portfolio', None)
               or getattr(trading_api, 'portfolio_avg_price', None))
    get_last = (getattr(trading_api, 'get_last_price', None)
                or getattr(trading_api, 'get_last', None)
                or getattr(trading_api, 'last_price', None))
    if not get_avg or not get_last:
        misc.log(f"{symbol} TP: нет функций avg/last -> skip"); return

    avg_raw = get_avg(symbol)
    if avg_raw is None:  # позиции нет — нечего продавать
        return
    avg  = avg_raw  if isinstance(avg_raw,  Decimal) else Decimal(str(avg_raw))
    last_raw = get_last(symbol)
    if last_raw is None:
        return
    last = last_raw if isinstance(last_raw, Decimal) else Decimal(str(last_raw))

    # --- 2) триггер из пары; пусто/некорректно = SELL-grid отключен
    raw_trigger = couple.get("sell_trigger", None)

    # нет ключа или пустая строка → не строим
    if raw_trigger in (None, ""):
        misc.log(f"{symbol} TP check: порог не задан -> skip")
        return

    # пробуем распарсить и отфильтровать неположительные значения
    try:
        trigger_diff = Decimal(str(raw_trigger))
        if trigger_diff <= 0:
            misc.log(f"{symbol} TP check: неположительный порог ({raw_trigger}) -> skip")
            return
    except Exception:
        misc.log(f"{symbol} TP check: некорректный порог '{raw_trigger}' -> skip")
        return

    # если разница меньше порога — не строим
    if (last - avg) < trigger_diff:
        misc.log(f"{symbol} TP check: avg={avg} last={last} diff={(last - avg)} < {trigger_diff} -> skip")
        return

    # дальше — построение SELL-сетки...

    # --- 3) параметры сетки из couple (только существующие ключи)
    grid_step = (couple.get("grid_step") or couple.get("step") or couple.get("STEP"))
    if grid_step is None:
        misc.log(f"{symbol} TP: нет шага сетки в couple -> skip"); return
    grid_step = grid_step if isinstance(grid_step, Decimal) else Decimal(str(grid_step))

    package = (couple.get("package") or couple.get("lot") or couple.get("LOT") or couple.get("qty"))
    if package is None:
        misc.log(f"{symbol} TP: нет размера пакета в couple -> skip"); return
    package = int(package)

    levels_cfg = int(couple.get("grid_levels_sell")
                     or couple.get("grid_levels")
                     or couple.get("levels")
                     or couple.get("orders_count")
                     or 5)

    # --- 4) количество в портфеле (что нужно продать)
    get_qty = (getattr(trading_api, 'get_portfolio_position_qty', None)
               or getattr(trading_api, 'get_position_qty', None)
               or getattr(trading_api, 'get_position_lots', None)
               or getattr(trading_api, 'portfolio_position_qty', None))
    if not get_qty:
        misc.log(f"{symbol} TP: нет функции получения qty позиции -> skip"); return
    qty_raw = get_qty(symbol)
    if qty_raw is None or int(qty_raw) <= 0:
        misc.log(f"{symbol} TP: qty пусто -> skip"); return
    qty = int(qty_raw)

    # --- 5) разбиение qty на пакеты
    full_packs = qty // package
    remainder  = qty %  package
    total_packs_needed = full_packs + (1 if remainder else 0)
    packs_to_place = min(levels_cfg, total_packs_needed)
    if packs_to_place == 0:
        misc.log(f"{symbol} TP: packs_to_place=0 -> skip"); return

    # --- 6) нормализация цены и отправка заявок short_limit
    normalize = (getattr(trading_api, '_quantize_price_for_symbol', None)
                 or getattr(trading_api, 'normalize_price', None))
    def _norm(p: Decimal) -> Decimal:
        return normalize(symbol, p) if normalize else p

    short_limit_fn = (getattr(trading_api, 'short_limit', None)
                      or getattr(trading_api, 'place_short_limit', None))
    if not short_limit_fn:
        misc.log(f"{symbol} TP: нет short_limit -> skip"); return

    misc.log(f"{symbol} TP trigger: avg={avg} last={last} diff={(last-avg)} >= {trigger_diff}; "
             f"qty={qty}, package={package}, full={full_packs}, rem={remainder}, "
             f"levels={levels_cfg} -> place {packs_to_place} пакетов")

    # цены: last + 1*step, last + 2*step, ...
    for i in range(1, packs_to_place + 1):
        raw_price = last + grid_step * i
        price = _norm(raw_price)

        # Лоты на уровне i:
        if total_packs_needed <= packs_to_place:
            # Укладываем все пакеты: 14*package + остаток (если есть)
            lots_i = package if i < total_packs_needed else (remainder if remainder else package)
        else:
            # Уровней меньше, чем нужно — все по полному пакету
            lots_i = package

        ticker = couple["symbol"]
        short_limit_fn(ticker, int(lots_i), float(price))

    misc.log(f"{symbol} TP: выставлено {packs_to_place} SELL-пакетов ↑")


# Получаем информацию о символах (тикерах) и их параметрах с API Tinkoff
figi = trading_api.get_figi()  # Функция из trading_api, которая запрашивает информацию о всех доступных инструментах
# print(figi)  # Выводим данные о символах (например, "SBER", "USD000UTSTOM") для отладки

def start_bot():
    """
    Главная функция бота. Здесь осуществляется управление торговлей для каждой пары символов.
    Бот отслеживает ордера, их статусы и цены, а также обновляет статус торговли для каждого символа.
    """
    settings = Settings.getSettings()  # Загружаем настройки бота из файла с настройками. Используется функция из модуля Settings, который читает настройки из 'data/settings.txt'

    def _qprice(symbol: str, p: float) -> float:
        return misc.ToPriceStep(p, figi[symbol]["step"])

    def _cid(symbol: str, typ: str, price: float, size: int, today_iso: str) -> str:
        # брокеру нужен UUID; всё остальное логируем в своих журналах
        return str(uuid4())

    def _reseed_open_grid_after_gap_down_long(symbol: str, ticker: str, couple: dict, settings: dict, figi_map: dict,
                                              px_now: float) -> int:
        """
        После гэпа вниз для long: не восстанавливаем маркетабельные OPEN, а строим новую BUY-сетку ниже рынка.
        Возвращает кол-во выставленных OPEN.
        """
        side = (couple.get("side") or "").lower()
        if side != "long" or px_now is None:
            return 0

        limit_val = int((couple.get("portfolio_limit") or 0) or (settings[symbol].get("portfolio_limit") or 0))
        slots_to_add = _grid_cap_slots_to_add(symbol, couple, limit_val)
        if slots_to_add <= 0:
            misc.send_msg(f"{symbol}: GAP↓ reseed: GRID_CAP=0 — пропуск")
            return 0

        step_val = float(couple.get("step_orders") or 0)
        if step_val <= 0:
            misc.send_msg(f"{symbol}: GAP↓ reseed: step_orders<=0 — пропуск")
            return 0

        placed = 0
        # первый уровень строго НИЖЕ рынка, дальше — вниз лесенкой
        target = misc.WithoutPrice(px_now, step_val, figi_map[symbol])

        for _ in range(slots_to_add):
            # актуальный headroom перед каждой заявкой
            _, _, _, headroom = _headroom_from_broker(symbol, couple, limit_val)
            if headroom <= 0:
                misc.send_msg(f"{symbol}: GAP↓ reseed: headroom=0 — остановка")
                break
            size_i = min(int(float(couple["size"])), headroom)

            # гарантируем уникальность уровня
            guard = 0
            while not misc.is_price_level_free_combined(ticker, target, settings):
                guard += 1
                target = misc.WithoutPrice(target, step_val, figi_map[symbol])
                if guard > 50:  # защита от «вечного» сдвига
                    break

            # пассивный BUY ниже рынка
            ord_new = trading_api.long_limit(ticker, size_i, target)
            misc.orderlog_init(str(ord_new["order_id"]), ticker, couple["side"], size_i, target,
                               figi_map[symbol]["step"], "OPEN:GAP_DOWN_RESEED")

            o = {"order_id": str(ord_new["order_id"]), "price": float(target), "size": int(size_i), "type": "open"}
            settings[symbol].setdefault("orders_open", {})[o["order_id"]] = o
            settings[symbol].setdefault("orders", []).append(o)
            placed += 1

            # следующий уровень ещё ниже
            target = misc.WithoutPrice(target, step_val, figi_map[symbol])

        if placed > 0:
            misc.send_msg(f"{symbol}: GAP↓ reseed: выставлено {placed} OPEN ниже рынка (из cap={slots_to_add})")
        return placed


    # Функция для получения начальных настроек для каждого символа.

        # Внештатно превысили лимит — тоже очищаем OPEN
        if held_only > limit and pending_open > 0:
            cancel_order_list = sorted(
                open_map.values(),
                key=(lambda o: (-float(o["price"]), o["order_id"])) if side == "long"
                else (lambda o: (float(o["price"]), o["order_id"]))
            )
            for o in cancel_order_list:
                if str(o.get("type", "open")).lower() != "open":
                    continue
                cancel_order(o, "orders_open", symbol)


    def is_order_already_placed(price, size, orders_dict):
        for order in orders_dict.values():
            if abs(order["price"] - price) < 0.005 and order["size"] == size:
                return True
        return False

    def _sync_live_from_broker(symbol: str, couple: dict, settings: dict, period_sec: int = 30):
        st_sym = settings.setdefault(symbol, {})
        now = time.time()
        if now - float(st_sym.get("_last_sync_broker_held_ts", 0)) < period_sec:
            return
        held_only, _ = _portfolio_used_lots(symbol, couple, settings)
        st_sym["live_held_same_side"] = int(held_only)
        st_sym["_last_sync_broker_held_ts"] = now
        settings[symbol] = st_sym


    # Функция для отмены ордера
    def cancel_order(order, orders_key, symbol):
        """
        Отменяем ордер, только если он НЕ в финальном статусе.
        При ошибке отмены — НЕ удаляем локально (повторим позже).
        """
        try:
            oid = str(order["order_id"])
            st = int(trading_api.get_orders_state(oid))
            misc.send_msg(f"{symbol}: проверка статуса ордера перед отменой: {st}")

            # маппинг финальных статусов (под свои значения, если у тебя другие числа)
            status_map = {
                1: "FILLED",
                2: "REJECTED",
                3: "CANCELED",
                6: "EXPIRED",
            }

            if st in status_map:
                # фиксируем реальный финал
                misc.orderlog_finish(oid, status_map[st])
                settings[symbol][orders_key].pop(oid, None)
                settings[symbol]["orders"] = misc.del_order_list(settings[symbol]["orders"], oid)
                misc.send_msg(f"{symbol}: {oid} уже финальный — {status_map[st]} (локально снят)")
                return

            # живая заявка — пробуем отменить
            misc.send_msg(f"{symbol}: отправка запроса на отмену ордера {oid}")
            trading_api.cancel_order(oid)

            # считаем успехом и чистим локально
            misc.orderlog_event(oid, symbol, "CANCELED", "CANCEL", f"Отменён из {orders_key}")
            misc.orderlog_finish(oid, "CANCELED")
            settings[symbol][orders_key].pop(oid, None)
            settings[symbol]["orders"] = misc.del_order_list(settings[symbol]["orders"], oid)
            misc.send_msg(f"{symbol} ордер отменён {order}")

        except Exception as e:
            misc.send_msg(f"{symbol}: отмена {order.get('order_id')} не удалась: {e} — повторим позже")

    while True:
        """
        Главный цикл бота, который работает бесконечно. Он проверяет каждую пару символов и выполняет операции с ордерами.
        """
        try:
            # Загружаем пары символов из настроек
            couples = Settings.getCouples()  # Использует функцию из Settings.py для загрузки пар из 'data/couples.txt'
        except Exception as err:
            # Если произошла ошибка при загрузке, логируем её
            misc.send_msg([err, extract_tb(exc_info()[2])])

        # Перебираем все символы и выполняем торговые операции для каждого
        for symbol, couple in couples.items():
            try:
                if couple["enable"] == "ON":  # Если для символа торговля включена
                    time.sleep(3)  # Задержка перед следующим действием

                    ticker = couple["symbol"]  # Получаем тикер (символ) для торговли

                    # Если символ ещё не существует в настройках, создаём его с начальными значениями
                    if symbol not in settings.keys():
                        settings[symbol] = get_settings()
                        settings[symbol].setdefault("positions_no_tp", {})


                    # (лимит посчитаем ПОСЛЕ обработки FILLED/REOPEN ниже)
                    plimit = int(couple.get("portfolio_limit") or 0) or None
                    settings[symbol]["_plimit_left"] = None

                    now = _now_msk()
                    t = now.time();
                    today = now.date().isoformat()
                    flags = settings[symbol].setdefault("_clearing_flags", {})

                    snap_kind = None
                    if _PRE_MID <= t < _dt.time(14, 0) and flags.get("mid_snap") != today:
                        snap_kind = "mid"
                    elif _PRE_EVE <= t < _dt.time(18, 45) and flags.get("eve_snap") != today:
                        snap_kind = "eve"
                    elif _PRE_NIGHT <= t < _NIGHT_START and flags.get("night_snap") != today:
                        snap_kind = "night"

                    if snap_kind:
                        snap = {"ts": now.isoformat(), "orders": []}
                        for key in ("orders_open", "orders_close"):
                            for o in list(settings[symbol].get(key, {}).values()):
                                typ = "open" if key == "orders_open" else "tp"
                                pr = _qprice(symbol, float(o["price"]))
                                sz = int(float(o["size"]))
                                uid = str(o.get("order_id") or "")  # сохраняем исходный UID заявки брокера
                                snap["orders"].append({"type": typ, "price": pr, "size": sz, "uid": uid})

                                # отменяем пока торги идут (до клиринга)

                                try:
                                    cancel_order(o, key, symbol)
                                except Exception as e:
                                    misc.send_msg(f"{symbol}: предклиринг — отмена {o.get('order_id')} не удалась: {e}")

                        settings[symbol]["_clearing_snapshot"] = snap
                        if snap_kind == "mid":
                            flags["mid_snap"] = today
                        elif snap_kind == "eve":
                            flags["eve_snap"] = today
                        else:
                            flags["night_snap"] = today

                        Settings.saveSettings(settings)
                        misc.send_msg(
                            f"{symbol}: предклиринг({snap_kind}) — сохранено и отменено заявок: {len(snap['orders'])}")
                        continue  # в эту итерацию по символу больше ничего не делаем

                    snap = settings[symbol].get("_clearing_snapshot")
                    yday = (now - _dt.timedelta(days=1)).date().isoformat()



                        # перед циклом: уже проверяем, что статус = NormalTrading; иначе ждём и выходим из этого прохода
                        side = (couple.get("side") or "").lower()
                        restored = 0

                        # --- ГЭП: закрываем КАЖДЫЙ перепрыгнутый TP по рынку,
                        # но логируем с ИСХОДНОЙ TP-ценой уровня, чтобы REOPEN восстановился корректно
                        try:
                            px_now = float(trading_api.get_price(ticker))
                        except Exception:
                            px_now = None

                        orders_to_restore = []
                        tp_hit = []

                        # 1) сначала набираем список восстановления с учётом перепрыгнутых TP
                        if px_now is not None:
                            for o in snap["orders"]:
                                typ, pr, sz = o["type"], float(o["price"]), int(o["size"])
                                if typ == "tp":
                                    hit_long = (side == "long" and px_now >= pr)  # гэп вверх для long
                                    hit_short = (side == "short" and px_now <= pr)  # гэп вниз  для short
                                    if hit_long or hit_short:
                                        tp_hit.append((pr, sz))
                                        continue  # этот TP лимиткой НЕ восстанавливаем
                                orders_to_restore.append(o)
                        else:
                            orders_to_restore = list(snap["orders"])

                        # 2) затем — фильтр гэпа вниз для long: не восстанавливаем маркетабельные BUY
                        gapdown_skipped_open = []
                        if side == "long" and px_now is not None:
                            new_restore = []
                            for o in orders_to_restore:
                                if o["type"] == "open" and px_now <= float(o["price"]):
                                    gapdown_skipped_open.append(o)  # пропускаем эти OPEN
                                else:
                                    new_restore.append(o)
                            orders_to_restore = new_restore

                        # 3) если были пропущенные OPEN — строим новую лестницу ниже рынка
                        if gapdown_skipped_open:
                            misc.send_msg(
                                f"{symbol}: GAP↓ — пропущено {len(gapdown_skipped_open)} OPEN; перестраиваю сетку ниже рынка")
                            try:
                                _reseed_open_grid_after_gap_down_long(symbol, ticker, couple, settings, figi, px_now)
                                broker_open = trading_api.get_orders(ticker) or []
                                settings[symbol]["_last_broker_open_ids"] = [str(x["order_id"]) for x in broker_open]
                            except Exception as e:
                                misc.send_msg(f"{symbol}: GAP↓ reseed ошибка: {e}")



                        if tp_hit:
                            # закрываем от "лучшей" цены к "хуже" для наглядности в логах
                            tp_hit.sort(reverse=(side == "long"))  # long: от более высокой цены вниз

                            h
                                # ВАЖНО: сохраняем ИСХОДНУЮ TP-цену уровня (pr), а не px_now
                                misc.orderlog_init(str(r["order_id"]), ticker, couple["side"],
                                                   lots, pr, figi[symbol]["step"], kind)
                                o_mkt = {"order_id": r["order_id"], "price": pr, "size": lots, "type": "tp"}
                                settings[symbol].setdefault("orders_close", {})[str(r["order_id"])] = o_mkt
                                settings[symbol].setdefault("orders", []).append(o_mkt)

                                misc.send_msg(
                                    f"{symbol}: GAP {arrow} — закрыл по рынку {lots} лот(ов) @ {px_now} (TP {pr})")
                                restored += 1


                        for o in orders_to_restore:
                            typ, pr, sz = o["type"], float(o["price"]), int(o["size"])
                            uid = str(o.get("uid") or "")  # берём сохранённый UID
                            try:
                                if side == "long":
                                    if typ == "open":
                                        r = trading_api.long_limit(ticker, sz, pr, client_order_id=uid if uid else None)
                                        st = {"order_id": r["order_id"], "price": pr, "size": sz, "type": "open"}
                                        settings[symbol].setdefault("orders_open", {})[str(r["order_id"])] = st
                                    else:
                                        r = trading_api.short_limit(ticker, sz, pr,
                                                                    client_order_id=uid if uid else None)
                                        st = {"order_id": r["order_id"], "price": pr, "size": sz, "type": "tp"}
                                        settings[symbol].setdefault("orders_close", {})[str(r["order_id"])] = st

                                        st = {"order_id": r["order_id"], "price": pr, "size": sz, "type": "tp"}
                                        settings[symbol].setdefault("orders_close", {})[str(r["order_id"])] = st

                                settings[symbol].setdefault("orders", []).append(st)
                                restored += 1
                            except Exception as e:
                                misc.send_msg(f"{symbol}: постклиринг — ошибка восстановления {typ} {sz}@{pr}: {e}")

                        # --- ПОСЛЕ цикла восстановления: один раз обновим список у брокера и проверим портфельный лимит
                        try:
                            broker_open = trading_api.get_orders(ticker)
                            settings[symbol]["_last_broker_open_ids"] = [str(x["order_id"]) for x in broker_open]
                            ps_restore = trading_api.get_price(ticker)
                            _enforce_portfolio_limit(symbol, couple, settings, ps_restore)
                        except Exception as e:
                            misc.send_msg(f"{symbol}: post-RESTORE limit guard — {e}")




                        # отметим «восстановлено» именно для того окна, которое сработало
                        if restored > 0:
                            if flags.get("mid_snap") == today and t >= _RESTORE_MID:
                                flags["mid_restore"] = today
                            if flags.get("eve_snap") == today and t >= _RESTORE_EVE:
                                flags["eve_restore"] = today
                            if flags.get("night_snap") == yday and t >= _RESTORE_MORNING:
                                flags["morning_restore"] = today
                            # снимок больше не нужен — всё (частично) восстановили
                            settings[symbol].pop("_clearing_snapshot", None)
                        else:
                            misc.send_msg(
                                f"{symbol}: восстановление отложено — восстановлено 0 заявок, снимок сохраняем")

                        Settings.saveSettings(settings)


                    # Проверка времени последнего обновления статуса торговли
                    if time.time() - settings[symbol]["lastUpdStatusTrading"] >= 100 or settings[symbol]["lastUpdStatusTrading"] == 0:
                        settings[symbol]["lastUpdStatusTrading"] = time.time()  # Обновляем время последнего обновления

                        # Проверяем статус торговли для этого символа
                        if settings[symbol]["NormalTrading"]:
                            status, status_code = trading_api.get_status_ticker(ticker)
                            if status != "NormalTrading":
                                # 1) отменяем ТОЛЬКО свои активные заявки через уже существующий helper cancel_order


                                # 2) помечаем состояние клиринга и сбрасываем антидребезг
                                settings[symbol]["NormalTrading"] = False
                                settings[symbol]["_in_clearing"] = True
                                settings[symbol]["_resume_cooldown_till"] = 0
                                settings[symbol]["_clearing_entered_at"] = _dt.datetime.utcnow().isoformat() + "Z"

                                misc.send_msg(f"{symbol} торговля остановлена (клиринг/перерыв): {status_code}")
                                Settings.saveSettings(settings)
                                continue




                        else:

                            status, status_code = trading_api.get_status_ticker(ticker)

                            if status == "NormalTrading":
                                settings[symbol]["NormalTrading"] = True
                                settings[symbol]["_resume_cooldown_till"] = time.time() + 5
                                settings[symbol]["_in_clearing"] = False
                                Settings.saveSettings(settings)
                                misc.send_msg(f"{symbol}: торговля возобновлена {status_code}")

                                continue


                    # --- брокерская защита лимита (истина у брокера)
                    limit = int(couple.get("portfolio_limit") or 0)

                    # 1) если ушли сверх лимита — режем «дальние» OPEN (включая вручную поставленные)

                    ticker = couple.get("symbol") or symbol
                    _cut_far_open_to_limit(symbol, couple, limit)

                    # 2) свежий headroom — только лог! никаких size/ break здесь
                    held_ss, pob, used, headroom = _headroom_from_broker(symbol, couple, limit)
                    misc.send_msg(
                        f"{symbol}: лимит={limit}, held={held_ss}, pending={pob}, headroom={headroom} "
                        f"(ticker={couple.get('symbol') or symbol})"
                    )


                    # Получаем цену для тикера
                    price_s = trading_api.get_price(ticker)



                    check_and_build_sell_grid(symbol, ticker, couple, settings, price_s, figi)

                    if settings[symbol]["status"] == "OFF":
                        misc.send_msg(f"{symbol}: статус OFF, начинаем открытие позиции")

                        # Округляем цену
                        price = misc.ToPriceStep(price_s, figi[symbol]["step"])

                        req = int(float(couple["size"]))
                        held_ss, pob, used, headroom = _headroom_from_broker(symbol, couple, limit)


                        if couple["side"] == "long":  # Открываем ордер на покупку
                            misc.send_msg(f"{symbol}: отправка MARKET ордера на ПОКУПКУ (long) — объем: {size_open}")
                            order = trading_api.long_market(ticker, size_open)
                        else:  # "short"
                            misc.send_msg(f"{symbol}: отправка MARKET ордера на ПРОДАЖУ (short) — объем: {size_open}")
                            order = trading_api.short_market(ticker, size_open)

                        misc.orderlog_init(str(order["order_id"]), ticker, couple["side"], size_open, price, figi[symbol]["step"], "OPEN:MARKET")
                        o = {"order_id": order["order_id"], "price": price, "size": size_open, "type": "open"}

                        settings[symbol]["orders_open"][str(order["order_id"])] = o
                        settings[symbol]["orders"] = [o]



                        misc.send_msg(f"{symbol}: базовый ордер открыт, начинаем построение сетки")

                        price_open = price


                        for i in range(slots_to_add):
                            try:
                                # актуальный headroom перед каждой заявкой
                                _, _, _, headroom = _headroom_from_broker(symbol, couple, limit_val)
                                if headroom <= 0:
                                    misc.send_msg(f"{symbol}: headroom=0 — останавливаю построение сетки")
                                    break

                                size_i = min(int(float(couple["size"])), headroom)

                                # шаг цены
                                if (couple.get("side") or "long").lower() == "long":
                                    price_open = misc.WithoutPrice(price_open, couple["step_orders"], figi[symbol])
                                else:
                                    price_open = misc.WithPrice(price_open, couple["step_orders"], figi[symbol])

                                # запрет дубликата уровня
                                if not misc.is_price_level_free_combined(ticker, price_open, settings):
                                    misc.send_msg(f"{symbol}: пропуск {price_open} — уровень уже занят")
                                    continue

                                # размещаем лимит
                                order = (trading_api.long_limit if (couple.get("side") or "long").lower() == "long"
                                         else trading_api.short_limit)(ticker, size_i, price_open)


                                else:
                                    misc.send_msg([err, extract_tb(exc_info()[2])])
                                    raise

                        # после батча — подхватить id живых ордеров у брокера (для корректного pending в логах)
                        try:
                            broker_open = trading_api.get_orders(ticker) or []
                            settings[symbol]["_last_broker_open_ids"] = [str(x["order_id"]) for x in broker_open]
                        except Exception:
                            pass

                        settings[symbol]["status"] = "ON"


                        # 1) Текущие активные заявки у брокера


                    elif settings[symbol]["status"] == "ON":

                        # 1) Какие заявки сейчас активны у брокера

                        open_orders = trading_api.get_orders(ticker)

                        open_orders_id = [str(oo["order_id"]) for oo in open_orders]  # строго строки

                        settings[symbol]["_last_broker_open_ids"] = open_orders_id

                        # копим только-что исполненные OPEN этого прохода, чтобы единым пакетом
                        # поставить по ним TP-сетку от текущего рынка
                        just_filled_open = []

                        # 2) OPEN -> FILLED => копим исполненные; TP-сетку ставим одним пакетом ОТ ТЕКУЩЕГО РЫНКА ниже
                        for order_id, order in settings[symbol]["orders_open"].copy().items():
                            oid = str(order_id)

                            # если у брокера ордер ещё живой — пропускаем
                            if oid in open_orders_id:
                                continue

                            state = trading_api.get_orders_state(oid)

                            if state == 1:  # ===== FILLED =====
                                misc.send_msg(f"{symbol} ордер на открытие исполнен {order['price']}")
                                misc.orderlog_event(oid, ticker, "FILLED", "OPEN->FILLED",
                                                    f"Исполнен по цене {order['price']}",
                                                    extra={"fill_price": order["price"]})
                                misc.orderlog_finish(oid, "FILLED")

                                # нарастим живую позицию по стороне стратегии
                                try:
                                    st_sym = settings.setdefault(symbol, {})
                                    st_sym["live_held_same_side"] = int(st_sym.get("live_held_same_side", 0)) + int(
                                        order["size"])
                                except Exception:
                                    pass

                                # НЕ ставим TP сразу. Копим, чтобы одним блоком расставить от рынка.
                                just_filled_open.append({"size": int(order["size"])})

                                # убираем исполненный OPEN из локальных структур
                                settings[symbol]["orders_open"].pop(oid, None)
                                settings[symbol]["orders"] = misc.del_order_list(settings[symbol]["orders"], oid)

                                # метка времени FILLED — пригодится для cooldown’ов
                                settings[symbol]["_last_filled_ts"] = time.time()


                            elif state in (2, 3, 6):  # ===== REJECTED / CANCELED / EXPIRED =====
                                status_map = {2: "REJECTED", 3: "CANCELED", 6: "EXPIRED"}
                                misc.orderlog_finish(oid, status_map[state])
                                settings[symbol]["orders_open"].pop(oid, None)
                                settings[symbol]["orders"] = misc.del_order_list(settings[symbol]["orders"], oid)
                                misc.send_msg(f"{symbol}: OPEN {oid} -> {status_map[state]} (удалён локально)")

                            # иначе — ничего, ждём следующего прохода


                        # если в этом проходе были исполнены OPEN — ставим TP-сетку от текущего рынка
                        if just_filled_open:
                            try:
                                last = float(trading_api.get_price(ticker))
                            except Exception:
                                last = None

                            if last is not None:
                                side = (couple.get("side") or "").lower()

                                # --- здесь переводим TP и step_orders из "шагов" в "деньги"
                                step_size = float(figi[symbol]["step"])  # минимальный шаг цены инструмента
                                tp_val = float(couple.get("TP") or 0) * step_size
                                step_val = float(couple.get("step_orders") or 0) * step_size

                                # БАЗОВЫЙ уровень: рынок ± TP (в деньгах)
                                if side == "long":
                                    # long: продаём выше рынка
                                    base = misc.WithPrice(last, tp_val, figi[symbol])  # рынок + TP
                                    step_move = lambda p, k: misc.WithPrice(p, k * step_val, figi[symbol])

                                else:
                                    # short: выкупаем ниже рынка
                                    base = misc.WithoutPrice(last, tp_val, figi[symbol])  # рынок − TP


                                # сетка: первый TP — base, далее base ± step * i
                                for i, f in enumerate(just_filled_open):
                                    target = base if i == 0 else step_move(base, i)

                                    # если уровень занят — сдвигаемся ещё на шаги, чтобы не конфликтовать
                                    guard = 0
                                    while not misc.is_price_level_free_combined(ticker, target, settings):
                                        guard += 1


                                    ord_tp = place_tp(int(f["size"]), float(target))
                                    misc.orderlog_init(
                                        str(ord_tp["order_id"]), ticker, couple["side"],
                                        int(f["size"]), float(target), figi[symbol]["step"], "TP:MARKET_GRID"
                                    )
                                    o_tp = {
                                        "order_id": str(ord_tp["order_id"]),
                                        "price": float(target),
                                        "size": int(f["size"]),
                                        "type": "tp"
                                    }

                                    settings[symbol].setdefault("orders", []).append(o_tp)

                                misc.send_msg(
                                    f"{symbol}: TP-сетка от рынка выставлена: base={base}, шаг={step_val}, пакетов={len(just_filled_open)}"
                                )
                            else:
                                misc.send_msg(f"{symbol}: не удалось получить last для TP-сетки — пропуск")

                        # --- AUTO TOP-UP OPEN: если headroom вырос (в т.ч. из-за ручной продажи) — докладываем недостающие OPEN
                        try:
                            limit_val = int(couple.get("portfolio_limit") or 0)
                        except Exception:
                            limit_val = 0


                        to_place = max(0, slots_needed - cur_open_slots)

                        if to_place > 0:
                            side = (couple.get("side") or "long").lower()
                            size_lot = max(1, int(float(couple.get("size") or 1)))

                            opens = list(settings[symbol].get("orders_open", {}).values())
                            closes = list(settings[symbol].get("orders_close", {}).values())

                            if side == "long":
                                # стартовая база: ниже самой дальней нашей лимитки; если нет — от самого низкого TP − TP; иначе — от рынка вниз
                                if opens:
                                    base = min(float(o["price"]) for o in opens)
                                elif closes:
                                    base = float(misc.WithoutPrice(min(float(o["price"]) for o in closes), couple["TP"],
                                                                   figi[symbol]))

                            else:
                                # short: выше самой дальней лимитки; если нет — от самого высокого TP + TP; иначе — от рынка вверх
                                if opens:
                                    base = max(float(o["price"]) for o in opens)
                                elif closes:
                                    base = float(misc.WithPrice(max(float(o["price"]) for o in closes), couple["TP"],
                                                                figi[symbol]))
                                else:
                                    base = float(misc.WithPrice(float(price_s), couple["step_orders"], figi[symbol]))
                                step_fn = lambda p: misc.WithPrice(p, couple["step_orders"], figi[symbol])
                                place_fn = lambda lots, price: trading_api.short_limit(ticker, lots, price)

                            placed = 0
                            for _ in range(to_place):
                                # актуализируем headroom каждый раз
                                _, _, _, headroom_now = _headroom_from_broker(symbol, couple, limit_val)
                                if headroom_now <= 0:
                                    break
                                lots_i = min(size_lot, headroom_now)

                                target = base
                                guard = 0
                                while not misc.is_price_level_free_combined(ticker, target, settings):
                                    base = step_fn(base)
                                    target = base
                                    guard += 1
                                    if guard > 100:
                                        break
                                if guard > 100:
                                    misc.send_msg(f"{symbol}: AUTO TOP-UP — не нашли свободный уровень, остановка")
                                    break

                                ord_new = place_fn(int(lots_i), float(target))
                                misc.orderlog_init(
                                    str(ord_new["order_id"]), ticker, couple["side"],
                                    int(lots_i), float(target), figi[symbol]["step"], "OPEN:AUTOTOPUP"
                                )
                                o = {"order_id": str(ord_new["order_id"]), "price": float(target), "size": int(lots_i),
                                     "type": "open"}
                                settings[symbol].setdefault("orders_open", {})[o["order_id"]] = o
                                settings[symbol].setdefault("orders", []).append(o)
                                placed += 1

                                # следующий уровень для следующего слота
                                base = step_fn(base)

                            if placed:
                                misc.send_msg(
                                    f"{symbol}: AUTO TOP-UP — добавлено OPEN: {placed} шт (slots_needed={slots_needed}, было={cur_open_slots})")


                        # 3) TP -> FILLED => ставим новый OPEN (уровень уникален)

                        # 3) TP -> FILLED / REJECTED / CANCELED / EXPIRED
                        for order_id, order in settings[symbol]["orders_close"].copy().items():
                            oid = str(order_id)

                            # TP ещё жив у брокера — ждём
                            if oid in open_orders_id:
                                continue

                            state = trading_api.get_orders_state(oid)

                            if state == 1:  # ===== TP FILLED =====
                                misc.send_msg(f"{symbol} ордер на закрытие исполнен {order['price']}")
                                misc.orderlog_event(oid, ticker, "FILLED", "TP->FILLED",
                                                    f"TP исполнен по цене {order['price']}")
                                misc.orderlog_finish(oid, "FILLED")

                                # уменьшаем живую позицию
                                st_sym = settings.setdefault(symbol, {})
                                try:
                                    st_sym["live_held_same_side"] = max(
                                        0, int(st_sym.get("live_held_same_side", 0)) - int(order["size"])
                                    )
                                except Exception:
                                    pass

                                # считаем REOPEN-уровень относительно цены TP
                                side = (couple.get("side") or "").lower()

                                step_size = float(figi[symbol]["step"])
                                tp_val = float(couple.get("TP") or 0) * step_size  # TP в деньгах

                                if side == "long":
                                    price = misc.WithoutPrice(order["price"], tp_val, figi[symbol])
                                    place = lambda sz: trading_api.long_limit(ticker, sz, price)
                                else:
                                    price = misc.WithPrice(order["price"], tp_val, figi[symbol])
                                    place = lambda sz: trading_api.short_limit(ticker, sz, price)

                                # уровень свободен?
                                if not misc.is_price_level_free_combined(ticker, price, settings):
                                    cur = trading_api.get_price(ticker)
                                    misc.send_msg(
                                        f"{symbol}: OPEN {price} пропущен — уровень занят (REOPEN; рынок {cur})")
                                else:
                                    # headroom по брокеру
                                    held_ss, pob, used, headroom = _headroom_from_broker(symbol, couple, limit)
                                    if headroom > 0:
                                        size_reopen = min(int(order["size"]), headroom)
                                        if size_reopen > 0:
                                            ord_new = place(size_reopen)
                                            misc.orderlog_init(str(ord_new["order_id"]), ticker, couple["side"],
                                                               size_reopen, price, figi[symbol]["step"], "OPEN:LIMIT")
                                            o = {"order_id": str(ord_new["order_id"]), "price": price,
                                                 "size": size_reopen, "type": "open"}
                                            settings[symbol]["orders_open"][o["order_id"]] = o
                                            settings[symbol]["orders"].append(o)
                                            misc.send_msg(f"{symbol} выставлен ордер на открытие {price} {size_reopen}")
                                            open_orders_id.append(str(ord_new["order_id"]))  # только str!

                                # TP удаляем из локальных структур
                                settings[symbol]["orders_close"].pop(oid, None)
                                settings[symbol]["orders"] = misc.del_order_list(settings[symbol]["orders"], oid)

                            elif state in (2, 3, 6):  # ===== TP REJECTED/CANCELED/EXPIRED =====
                                status_map = {2: "REJECTED", 3: "CANCELED", 6: "EXPIRED"}
                                misc.orderlog_finish(oid, status_map[state])
                                settings[symbol]["orders_close"].pop(oid, None)
                                settings[symbol]["orders"] = misc.del_order_list(settings[symbol]["orders"], oid)
                                misc.send_msg(f"{symbol}: TP {oid} -> {status_map[state]} (удалён локально)")
                                # иначе — ничего, ждём


                        # если есть сохранённый снимок клиринга — ничего с сеткой не делаем
                        if settings[symbol].get("_clearing_snapshot"):
                            misc.send_msg(
                                f"{symbol}: клиринг — пропускаем regrid/перезапуск до восстановления по снимку")
                            continue

                        # 4) Если остались только OPEN — перезапуск сетки
                        if not settings[symbol]["orders_close"]:
                            # если только что были FILLED — даём системе стабилизироваться
                            if time.time() - settings[symbol].get("_last_filled_ts", 0) < 10:
                                misc.send_msg(f"{symbol}: FILLED недавно — пропускаем regrid в течение 10с")
                                continue

                            misc.send_msg(
                                f"{symbol}: в сетке остались только ордера на открытие — отменяем и пересоздаём")
                            cancel_only_own_orders_local(symbol, ticker)

                            # Сохраняем важные поля перед сбросом
                            _no_tp = settings[symbol].get("positions_no_tp", {})
                            _held_ss = int(settings[symbol].get("live_held_same_side", 0))

                            # Сбрасываем, но восстанавливаем сохранённое
                            settings[symbol] = get_settings()
                            settings[symbol]["positions_no_tp"] = _no_tp
                            settings[symbol]["live_held_same_side"] = _held_ss

                            misc.send_msg(
                                f"{symbol}: перезапуск сетки. Позиции без TP сохранены: {list(_no_tp.keys()) or '-'}; "
                                f"held_same_side={_held_ss}")
                            continue

                        # 5) Если остались только TP — перестраиваем сетку
                        if not settings[symbol]["orders_open"]:
                            # --- REGRID cooldown: не дёргаем перестройку слишком часто
                            now_ts = time.time()
                            next_try_at = settings[symbol].get("_regrid_next_try_at", 0)
                            if now_ts < next_try_at:
                                continue  # ждём окончание кулдауна

                            # --- REGRID gate: пробуем перестраивать только если рынок сдвинулся хотя бы на step_orders
                            price_s = trading_api.get_price(ticker)
                            move_step = float(couple.get("step_orders") or 0)
                            last_mkt = settings[symbol].get("_regrid_last_mkt")
                            if last_mkt is not None and move_step > 0:
                                if abs(float(price_s) - float(last_mkt)) < move_step:
                                    settings[symbol]["_regrid_next_try_at"] = now_ts + 10  # короткий кулдаун
                                    continue

                            # фиксируем «рынок на момент попытки»
                            settings[symbol]["_regrid_last_mkt"] = float(price_s)

                            # --- считаем, можем ли поставить ЕЩЁ ОДНУ сетку с учётом лимита портфеля
                            portfolio_lim = int(
                                (couple.get("portfolio_limit") or 0) or (settings[symbol].get("portfolio_limit") or 0)
                            )
                            slots_to_add = _grid_slots_for_new_layer(symbol, couple, portfolio_lim)
                            if slots_to_add <= 0:
                                settings[symbol]["_regrid_next_try_at"] = time.time() + 10
                                misc.send_msg(
                                    f"{symbol}: GRID_CAP=0 — перестроение пропущено (нет места для новой сетки относительно лимита портфеля)"
                                )
                                continue

                            # Базовые ордера в TP-части
                            max_order = misc.get_max_order(settings[symbol]["orders"])
                            min_order = misc.get_min_order(settings[symbol]["orders"])

                            # Универсальный расчёт цены нового OPEN:
                            #   long  -> ставим BUY ниже минимума TP на TP (фактически ниже последнего проданного)
                            #   short -> ставим SELL выше максимума TP на TP
                            side = (couple.get("side") or "").lower()

                            step_size = float(figi[symbol]["step"])
                            tp_val = float(couple.get("TP") or 0) * step_size
                            step_val = float(couple.get("step_orders") or 0) * step_size

                            if side == "long":
                                target_price = misc.WithoutPrice(min_order["price"], tp_val, figi[symbol])
                                place_ok = (price_s > target_price)
                                base_step_fn = lambda p: misc.WithoutPrice(p, step_val, figi[symbol])
                            else:
                                target_price = misc.WithPrice(max_order["price"], tp_val, figi[symbol])
                                place_ok = (price_s < target_price)
                                base_step_fn = lambda p: misc.WithPrice(p, step_val, figi[symbol])

                            # headroom по брокеру (на всякий случай ещё раз перед самой постановкой)
                            held_ss, pob, used, headroom = _headroom_from_broker(symbol, couple, portfolio_lim)
                            if headroom <= 0:
                                misc.send_msg(f"{symbol}: REGRID отменён — headroom=0")
                                settings[symbol]["_regrid_next_try_at"] = time.time() + 10
                                continue

                            size_lot = max(1, int(float(couple.get("size") or 1)))


                            if side == "long":
                                place_fn = lambda lots, price: trading_api.long_limit(
                                    ticker, lots, price, client_order_id=str(uuid4())
                                )
                            else:
                                place_fn = lambda lots, price: trading_api.short_limit(
                                    ticker, lots, price, client_order_id=str(uuid4())
                                )

                            if place_ok:
                                placed = 0
                                cur_target = float(target_price)

                                for _ in range(slots_to_add):
                                    # актуальный headroom перед каждой заявкой
                                    _, _, _, headroom_now = _headroom_from_broker(symbol, couple, portfolio_lim)
                                    if headroom_now <= 0:
                                        break

                                    lots_i = min(size_lot, headroom_now)

                                    # уровень должен быть свободен
                                    guard = 0
                                    while not misc.is_price_level_free_combined(ticker, cur_target, settings):
                                        cur_target = float(base_step_fn(cur_target))
                                        guard += 1
                                        if guard > 100:
                                            break

                                    if guard > 100:
                                        misc.send_msg(f"{symbol}: REGRID — не нашли свободный уровень, остановка")
                                        break

                                    ord_new = place_fn(int(lots_i), float(cur_target))
                                    misc.orderlog_init(
                                        str(ord_new["order_id"]), ticker, couple["side"], int(lots_i),
                                        float(cur_target), figi[symbol]["step"], "OPEN:REGRID"
                                    )
                                    o = {
                                        "order_id": ord_new["order_id"],
                                        "price": float(cur_target),
                                        "size": int(lots_i),
                                        "type": "open",
                                    }
                                    settings[symbol].setdefault("orders_open", {})[str(ord_new["order_id"])] = o
                                    settings[symbol].setdefault("orders", []).append(o)
                                    placed += 1

                                    # следующий уровень для следующего слота
                                    cur_target = float(base_step_fn(cur_target))

                                if placed == 0:
                                    settings[symbol]["_regrid_next_try_at"] = time.time() + 25
                                    settings[symbol]["_regrid_last_price"] = float(cur_target)
                                else:
                                    # успех REGRID: чистим кулдаун/маркеры
                                    settings[symbol].pop("_regrid_next_try_at", None)
                                    settings[symbol]["_regrid_last_mkt"] = float(price_s)
                            else:
                                # лимит был бы активным — ждём
                                settings[symbol]["_regrid_next_try_at"] = time.time() + 5

                    # --- ПОРТФЕЛЬНЫЙ ЛИМИТ: финальный срез ПОСЛЕ обновления состояний

                    held_ss, pob, used, headroom = _headroom_from_broker(symbol, couple, limit)
                    if limit > 0:
                        misc.send_msg(
                            f"{symbol}: лимит={limit}, held={held_ss}, pending={pob}, осталось(headroom)={headroom} "
                            f"(ticker={couple.get('symbol') or symbol})"
                        )
                        _plog(symbol, limit, used, headroom, settings, min_interval=120)



                elif couple["enable"] == "OFF":  # Если торговля отключена

                    if symbol in settings.keys():
                        # отчёт перед удалением состояния

                        no_tp = settings[symbol].get("positions_no_tp", {})

                        msg = misc.format_no_tp_report(symbol, no_tp)

                        misc.send_msg(msg)

                        misc.log_operation({

                            "symbol": symbol,

                            "kind": "STOP_REPORT",

                            "count": len(no_tp),

                            "orders": list(no_tp.keys())

                        }, op_name="STOP_REPORT")

                        # теперь можно очищать состояние символа

                        del settings[symbol]

                        Settings.saveSettings(settings)
            except Exception as err:
                # Логируем ошибку
                misc.send_msg([err, extract_tb(exc_info()[2])])
                time.sleep(60)

    misc.send_msg(f"бот остановлен!")  # Сообщение о завершении работы бота

if __name__ == '__main__':
    start_bot()  # Запуск бота