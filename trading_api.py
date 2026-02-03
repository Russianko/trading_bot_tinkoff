from config import *
from tinkoff.invest import Client
from tinkoff.invest.schemas import Quotation
import time
import datetime
from openapi_client import openapi
import time, uuid, random
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN
import re
from uuid import uuid4, UUID


UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

def _as_valid_uuid_or_none(x) -> str | None:
    if x is None:
        return None
    try:
        val = str(UUID(str(x)))
        return val
    except Exception:
        print(f"[post_order WARN] provided client_order_id is not a valid UUID: {x!r}")
        return None

NANOS_IN_UNIT = Decimal("1000000000")

slt = 0.1  # Время задержки между запросами к API
client_oa = openapi.api_client(TOKEN)

_TRANSIENT_CODES = {
    StatusCode.INTERNAL,
    StatusCode.UNAVAILABLE,
    StatusCode.DEADLINE_EXCEEDED,
}
_TRANSIENT_MSGS = (
    "internal",
    "network error",
    "deadline exceeded",
    "ratelimit",
    "resource_exhausted",
    "temporarily unavailable",
)


# === Цена: нормализация и конвертация в Quotation (БЕЗ плавающей точки) ===


def _quantize_price_for_symbol(symbol: str, price: float | int) -> Decimal:
    """
    Округляет price до допустимой точности инструмента (кол-во знаков после запятой),
    используя settings из figi[symbol]["min_price"].
    """
    # min_price у тебя — это КОЛИЧЕСТВО знаков после запятой (0, 2, 3 и т.п.)
    decimals = int(figi[symbol]["min_price"])
    q = Decimal("1").scaleb(-decimals)  # 0.01 для 2 знаков, 0.001 для 3 и т.д.
    return Decimal(str(price)).quantize(q, rounding=ROUND_HALF_UP)

def _price_to_quotation(symbol: str, price: float | int) -> Quotation:
    """
    Преобразует цену в Quotation для Тинькофф с гарантированным количеством нулей в nano.
    Никаких "40.3" -> только "40.30" (в nano это 300000000).
    """
    d = _quantize_price_for_symbol(symbol, price)  # Decimal уже с нужной точностью
    sign = -1 if d < 0 else 1
    d = abs(d)



def _assert_price_step_ok(symbol: str, price: float | int):
    """
    Проверяет, что цена кратна шагу и точность соответствует инструменту.
    Бросает ValueError, если не ок (лучше выявить до отправки заказа).
    """
    decimals = int(figi[symbol]["min_price"])
    step_ticks = float(figi[symbol]["step"])  # "сколько тиков отступ", у тебя это множитель

    d = _quantize_price_for_symbol(symbol, price)
    # Проверка «кратности сотым/тысячным» делается по nano: nano % 10**(9-decimals) == 0
    scale = 10 ** (9 - decimals)
    q = _price_to_quotation(symbol, float(d))
    if (q.nano % scale) != 0:
        raise ValueError(f"{symbol}: некорректная точность nano для цены {d} (decimals={decimals})")


def get_price_quotation(symbol: str, price: float | int) -> Quotation:
    """
    Гарантирует:
    - резка цены до точности инструмента (сотые/тысячные),
    - nano строго 9 знаков,
    - nano кратен нужному разряду (для сотых — 10^7).
    """
    d = _quantize_price_for_symbol(symbol, price)
    q = _price_to_quotation(symbol, float(d))
    # валидация nano (поймаем проблему сразу, не в брокере)
    _assert_price_step_ok(symbol, float(d))
    return q


# Получение идентификатора аккаунта
def get_account_id():
    with Client(TOKEN) as client:
        # Получаем список аккаунтов и берем первый
        res = client.users.get_accounts().accounts[0].id
        return str(res)


account_id = get_account_id()  # Получаем ID аккаунта
time.sleep(slt)


# Получение FIGI для различных типов инструментов (акции, облигации, валюты, фьючерсы и т.д.)
def get_figi():
    exchss = []
    with Client(TOKEN) as client:
        # Получаем инструменты с разных рынков
        exchss.append(client.instruments.bonds().instruments)
        time.sleep(slt)
        exchss.append(client.instruments.currencies().instruments)
        time.sleep(slt)
        exchss.append(client.instruments.etfs().instruments)
        time.sleep(slt)
        exchss.append(client.instruments.futures().instruments)
        time.sleep(slt)
        exchss.append(client.instruments.shares().instruments)
        time.sleep(slt)

    data = {}

    for exchs in exchss:
        for exch in exchs:
            nano = 0
            # Получаем минимальную цену инструмента, включая "нано" части
            if exch.min_price_increment.nano != 0:
                nano = float(exch.min_price_increment.nano) / 1000000000
            units = float(exch.min_price_increment.units)
            step = units + nano
            if step == 0:
                step = 1
            min_price = str(float(step)).split(".")
            if len(min_price) == 2:
                min_price = len(list(min_price[1]))
            else:
                min_price = 0
                # if exch.ticker == "SiU2":
                #     print(exch)

            # Добавляем информацию о каждом инструменте в словарь
            data[exch.ticker] = {
                'figi': exch.figi,  # Уникальный идентификатор инструмента
                'lot': exch.lot,  # Лот
                'min_price': min_price,  # Минимальная цена
                'step': step,  # Шаг цены
                'nano': str(exch.min_price_increment.nano)  # Нано-шаг
            }

    return data  # Возвращаем данные всех инструментов


figi = get_figi()  # Получаем информацию о инструментах


# Преобразование объекта Quotation в тип float
def convert_float(q) -> float:
    """Корректно переводит Tinkoff Quotation в float, не теряя ведущие нули."""
    u = int(q.units)
    n = int(q.nano)
    # У Tinkoff знак у units и nano может быть разным, поэтому берём модуль и отдельно знак.



# Получение цены с учетом минимального шага
def get_price(ticker: str) -> float:
    """
    Возвращает цену как float, собранную из Quotation корректно:
    - nano всегда паддится до 9 знаков
    - потом приводим к допустимой точности инструмента
    """
    try:


        # Плюс/минус учитываем через знак
        sign = -1 if (q.units < 0 or q.nano < 0) else 1
        units = abs(q.units)
        nano = abs(q.nano)

        # ВАЖНО: паддинг до 9 знаков
        price_str = f"{units}.{nano:09d}"
        raw = Decimal(price_str) * sign

        # округление под точность инструмента
        d = _quantize_price_for_symbol(ticker, float(raw))
        return float(d)
    except Exception as e:
        print(f"Ошибка при получении цены для {ticker}: {e}")
        return 0.0


# Формирование структуры данных для ордера
def order_const(order):
    price = 0
    try:
        price = convert_float(order.initial_security_price)  # Преобразуем цену
    except:
        pass

    return {
        "order_id": order.order_id,  # ID ордера
        "lots_requested": order.lots_requested,  # Запрашиваемое количество лотов
        "lots_executed": order.lots_executed,  # Исполненное количество лотов
        "price": price,  # Цена ордера
        "status": order.execution_report_status.value,  # Статус ордера
        "direction": order.direction.value,  # Направление ордера
        "order_type": order.order_type.value  # Тип ордера
    }


# Получение статуса торгов для тикера
def get_status_ticker(symbol):
    with Client(TOKEN) as client:
        # Получаем статус торгов для инструмента
        res = client.instruments.get_instrument_by(id=figi[symbol]["figi"], id_type=1).instrument.trading_status
        if res.value == 5:
            return "NormalTrading", str(res)  # Если торги доступны
        return "NotAvailableforTrading", str(res)  # Если торги недоступны



def _post_order_with_retry(symbol: str, quantity: int, direction: int, order_type: int,
                           price=None, max_retries: int = 5, client_order_id: str | None = None):
    # Проверка nano для лимиток
    if order_type == 1 and isinstance(price, Quotation):

        if abs(price.nano) % scale != 0:
            raise ValueError(f"{symbol}: nano={price.nano} не кратен {scale} для точности {decimals} знаков")

    # Для MARKET цена не нужна
    if order_type == 2:
        price = None

    # ==== ЕДИНОРАЗОВО вычисляем валидный idempotency-key ====
    client_uuid = _as_valid_uuid_or_none(client_order_id)
    order_id = client_uuid or str(uuid4())

    # Лог — подтвердит какой id реально уехал
    print(f"[post_order] {symbol} qty={quantity} dir={direction} type={order_type} order_id={order_id}")

    last_exc = None
    for attempt in range(max_retries):
        try:
            with Client(TOKEN) as client:
                res = client.orders.post_order(
                    figi=figi[symbol]["figi"],
                    quantity=int(quantity),
                    direction=direction,
                    order_type=order_type,
                    account_id=account_id,
                    price=price,
                    confirm_margin_trade=True,
                    order_id=order_id,   # <— используется ровно то значение, что выше
                )
            print(f"[post_order OK] order_id={order_id} status={res.execution_report_status.value}")
            return order_const(res)
        except RequestError as e:
            last_exc = e
            code = getattr(e, "code", None) or getattr(e, "status", None)
            msg  = (getattr(e, "message", "") or str(e)).lower()

            # сетевые/временные ошибки — ретраим
            if (code in _TRANSIENT_CODES) or any(s in msg for s in _TRANSIENT_MSGS):
                delay = min(2 ** attempt, 16) + random.uniform(0, 0.3)
                print(f"[post_order RETRY {attempt+1}/{max_retries}] {code or ''}: {e}. sleep {delay:.2f}s")
                time.sleep(delay)
                continue

            # лимиты — подождать и снова
            if "ratelimit" in msg or "resource_exhausted" in msg:
                delay = 1.0 + attempt * 0.5
                print(f"[post_order RATELIMIT] sleep {delay:.2f}s")
                time.sleep(delay)
                continue

            # дубль: ордер уже принят брокером, но отчёт не вернулся
            if code == StatusCode.INVALID_ARGUMENT and ("duplicate" in msg or "30057" in msg):
                try:
                    with Client(TOKEN) as client:
                        st = client.orders.get_order_state(account_id=account_id, order_id=order_id)
                    return order_const(st)  # трактуем как успешное размещение
                except Exception:
                    delay = min(2 ** attempt, 16) + random.uniform(0, 0.3)
                    time.sleep(delay)
                    continue


            # прочее — отдаем наверх
            raise
    # не получилось после всех попыток
    raise last_exc


def long_market(symbol, size, client_order_id=None):


def short_market(symbol, size, client_order_id=None):


def long_limit(symbol, size, price, client_order_id=None):
    q = get_price_quotation(symbol, price)
    return _post_order_with_retry(symbol, size, direction=1, order_type=1, price=q, client_order_id=client_order_id)

def short_limit(symbol, size, price, client_order_id=None):
    q = get_price_quotation(symbol, price)
    return _post_order_with_retry(symbol, size, direction=2, order_type=1, price=q, client_order_id=client_order_id)


# Получение списка ордеров для конкретного тикера
def get_orders(symbol):
    with Client(TOKEN) as client:
        res = client.orders.get_orders(account_id=account_id)
        orders = []
        for order in res.orders:
            if order.figi == figi[symbol]["figi"]:
                orders.append(order_const(order))  # Возвращаем ордера только для нужного тикера
        return orders


# Получение статуса ордера по его ID
def get_orders_state(order_id):
    with Client(TOKEN) as client:
        res = client.orders.get_order_state(account_id=account_id, order_id=order_id)
        return res.execution_report_status.value


# Отмена всех ордеров для конкретного тикера
def cancel_all_orders(symbol):
    orders = get_orders(symbol)
    for order in orders:
        try:
            cancel_order(order["order_id"])  # Отмена каждого ордера
            time.sleep(0.5)  # Задержка между запросами
        except:
            pass


# Отмена конкретного ордера
def cancel_order(order_id):
    with Client(TOKEN) as client:
        return client.orders.cancel_order(account_id=account_id, order_id=str(order_id))


# Получение баланса для конкретного тикера
def balance_ticker(symbol):
    with Client(TOKEN) as client:
        res = client.operations.get_portfolio(account_id=account_id)
        for r in res.positions:
            if r.figi == figi[symbol]["figi"]:
                lot = convert_float(r.quantity_lots)
                return lot

    return 0


# Продажа всех позиций по конкретному тикеру(дописать логику согласно шагов на продажу)
def sell_all(ticker, market, side):
    lots = balance_ticker(ticker)
    if (lots > 0 and side == "long") or (lots < 0 and side == "short"):
        # if market == "margin":
        #     lots /= 10
        if side == "long":
            short_market(ticker, lots)  # Если позиция long, то продаем
        elif side == "short":
            long_market(ticker, abs(lots))  # Если позиция short, то покупаем


# Установка шага цены
def set_price_step(symbol: str, price: float) -> float:
    """
    Округляет цену к ближайшему кратному шагу инструмента и
    режет до нужного количества знаков после запятой.
    """
    step = figi[symbol]["step"]
    return round(round(float(price) / step) * step, int(digits))


if __name__ == '__main__':
    pass
