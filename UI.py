from PyQt5 import QtWidgets, QtCore, QtGui
from config import *
from qUI import Ui_MainWindow
import sys
from PyQt5.QtWidgets import QTableWidgetItem, QCheckBox, QGroupBox, QLabel, QMdiArea
import time
import Settings

from sys import exc_info
from traceback import extract_tb


try:
    import trading_api
    import misc
except Exception as err:
    print(err, extract_tb(exc_info()[2]))
    print(f"Укажите ТОКЕН и перезапустите бота и панель")

#pyuic5 main.ui -o qUI.py

try:
    symbols = misc.getSymbols()
except:
    symbols = {}

couples = Settings.getCouples()
markets = ["spot", "margin"]
sides = ["long", "short"]

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):

        super(MainWindow, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.sell_trigger.setValidator(QtGui.QDoubleValidator(0.0, 1000.0, 3, self))
        self.ui.portfolio_limit.setValidator(QtGui.QIntValidator(0, 1_000_000, self))
        self.setFixedSize(self.size())

        self.ui.symbol.addItems(symbols)
        self.ui.market.addItems(markets)
        self.ui.side.addItems(sides)

        self.ui.API_KEY.setText(TOKEN)

        self.ui.saveAPI.clicked.connect(self.saveAPI)
        self.ui.add.clicked.connect(self.add)
        self.ui.on.clicked.connect(self.on)
        self.ui.off.clicked.connect(self.off)
        self.ui.close_all.clicked.connect(self.close_all)
        self.ui.symbol.activated.connect(self.act)
        self.ui.market.activated.connect(self.market_act)
        self.ui.cancel_all.clicked.connect(self.cancel_all_orders)
        self.ui.cancel_sell.clicked.connect(self.cancel_sell_orders)
        self.ui.cancel_buy.clicked.connect(self.cancel_buy_orders)
        self.ui.add.setEnabled(False)
        self.ui.on.setEnabled(False)
        self.ui.off.setEnabled(False)

        self.ui.symbol.editTextChanged.connect(self.filter_symbol)


        self.print_st()
        self.market_act()

    def filter_symbol(self):
        try:
            # text = self.ui.symbol.currentText()
            self.ui.symbol.showPopup()
        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def close_all(self):
        global couples
        try:
            symbol = self.ui.symbol.currentText()
            if symbol != "-":
                trading_api.cancel_all_orders(symbol)
                trading_api.sell_all(symbol, couples[symbol]["market"], couples[symbol]["side"])
                misc.send_msg(f"{symbol} позиция закрыта, ордера отменены!")

        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def add(self):
        global couples
        try:
            symbol = self.ui.symbol.currentText()
            if symbol != "-":

                couples[symbol] = {
                    "enable": "OFF",
                    "symbol": symbol,
                    "side": self.ui.side.currentText(),
                    "size": float(self.ui.size.text().replace(",", ".")),
                    "step_orders": float(self.ui.step_orders.text().replace(",", ".")),
                    "quantity_orders": int(self.ui.quantity_orders.text()),
                    "TP": float(self.ui.TP.text().replace(",", ".")),
                    "SL": float(self.ui.SL.text().replace(",", ".")),
                    "market": self.ui.market.currentText(),
                    # ↓ новые поля
                    "sell_trigger": self.ui.sell_trigger.text().strip().replace(",", "."),  # "" = отключено
                    "portfolio_limit": int(self.ui.portfolio_limit.text() or 0)  # 0 = без лимита
                }
                # лимит портфеля берём из portfolio_limit
                txt = (self.ui.portfolio_limit.text() or "").strip().replace(",", ".")
                if txt:
                    try:
                        val = int(float(txt))
                        couples[symbol]["portfolio_limit"] = val
                        # для обратной совместимости (старые модули могут читать max_lots)
                        couples[symbol]["max_lots"] = val
                    except Exception:
                        couples[symbol].pop("portfolio_limit", None)
                        couples[symbol].pop("max_lots", None)
                else:
                    couples[symbol].pop("portfolio_limit", None)
                    couples[symbol].pop("max_lots", None)

                Settings.saveCouples(couples)

                # Sell Grid: пусто = выключено
                txt = (self.ui.sell_trigger.text() or "").strip().replace(",", ".")
                if txt:
                    try:
                        couples[symbol]["sell_trigger"] = float(txt)
                    except Exception:
                        # некорректный ввод — считаем выключенным
                        couples[symbol].pop("sell_trigger", None)
                else:
                    couples[symbol].pop("sell_trigger", None)

                Settings.saveCouples(couples)
                self.print_st()

                self.ui.add.setEnabled(True)
                self.ui.on.setEnabled(True)
                self.ui.off.setEnabled(False)

                misc.send_msg(f"{symbol} сохранено!")


        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def on(self):
        global couples
        try:
            symbol = self.ui.symbol.currentText()

            couples[symbol]["enable"] = "ON"
            Settings.saveCouples(couples)

            self.ui.add.setEnabled(False)
            self.ui.on.setEnabled(False)
            self.ui.off.setEnabled(True)

            self.ui.status.setText("ON")

            try:
                settings = Settings.getSettings()
                del settings[symbol]
                Settings.saveSettings(settings)
            except:
                pass

            misc.send_msg(f"{symbol} включено!")

            self.print_st()


        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def off(self):
        global couples
        try:
            symbol = self.ui.symbol.currentText()

            couples[symbol]["enable"] = "OFF"
            Settings.saveCouples(couples)

            self.ui.add.setEnabled(True)
            self.ui.on.setEnabled(True)
            self.ui.off.setEnabled(False)

            self.ui.status.setText("OFF")

            try:
                settings = Settings.getSettings()
                del settings[symbol]
                Settings.saveSettings(settings)
            except:
                pass

            misc.send_msg(f"{symbol} отключено!")

            self.print_st()


        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def act(self):
        global couples
        try:
            couples = Settings.getCouples()

            symbol = self.ui.symbol.currentText()
            if symbol != "-":
                if symbol in couples.keys():
                    couple = couples[symbol]

                    status = couple["enable"]
                    self.ui.status.setText(status)
                    if status == "ON":
                        self.ui.add.setEnabled(False)
                        self.ui.on.setEnabled(False)
                        self.ui.off.setEnabled(True)
                    elif status == "OFF":
                        self.ui.add.setEnabled(True)
                        self.ui.on.setEnabled(True)
                        self.ui.off.setEnabled(False)


                    # простая и однократная инициализация полей
                    self.ui.size.setText(str(couple["size"]))
                    self.ui.step_orders.setText(str(couple["step_orders"]))
                    self.ui.quantity_orders.setText(str(couple["quantity_orders"]))
                    self.ui.TP.setText(str(couple["TP"]))
                    self.ui.SL.setText(str(couple["SL"]))

                    self.ui.market.setCurrentText(couple["market"])
                    self.market_act()
                    self.ui.side.setCurrentText(couple["side"])

                    self.ui.sell_trigger.setText("" if "sell_trigger" not in couple else str(couple["sell_trigger"]))

                    # сначала пробуем новый ключ; если его нет — читаем старый max_lots (для обратной совместимости)
                    if "portfolio_limit" in couple:
                        self.ui.portfolio_limit.setText(str(int(couple["portfolio_limit"])))
                    elif "max_lots" in couple:
                        self.ui.portfolio_limit.setText(str(int(couple["max_lots"])))
                    else:
                        self.ui.portfolio_limit.clear()


                else:
                    self.ui.status.setText("-")
                    self.ui.add.setEnabled(True)
                    self.ui.on.setEnabled(False)
                    self.ui.off.setEnabled(False)

                    self.clear_all()
                    self.market_act()

            else:
                self.ui.status.setText("-")

                self.ui.add.setEnabled(False)
                self.ui.on.setEnabled(False)
                self.ui.off.setEnabled(False)

                self.clear_all()
                self.market_act()


        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def market_act(self):
        global couples
        try:
            market = self.ui.market.currentText()
            if market == "spot":
                self.ui.side.setCurrentText("long")
                self.ui.side.setEnabled(False)
            else:
                self.ui.side.setEnabled(True)


        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def saveAPI(self):
        try:
            API = {
                "API_KEY": self.ui.API_KEY.text()
            }

            Settings.saveAPI(API)

            print(f"API ключи сохранены!")
        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def clear_all(self):
        try:
            self.ui.size.clear()
            self.ui.step_orders.clear()
            self.ui.quantity_orders.clear()
            self.ui.TP.clear()
            # self.ui.max_lots.clear()  # такого виджета нет
            self.ui.SL.clear()
            self.ui.market.setCurrentText(markets[0])
            self.ui.side.setCurrentText("long")
            self.ui.sell_trigger.clear()
            self.ui.portfolio_limit.clear()
        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def print_st(self):
        try:
            on_t = []
            off_t = []

            for symbol, couple in couples.items():
                if couple["enable"] == "ON":
                    on_t.append(symbol)
                elif couple["enable"] == "OFF":
                    off_t.append(symbol)

            self.ui.actSt.clear()
            for sym in on_t:
                self.ui.actSt.append(f'<span style="color:#1c8f0d;">{sym} - ON</span>')
            for sym in off_t:
                self.ui.actSt.append(f'<span style="color:#ff0000;">{sym} - OFF</span>')
        except Exception as err:
            print(err, extract_tb(exc_info()[2]))

    def cancel_all_orders(self):
        """Отменить все активные ордера по выбранному инструменту (без закрытия позиции)."""
        # 1) Заблокировать кнопки на время операции
        self.ui.cancel_all.setEnabled(False)
        self.ui.cancel_sell.setEnabled(False)
        try:
            symbol = self.ui.symbol.currentText()
            if symbol == "-":
                return
            orders = trading_api.get_orders(symbol)

            total, ok = 0, 0
            for o in orders:
                total += 1
                try:
                    trading_api.cancel_order(o["order_id"])
                    ok += 1
                    time.sleep(0.2)  # ваш «троттлинг»
                except Exception as e:
                    misc.send_msg(f"{symbol}: ошибка отмены {o.get('order_id')}: {e}")

            misc.send_msg(f"{symbol}: отмена всех ордеров завершена: всего {total}, успешно {ok}")

        finally:
            # 2) Всегда вернуть кнопки в активное состояние,
            #    даже если случилось исключение внутри try
            self.ui.cancel_all.setEnabled(True)
            self.ui.cancel_sell.setEnabled(True)

    def cancel_buy_orders(self):
        """Отменить только BUY-заявки по выбранному инструменту."""
        # блокируем кнопки на время операции
        self.ui.cancel_all.setEnabled(False)
        self.ui.cancel_sell.setEnabled(False)
        self.ui.cancel_buy.setEnabled(False)
        try:
            symbol = self.ui.symbol.currentText()
            if symbol == "-":
                return

            orders = trading_api.get_orders(symbol)

            # Универсально определяем BUY:
            def is_buy(o):
                # direction: 1=BUY, 2=SELL (как в cancel_sell)
                s = o.get("direction")
                try:
                    if s is not None and int(s) == 1:
                        return True
                except Exception:
                    pass
                # текстовые поля side/operation
                s = (o.get("side") or o.get("operation") or "").upper()
                if s in ("B", "BUY", "BUY_LIMIT", "BUY_MARKET", "PURCHASE"):
                    return True
                # альтернативный признак dir = +1/-1
                d = o.get("dir")
                try:
                    if d is not None and int(d) > 0:
                        return True
                except Exception:
                    pass
                return False

            buy_orders = [o for o in orders if is_buy(o)]

            total, ok = 0, 0
            for o in buy_orders:
                total += 1
                try:
                    trading_api.cancel_order(o["order_id"])
                    ok += 1
                    time.sleep(0.2)  # небольшой троттлинг
                except Exception as e:
                    misc.send_msg(f"{symbol}: ошибка отмены BUY {o.get('order_id')}: {e}")

            misc.send_msg(f"{symbol}: отмена BUY ордеров завершена: всего {total}, успешно {ok}")

        finally:
            # обязательно вернуть кнопки в активное состояние
            self.ui.cancel_all.setEnabled(True)
            self.ui.cancel_sell.setEnabled(True)
            self.ui.cancel_buy.setEnabled(True)


    def cancel_sell_orders(self):
        """Отменить только SELL-заявки (direction == 2) по выбранному инструменту."""
        self.ui.cancel_all.setEnabled(False)
        self.ui.cancel_sell.setEnabled(False)
        try:
            symbol = self.ui.symbol.currentText()
            if symbol == "-":
                return
            orders = trading_api.get_orders(symbol)
            sell_orders = [o for o in orders if int(o.get("direction", 0)) == 2]

            total, ok = 0, 0
            for o in sell_orders:
                total += 1
                try:
                    trading_api.cancel_order(o["order_id"])
                    ok += 1
                    time.sleep(0.2)
                except Exception as e:
                    misc.send_msg(f"{symbol}: ошибка отмены SELL {o.get('order_id')}: {e}")

            misc.send_msg(f"{symbol}: отмена SELL ордеров завершена: всего {total}, успешно {ok}")

        finally:
            self.ui.cancel_all.setEnabled(True)
            self.ui.cancel_sell.setEnabled(True)


def start_app():
    app = QtWidgets.QApplication([])
    application = MainWindow()

    application.show()

    sys.exit(app.exec())

if __name__ == '__main__':
    start_app()







