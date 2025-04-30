import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation

import websockets

from websockets.client import WebSocketClientProtocol
from models import TickerData
from config import (
    BYBIT_EXCHANGE_NAME,
    BYBIT_SPOT_PUBLIC_V5_ENDPOINT,
    BYBIT_ORDERBOOK_DEPTH,
    SYMBOLS_TO_TRACK,
)
try:
    from shared_data import latest_tickers, find_arbitrage_opportunities
except ImportError:
    print("Не удалось импортировать latest_tickers/find_arbitrage_opportunities из __main__")
    latest_tickers = {}
    def find_arbitrage_opportunities(): pass

logger = logging.getLogger(__name__)


async def bybit_client(symbols: list[str]):
    """Клиент WebSocket для Bybit, подключающийся к orderbook."""
    uri = BYBIT_SPOT_PUBLIC_V5_ENDPOINT
    logger.info(f"[{BYBIT_EXCHANGE_NAME}] Инициализация клиента для {uri}...")
    while True:
        try:
            logger.info(f"[{BYBIT_EXCHANGE_NAME}] Попытка подключения к {uri}...")
            async with websockets.connect(uri, ping_interval=15, ping_timeout=10) as websocket:
                logger.info(f"[{BYBIT_EXCHANGE_NAME}] WebSocket соединение установлено.")
                await subscribe_bybit(websocket, symbols)
                await handle_bybit_messages(websocket) # Эта функция будет работать, пока соединение активно

        except websockets.exceptions.ConnectionClosedOK as e:
            logger.info(f"[{BYBIT_EXCHANGE_NAME}]  WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError) as e:
            logger.error(f"[{BYBIT_EXCHANGE_NAME}] Ошибка соединения WebSocket перед переподключением: {e}")
        except Exception as e:
            logger.error(f"[{BYBIT_EXCHANGE_NAME}] Непредвиденная ошибка в client перед переподключением: {e}", exc_info=True)

        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)

async def subscribe_bybit(websocket: WebSocketClientProtocol, symbols: list[str]):
    """Подписка на orderbook.{depth} (лучшие bid/ask) для Bybit."""
    logger.info(f"[{BYBIT_EXCHANGE_NAME}] Вход в subscribe_bybit для {symbols}")
    args = [f"orderbook.{BYBIT_ORDERBOOK_DEPTH}.{symbol}" for symbol in symbols]
    subscription_message = {
        "op": "subscribe",
        "args": args,
        "req_id": f"subscribe_orderbook_{'_'.join(symbols)}",
    }
    try:
        logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Попытка отправки: {subscription_message}")
        await websocket.send(json.dumps(subscription_message))
        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Отправлена подписка: {args}")
    except websockets.exceptions.ConnectionClosed:
        logger.error(f"[{BYBIT_EXCHANGE_NAME}] Не удалось отправить подписку: соединение закрыто.")
    except Exception as e:
        logger.error(f"[{BYBIT_EXCHANGE_NAME}] Ошибка при отправке подписки: {e}", exc_info=True)
    logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Выход из subscribe_bybit")


async def handle_bybit_messages(websocket: WebSocketClientProtocol):
    """Обработка сообщений от Bybit (из orderbook)."""
    logger.info(f"[{BYBIT_EXCHANGE_NAME}] Запуск обработчика сообщений...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Получено raw сообщение: {data}")

                # Обработка пинг-понга
                if data.get("op") == "ping":
                    pong_message = {"op": "pong", "req_id": data.get("req_id")}
                    await websocket.send(json.dumps(pong_message))
                    logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Отправлен Pong")
                    continue

                # Обработка ответа на подписку
                if data.get("topic", "").startswith("orderbook.") and data.get("type") in ["snapshot", "delta"]:
                    orderbook_data = data.get("data", {})
                    symbol = orderbook_data.get("s")

                    if not symbol:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Получены данные стакана без символа: {data}")
                        continue

                    if symbol not in SYMBOLS_TO_TRACK:  # Добавим проверку
                        logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Пропуск не отслеживаемого символа: {symbol}")
                        continue

                    best_ask_list = orderbook_data.get("a", [])
                    best_bid_list = orderbook_data.get("b", [])
                    # Переменные называются best_ask_price / best_bid_price
                    best_ask_price = None
                    best_bid_price = None

                    if best_ask_list:
                        try:
                            best_ask_price = Decimal(best_ask_list[0][0])
                        except (IndexError, InvalidOperation, TypeError) as e:
                            logger.warning(
                                f"[{BYBIT_EXCHANGE_NAME}][{symbol}] Не удалось извлечь best ask: {e} из {best_ask_list}")
                    if best_bid_list:
                        try:
                            best_bid_price = Decimal(best_bid_list[0][0])
                        except (IndexError, InvalidOperation, TypeError) as e:
                            logger.warning(
                                f"[{BYBIT_EXCHANGE_NAME}][{symbol}] Не удалось извлечь best bid: {e} из {best_bid_list}")

                    # === ИСПРАВЛЕННАЯ ЛОГИКА ОБНОВЛЕНИЯ ===
                    # Получаем или создаем объект TickerData
                    symbol_data = latest_tickers[BYBIT_EXCHANGE_NAME].setdefault(symbol, None)
                    if symbol_data is None:
                        symbol_data = TickerData(exchange=BYBIT_EXCHANGE_NAME, symbol=symbol, timestamp_ms=0)
                        latest_tickers[BYBIT_EXCHANGE_NAME][symbol] = symbol_data

                    # Обновляем поля объекта
                    symbol_data.timestamp_ms = int(data.get("ts", 0))
                    symbol_data.bid_price = best_bid_price  # Имя переменной здесь best_bid_price
                    symbol_data.ask_price = best_ask_price  # Имя переменной здесь best_ask_price
                    # last_price из стакана не получаем
                    # symbol_data.last_price = None # Опционально

                    logger.debug(
                        f"[{BYBIT_EXCHANGE_NAME}] Обновлен стакан [{symbol}]: "
                        f"B:{best_bid_price} A:{best_ask_price}"  # Логируем правильные переменные
                    )

                    find_arbitrage_opportunities()
                    continue  # Сообщение обработано
                    # === КОНЕЦ ИСПРАВЛЕНИЯ ===

                else:
                    logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}")

            except json.JSONDecodeError:
                logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{BYBIT_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Соединение закрыто в handle_messages: {e}")
    except Exception as e:
        logger.error(f"[{BYBIT_EXCHANGE_NAME}] Критическая ошибка в цикле handle_bybit_messages: {e}", exc_info=True)
    finally:
        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Завершение обработчика сообщений.")
