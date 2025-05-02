# exchange_clients/bybit_ws.py
import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation
import data_store

import websockets
from websockets.client import WebSocketClientProtocol

from models import TickerData
from config import (
    BYBIT_EXCHANGE_NAME,
    BYBIT_SPOT_PUBLIC_V5_ENDPOINT,
    BYBIT_ORDERBOOK_DEPTH,
    SYMBOLS_TO_TRACK, # Нужен для проверки символа
)

logger = logging.getLogger(__name__)

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
        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Отправлена подписка на: {args}")
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

                if not isinstance(data, dict):
                    logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Сообщение не словарь: {data}")
                    continue

                # 1. Обработка пинг-понга
                if data.get("op") == "ping":
                    pong_message = {"op": "pong", "req_id": data.get("req_id")}
                    await websocket.send(json.dumps(pong_message))
                    logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Отправлен Pong")
                    continue

                # 2. Обработка ответа на подписку
                is_response = False
                if data.get("op") == "subscribe":
                    if data.get("success"):
                        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Успешная подписка на args: {data.get('ret_msg')}")
                    else:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Ошибка подписки: {data.get('ret_msg')}")
                    is_response = True
                if is_response:
                     continue

                # 3. Обработка данных стакана (orderbook)
                if data.get("topic", "").startswith("orderbook.") and data.get("type") in ["snapshot", "delta"]:
                    orderbook_data = data.get("data", {})
                    symbol = orderbook_data.get("s")

                    if not symbol:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Получены данные стакана без символа: {data}")
                        continue

                    if symbol not in SYMBOLS_TO_TRACK:
                        if logger.isEnabledFor(logging.DEBUG):
                             logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Пропуск не отслеживаемого символа: {symbol}")
                        continue

                    best_ask_list = orderbook_data.get("a", [])
                    best_bid_list = orderbook_data.get("b", [])
                    best_ask_price = None
                    best_bid_price = None

                    try:
                        if best_ask_list: best_ask_price = Decimal(best_ask_list[0][0])
                        if best_bid_list: best_bid_price = Decimal(best_bid_list[0][0])
                    except (IndexError, InvalidOperation, TypeError) as e:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}][{symbol}] Ошибка извлечения bid/ask: {e}")
                        continue # Пропускаем это обновление, если цены не извлечь

                    if best_bid_price is None or best_ask_price is None:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}][{symbol}] Отсутствуют bid или ask после парсинга: B={best_bid_price}, A={best_ask_price}")
                        continue

                    # Создаем объект TickerData
                    ticker_obj = TickerData(
                        exchange=BYBIT_EXCHANGE_NAME,
                        symbol=symbol,
                        timestamp_ms=int(data.get("ts", 0)),
                        bid_price=best_bid_price,
                        ask_price=best_ask_price,
                        last_price=None # Стакан не дает last_price
                    )

                    # Отправляем в Redis
                    await data_store.update_ticker_in_redis(ticker_obj)

                    logger.debug(
                        f"[{BYBIT_EXCHANGE_NAME}] Обновлен стакан [{symbol}]: "
                        f"B:{best_bid_price} A:{best_ask_price}"
                    )
                    continue # Сообщение обработано

                else:
                    # Логируем только если это не ответ на подписку
                    if not is_response:
                         logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}")

            except json.JSONDecodeError:
                logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{BYBIT_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Соединение закрыто: Код={e.code}, Причина='{e.reason}'")
    except Exception as e:
        logger.error(f"[{BYBIT_EXCHANGE_NAME}] Критическая ошибка в цикле handle_bybit_messages: {e}", exc_info=True)
    finally:
        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Завершение обработчика сообщений.")


async def bybit_client(symbols: list[str]):
    """Клиент WebSocket для Bybit, подключающийся к orderbook."""
    uri = BYBIT_SPOT_PUBLIC_V5_ENDPOINT
    logger.info(f"[{BYBIT_EXCHANGE_NAME}] Инициализация клиента для {uri}...")
    while True:
        try:
            logger.info(f"[{BYBIT_EXCHANGE_NAME}] Попытка подключения к {uri}...")
            async with websockets.connect(
                uri,
                ping_interval=15, # Интервал пингов от клиента
                ping_timeout=10   # Таймаут ожидания понга
                ) as websocket:
                logger.info(f"[{BYBIT_EXCHANGE_NAME}] WebSocket соединение установлено: {websocket.id}")
                await subscribe_bybit(websocket, symbols)
                await handle_bybit_messages(websocket)

        except websockets.exceptions.ConnectionClosedOK as e:
            logger.info(f"[{BYBIT_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            logger.error(f"[{BYBIT_EXCHANGE_NAME}] Ошибка/таймаут соединения WebSocket: {e}")
        except Exception as e:
            logger.error(f"[{BYBIT_EXCHANGE_NAME}] Непредвиденная ошибка в bybit_client: {e}", exc_info=True)

        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)