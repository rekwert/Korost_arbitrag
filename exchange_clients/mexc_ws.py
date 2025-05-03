# exchange_clients/mexc_ws.py
import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation

import websockets
from websockets.client import WebSocketClientProtocol

# Локальные импорты
try:
    import data_store
    from config import (
        MEXC_EXCHANGE_NAME,
        MEXC_SPOT_PUBLIC_V3_ENDPOINT,
        SYMBOLS_TO_TRACK,
    )
    from models import TickerData
except ImportError as e:
    # Выводим более информативное сообщение, если импорт не удался
    print(f"Ошибка импорта в mexc_ws.py: {e}")
    # Простая заглушка, чтобы избежать падения при анализе кода
    class MockDataStore:
        async def update_ticker_in_redis(self, ticker):
            pass
    data_store = MockDataStore()
    TickerData = dict # Используем dict как временную заглушку для TickerData
    MEXC_EXCHANGE_NAME = "MEXC"
    SYMBOLS_TO_TRACK = []


logger = logging.getLogger(__name__)


async def subscribe_mexc(websocket: WebSocketClientProtocol, symbols: list[str]):
    """Подписка на bookTicker (spot@public.bookTicker.v3.api@SYMBOL) для MEXC."""
    logger.info(f"[{MEXC_EXCHANGE_NAME}] Вход в subscribe_mexc для {symbols}")
    params = [f"spot@public.bookTicker.v3.api@{symbol}" for symbol in symbols]
    subscription_message = {"method": "SUBSCRIPTION", "params": params}
    try:
        logger.debug(f"[{MEXC_EXCHANGE_NAME}] Попытка отправки: {subscription_message}")
        await websocket.send(json.dumps(subscription_message))
        logger.info(f"[{MEXC_EXCHANGE_NAME}] Отправлена подписка на: {params}")
    except websockets.exceptions.ConnectionClosed:
        logger.error(
            f"[{MEXC_EXCHANGE_NAME}] Не удалось отправить подписку: соединение закрыто."
        )
    except Exception as e:
        logger.error(
            f"[{MEXC_EXCHANGE_NAME}] Ошибка при отправке подписки: {e}",
            exc_info=True,
        )
    logger.debug(f"[{MEXC_EXCHANGE_NAME}] Выход из subscribe_mexc")


async def handle_mexc_messages(websocket: WebSocketClientProtocol):
    """Обрабатывает сообщения от WebSocket MEXC (bookTicker)."""
    logger.info(f"[{MEXC_EXCHANGE_NAME}] Запуск обработчика сообщений...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(
                    f"[{MEXC_EXCHANGE_NAME}] Получено raw сообщение: {data}"
                )

                if not isinstance(data, dict):
                    logger.warning(
                        f"[{MEXC_EXCHANGE_NAME}] Сообщение не словарь: {data}"
                    )
                    continue

                # 1. Обработка Ping/Pong
                if data.get("method") == "ping":
                    pong_message = {"method": "pong"}
                    await websocket.send(json.dumps(pong_message))
                    logger.debug(f"[{MEXC_EXCHANGE_NAME}] Отправлен Pong")
                    continue

                # 2. Обработка ответов на запросы
                is_response = False
                if "result" in data:
                    if data["result"] == "success":
                        logger.info(
                            f"[{MEXC_EXCHANGE_NAME}] Успешный ответ (result): {data}"
                        )
                    else:
                        logger.warning(
                            f"[{MEXC_EXCHANGE_NAME}] Ответ с ошибкой (result): {data}"
                        )
                    is_response = True
                elif "code" in data and data["code"] != 0:
                    logger.warning(
                        f"[{MEXC_EXCHANGE_NAME}] Ответ с кодом ошибки (code): {data}"
                    )
                    is_response = True
                elif "msg" in data:
                    msg_lower = str(data["msg"]).lower()
                    if (
                        "success" in msg_lower
                        or "subscribe" in msg_lower
                        or "spot@public" in msg_lower
                    ):
                        logger.info(
                            f"[{MEXC_EXCHANGE_NAME}] Подтверждение/инфо (msg): {data}"
                        )
                        is_response = True
                    else:
                        logger.warning(
                            f"[{MEXC_EXCHANGE_NAME}] Ответ с ошибкой (msg): {data}"
                        )
                        is_response = True
                if is_response:
                    continue

                # 3. Обработка данных bookTicker
                channel = data.get("c", "")
                if (
                    channel.startswith("spot@public.bookTicker.v3.api@")
                    and "d" in data
                ):
                    ticker_data_payload = data.get("d", {})
                    symbol = data.get("s")

                    if not symbol or symbol not in SYMBOLS_TO_TRACK:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"[{MEXC_EXCHANGE_NAME}] Пропуск тикера: {symbol}"
                            )
                        continue

                    try:
                        bid_price = (
                            Decimal(ticker_data_payload.get("b"))
                            if ticker_data_payload.get("b")
                            else None
                        )
                        ask_price = (
                            Decimal(ticker_data_payload.get("a"))
                            if ticker_data_payload.get("a")
                            else None
                        )
                        bid_size = (
                            Decimal(ticker_data_payload.get("B"))
                            if ticker_data_payload.get("B")
                            else None
                        )
                        ask_size = (
                            Decimal(ticker_data_payload.get("A"))
                            if ticker_data_payload.get("A")
                            else None
                        )
                        timestamp_ms = int(data.get("t", 0))

                        if bid_price is None or ask_price is None:
                            logger.warning(
                                f"[{MEXC_EXCHANGE_NAME}][{symbol}] Отсутствуют bid/ask: {ticker_data_payload}"
                            )
                            continue

                        # Создаем объект TickerData
                        ticker_obj = TickerData(
                            exchange=MEXC_EXCHANGE_NAME,
                            symbol=symbol,
                            timestamp_ms=timestamp_ms,
                            bid_price=bid_price,
                            ask_price=ask_price,
                            bid_size=bid_size,
                            ask_size=ask_size,
                            last_price=None,
                        )

                        # Отправляем в Redis
                        await data_store.update_ticker_in_redis(ticker_obj)

                        logger.debug(
                            f"[{MEXC_EXCHANGE_NAME}] Обновлен bookTicker [{symbol}]: "
                            f"B:{bid_price} A:{ask_price}"
                        )
                        continue # Важно: переходим к след. сообщению после обработки

                    except (InvalidOperation, ValueError, TypeError) as e:
                        logger.warning(
                            f"[{MEXC_EXCHANGE_NAME}][{symbol}] Ошибка обработки BBO: {e} - Tick: {ticker_data_payload}"
                        )
                    except Exception as e_inner:
                        logger.error(
                            f"[{MEXC_EXCHANGE_NAME}][{symbol}] Ошибка при обработке BBO: {e_inner}",
                            exc_info=True,
                        )

                else:
                    # Логируем необрабатываемые сообщения (не пинг, не ответ, не BBO)
                    if not ("ping" in data) and not is_response:
                        logger.debug(
                            f"[{MEXC_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}"
                        )

            except json.JSONDecodeError:
                logger.warning(
                    f"[{MEXC_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}"
                )
            except Exception as e:
                logger.error(
                    f"[{MEXC_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}",
                    exc_info=True,
                )

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(
            f"[{MEXC_EXCHANGE_NAME}] Соединение закрыто: Код={e.code}, Причина='{e.reason}'"
        )
    except Exception as e:
        logger.error(
            f"[{MEXC_EXCHANGE_NAME}] Критическая ошибка в цикле handle_mexc_messages: {e}",
            exc_info=True,
        )
    finally:
        logger.info(f"[{MEXC_EXCHANGE_NAME}] Завершение обработчика сообщений.")


async def mexc_client(symbols: list[str]):
    """Клиент WebSocket для MEXC."""
    uri = MEXC_SPOT_PUBLIC_V3_ENDPOINT
    logger.info(f"[{MEXC_EXCHANGE_NAME}] Инициализация клиента для {uri}...")
    while True:
        try:
            logger.info(f"[{MEXC_EXCHANGE_NAME}] Попытка подключения к {uri}...")
            async with websockets.connect(
                uri, ping_interval=None, ping_timeout=40
            ) as websocket:
                logger.info(
                    f"[{MEXC_EXCHANGE_NAME}] WebSocket соединение установлено: {websocket.id}"
                )
                await subscribe_mexc(websocket, symbols)
                await handle_mexc_messages(websocket)

        except websockets.exceptions.ConnectionClosedOK as e:
            logger.info(
                f"[{MEXC_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}"
            )
        except (
            websockets.exceptions.ConnectionClosedError,
            ConnectionRefusedError,
            OSError,
            asyncio.TimeoutError,
        ) as e:
            logger.error(
                f"[{MEXC_EXCHANGE_NAME}] Ошибка/таймаут соединения WebSocket: {e}"
            )
        except Exception as e:
            logger.error(
                f"[{MEXC_EXCHANGE_NAME}] Непредвиденная ошибка в mexc_client: {e}",
                exc_info=True,
            )

        logger.info(
            f"[{MEXC_EXCHANGE_NAME}] Попытка переподключения через 10 секунд..."
        )
        await asyncio.sleep(10)