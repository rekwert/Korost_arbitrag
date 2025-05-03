# exchange_clients/binance_ws.py
import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
import data_store
import websockets
from websockets.client import WebSocketClientProtocol

from models import TickerData
from config import (
    BINANCE_EXCHANGE_NAME,
    BINANCE_SPOT_STREAM_ENDPOINT,
    SYMBOLS_TO_TRACK,
)


logger = logging.getLogger(__name__)


async def handle_binance_messages(websocket: WebSocketClientProtocol):
    """Обрабатывает сообщения от WebSocket Binance (bookTicker)."""
    logger.info(f"[{BINANCE_EXCHANGE_NAME}] Запуск обработчика сообщений...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Получено raw сообщение: {data}")

                stream_data = None
                stream_name = None
                if isinstance(data, dict) and "stream" in data and "data" in data:
                    stream_data = data.get("data", {})
                    stream_name = data.get("stream", "")
                elif isinstance(data, dict) and 's' in data and 'b' in data and 'a' in data:
                    stream_data = data
                    stream_name = stream_data.get('s','').lower() + "@bookTicker"
                else:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}")
                    continue

                if "@bookTicker" not in stream_name:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Сообщение не из bookTicker потока: {stream_name}")
                    continue

                symbol = stream_data.get("s")
                if not symbol or symbol not in SYMBOLS_TO_TRACK:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Пропуск не отслеживаемого/неполного тикера: {symbol}")
                    continue

                try:
                    bid_price = Decimal(stream_data.get("b")) if stream_data.get("b") else None
                    ask_price = Decimal(stream_data.get("a")) if stream_data.get("a") else None
                    bid_size = Decimal(stream_data.get("B")) if stream_data.get("B") else None # <-- Добавили
                    ask_size = Decimal(stream_data.get("A")) if stream_data.get("A") else None # <-- Добавили
                    timestamp_ms = int(time.time() * 1000)

                    if bid_price is None or ask_price is None:
                         logger.warning(f"[{BINANCE_EXCHANGE_NAME}][{symbol}] Отсутствуют bid/ask: {stream_data}")
                         continue

                    # Создаем объект TickerData
                    ticker_obj = TickerData(
                        exchange=BINANCE_EXCHANGE_NAME,
                        symbol=symbol,
                        timestamp_ms=timestamp_ms,
                        bid_price=bid_price,
                        ask_price=ask_price,
                        bid_size=bid_size, # <-- Добавили
                        ask_size=ask_size, # <-- Добавили
                        last_price=None
                    )

                    # Отправляем в Redis
                    await data_store.update_ticker_in_redis(ticker_obj)

                    logger.debug(
                        f"[{BINANCE_EXCHANGE_NAME}] Обновлен стакан [{symbol}]: "
                        f"B:{bid_price} A:{ask_price}"
                    )
                    continue

                except (InvalidOperation, ValueError, TypeError) as e:
                    logger.warning(
                        f"[{BINANCE_EXCHANGE_NAME}][{symbol}] "
                        f"Ошибка обработки данных bookTicker: {e} - Данные: {stream_data}"
                    )
                except Exception as e_inner:
                     logger.error(
                         f"[{BINANCE_EXCHANGE_NAME}][{symbol}] Ошибка при обработке тикера: {e_inner}",
                         exc_info=True
                     )

            except json.JSONDecodeError:
                logger.warning(f"[{BINANCE_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{BINANCE_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(
            f"[{BINANCE_EXCHANGE_NAME}] Соединение закрыто: "
            f"Код={e.code}, Причина='{e.reason}'"
        )
    except Exception as e:
        logger.error(
            f"[{BINANCE_EXCHANGE_NAME}] Критическая ошибка в цикле handle_binance_messages: {e}",
            exc_info=True
        )
    finally:
        logger.info(f"[{BINANCE_EXCHANGE_NAME}] Завершение обработчика сообщений.")


async def binance_client(symbols: list[str]):
    """Клиент WebSocket для Binance, подключающийся к bookTicker."""
    streams = "/".join([f"{symbol.lower()}@bookTicker" for symbol in symbols])
    uri = f"{BINANCE_SPOT_STREAM_ENDPOINT}/{streams}"
    logger.info(f"[{BINANCE_EXCHANGE_NAME}] Инициализация клиента для {uri}...")

    while True:
        try:
            logger.info(f"[{BINANCE_EXCHANGE_NAME}] Попытка подключения к {uri}...")
            async with websockets.connect(
                uri,
                ping_interval=30, # Пингуем реже, т.к. поток активный
                ping_timeout=20
                ) as websocket:
                logger.info(f"[{BINANCE_EXCHANGE_NAME}] WebSocket соединение установлено: {websocket.id}")
                await handle_binance_messages(websocket)

        except websockets.exceptions.ConnectionClosedOK as e:
            logger.info(f"[{BINANCE_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            logger.error(f"[{BINANCE_EXCHANGE_NAME}] Ошибка/таймаут соединения WebSocket: {e}")
        except Exception as e:
            logger.error(f"[{BINANCE_EXCHANGE_NAME}] Непредвиденная ошибка в binance_client: {e}", exc_info=True)

        logger.info(f"[{BINANCE_EXCHANGE_NAME}] Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)