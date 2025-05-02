import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation

# Third-Party Libraries
import websockets
# Импорт для тайп-хинтинга
from websockets.client import WebSocketClientProtocol

# Local Application/Library Imports
from models import TickerData
from config import (
    BINANCE_EXCHANGE_NAME,
    BINANCE_SPOT_STREAM_ENDPOINT,
    SYMBOLS_TO_TRACK, # Нужен для проверки символа
)
# Import shared state and logic
try:
    from shared_data import latest_tickers, find_arbitrage_opportunities
except ImportError:
    print("Не удалось импортировать latest_tickers/find_arbitrage_opportunities из __main__")
    latest_tickers = {}
    def find_arbitrage_opportunities(): pass

# Initialize logger for this module
logger = logging.getLogger(__name__)


# Функция subscribe_binance НЕ НУЖНА при подписке через URL, её можно удалить


async def handle_binance_messages(websocket: WebSocketClientProtocol):
    """
    Обрабатывает сообщения от WebSocket Binance (bookTicker).
    """
    logger.info(f"[{BINANCE_EXCHANGE_NAME}] Запуск обработчика сообщений...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Получено raw сообщение: {data}")

                # Binance @bookTicker не требует ping/pong и не шлет явных ответов на подписку по URL
                # Определяем, комбинированный это поток или одиночный
                stream_data = None
                stream_name = None
                if isinstance(data, dict) and "stream" in data and "data" in data: # Комбинированный поток
                    stream_data = data.get("data", {})
                    stream_name = data.get("stream", "")
                elif isinstance(data, dict) and 's' in data and 'b' in data and 'a' in data: # Одиночный поток
                    stream_data = data
                    # Восстанавливаем имя потока (не критично, но для полноты)
                    stream_name = stream_data.get('s','').lower() + "@bookTicker"
                else:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}")
                    continue

                # Проверяем, что это сообщение с данными bookTicker
                if "@bookTicker" not in stream_name:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Сообщение не из bookTicker потока: {stream_name}")
                    continue

                symbol = stream_data.get("s") # 'BTCUSDT'
                if not symbol or symbol not in SYMBOLS_TO_TRACK:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Пропуск не отслеживаемого/неполного тикера: {symbol}")
                    continue

                try:
                    # Извлекаем цены в переменные bid_price и ask_price
                    bid_price = Decimal(stream_data.get("b")) if stream_data.get("b") else None
                    ask_price = Decimal(stream_data.get("a")) if stream_data.get("a") else None
                    timestamp_ms = int(time.time() * 1000)

                    # Получаем или создаем объект TickerData
                    symbol_data = latest_tickers[BINANCE_EXCHANGE_NAME].setdefault(symbol, None)
                    if symbol_data is None:
                        symbol_data = TickerData(exchange=BINANCE_EXCHANGE_NAME, symbol=symbol, timestamp_ms=0)
                        latest_tickers[BINANCE_EXCHANGE_NAME][symbol] = symbol_data

                    # === ИСПРАВЛЕНИЕ ЗДЕСЬ ===
                    # Обновляем поля существующего объекта, используя ПРАВИЛЬНЫЕ имена переменных
                    symbol_data.timestamp_ms = timestamp_ms
                    symbol_data.bid_price = bid_price  # <-- ПРОВЕРЬ ЭТУ СТРОКУ
                    symbol_data.ask_price = ask_price  # <-- ПРОВЕРЬ ЭТУ СТРОКУ
                    symbol_data.last_price = None      # bookTicker не дает last_price

                    logger.debug(
                        f"[{BINANCE_EXCHANGE_NAME}] Обновлен стакан [{symbol}]: "
                        f"B:{bid_price} A:{ask_price}" # Используем bid_price и ask_price в логе
                    )
                    # === КОНЕЦ ИСПРАВЛЕНИЯ ===

                    # Вызываем функцию сравнения
                    find_arbitrage_opportunities()
                    continue # Сообщение обработано

                except (InvalidOperation, ValueError, TypeError) as e:
                    logger.warning(
                        f"[{BINANCE_EXCHANGE_NAME}][{symbol}] "
                        f"Не удалось обработать данные bookTicker (InvalidOperation/ValueError/TypeError): {e} - Данные: {stream_data}"
                    )

                except Exception as e:
                     logger.error(
                         f"[{BINANCE_EXCHANGE_NAME}][{symbol}] Ошибка при создании TickerData: {e}",
                         exc_info=True
                     )

            except json.JSONDecodeError:
                logger.warning(f"[{BINANCE_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{BINANCE_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(
            f"[{BINANCE_EXCHANGE_NAME}] Соединение закрыто в handle_messages: "
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
            # Используем ping_interval для поддержания соединения
            async with websockets.connect(uri, ping_interval=30, ping_timeout=20) as websocket:
                logger.info(f"[{BINANCE_EXCHANGE_NAME}] WebSocket соединение установлено.")
                await handle_binance_messages(websocket) # Эта функция будет работать, пока соединение активно

        except websockets.exceptions.ConnectionClosedOK as e: # Добавили 'as e'
            logger.info(
                f"[{BINANCE_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. "
                f"Код: {e.code}, Причина: {e.reason}"
            )
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError) as e:
            logger.error(f"[{BINANCE_EXCHANGE_NAME}] Ошибка соединения WebSocket перед переподключением: {e}")
        except Exception as e:
            logger.error(
                f"[{BINANCE_EXCHANGE_NAME}] Непредвиденная ошибка в binance_client перед переподключением: {e}",
                exc_info=True
            )

        logger.info(f"[{BINANCE_EXCHANGE_NAME}] Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)