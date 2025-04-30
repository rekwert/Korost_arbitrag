import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation
import websockets

from websockets.client import WebSocketClientProtocol

from models import TickerData
from config import (
    MEXC_EXCHANGE_NAME,
    MEXC_SPOT_PUBLIC_V3_ENDPOINT,
    SYMBOLS_TO_TRACK, # Нужен для проверки символа в handle_mexc_messages
)

try:
    from shared_data import latest_tickers, find_arbitrage_opportunities
except ImportError:
    print("Не удалось импортировать latest_tickers/find_arbitrage_opportunities из __main__")
    latest_tickers = {}
    def find_arbitrage_opportunities(): pass

logger = logging.getLogger(__name__)


async def subscribe_mexc(websocket: WebSocketClientProtocol, symbols: list[str]):
    """
    Подписка на bookTicker (spot@public.bookTicker.v3.api@SYMBOL) для MEXC.
    """  # <-- Обновили docstring
    logger.info(f"[{MEXC_EXCHANGE_NAME}] Вход в subscribe_mexc для {symbols}")
    # === Пробуем канал bookTicker ===
    params = [f"spot@public.bookTicker.v3.api@{symbol}" for symbol in symbols] # <-- ИЗМЕНИЛИ ЗДЕСЬ
    subscription_message = {"method": "SUBSCRIPTION", "params": params}
    try:
        logger.debug(f"[{MEXC_EXCHANGE_NAME}] Попытка отправки: {subscription_message}")
        await websocket.send(json.dumps(subscription_message))
        logger.info(f"[{MEXC_EXCHANGE_NAME}] Отправлена подписка на: {params}")
    except websockets.exceptions.ConnectionClosed:
        logger.error(f"[{MEXC_EXCHANGE_NAME}] Не удалось отправить подписку: соединение закрыто.")
    except Exception as e:
        logger.error(f"[{MEXC_EXCHANGE_NAME}] Ошибка при отправке подписки: {e}", exc_info=True)
    logger.debug(f"[{MEXC_EXCHANGE_NAME}] Выход из subscribe_mexc")

async def handle_mexc_messages(websocket: WebSocketClientProtocol):
    """
    Обрабатывает сообщения от WebSocket MEXC, включая подписку, пинг/понг и bookTicker.
    """
    logger.info(f"[{MEXC_EXCHANGE_NAME}] Запуск обработчика сообщений...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.info(f"[{MEXC_EXCHANGE_NAME}] ПОЛУЧЕНО СООБЩЕНИЕ: {data}")

                if not isinstance(data, dict):
                    logger.debug(f"[{MEXC_EXCHANGE_NAME}] Получено не-словарное сообщение: {data}")
                    continue

                # 1. Обработка Ping/Pong
                if data.get("method") == "ping":
                    pong_message = {"method": "pong"}
                    await websocket.send(json.dumps(pong_message))
                    logger.debug(f"[{MEXC_EXCHANGE_NAME}] Отправлен Pong")
                    continue

                # 2. Обработка ответов на запросы
                is_response = False
                # ... (логика обработки ответов остается ТАКОЙ ЖЕ, как в предыдущей версии) ...
                if "result" in data:
                    # ...
                    is_response = True
                elif "code" in data and data["code"] != 0:
                    # ...
                    is_response = True
                elif "msg" in data:
                    msg_lower = str(data["msg"]).lower()
                    if "success" in msg_lower or "subscribe" in msg_lower or "spot@public" in msg_lower:
                         logger.info(f"[{MEXC_EXCHANGE_NAME}] Подтверждение/инфо (msg): {data}")
                         is_response = True
                    else:
                         logger.warning(f"[{MEXC_EXCHANGE_NAME}] Ответ с сообщением об ошибке (msg): {data}")
                         is_response = True
                if is_response:
                    continue

                # === 3. Обработка данных bookTicker (spot@public.bookTicker.v3.api) ===
                # Документация: https://mexcdevelop.github.io/apidocs/spot_v3_en/#individual-symbol-book-ticker-streams
                # Формат: {"c": channel, "d": {"b": bid_price, "B": bid_qty, "a": ask_price, "A": ask_qty}, "s": symbol, "t": timestamp}
                channel = data.get("c", "")
                if channel.startswith("spot@public.bookTicker.v3.api@") and "d" in data:
                    ticker_data = data.get("d", {})
                    symbol = data.get("s")

                    if not symbol or symbol not in SYMBOLS_TO_TRACK:
                         logger.debug(f"[{MEXC_EXCHANGE_NAME}] Пропуск не отслеживаемого/неполного тикера: {symbol}")
                         continue

                    # Переменные называются best_ask_price / best_bid_price
                    best_ask_price = None
                    best_bid_price = None

                    try:
                        if ticker_data.get("b"):
                             best_bid_price = Decimal(ticker_data.get("b"))
                        if ticker_data.get("a"):
                             best_ask_price = Decimal(ticker_data.get("a"))
                    except (InvalidOperation, TypeError) as e:
                        logger.warning(f"[{MEXC_EXCHANGE_NAME}][{symbol}] Не удалось извлечь bid/ask: {e} из {ticker_data}")
                        continue

                    # === ИСПРАВЛЕННАЯ ЛОГИКА ОБНОВЛЕНИЯ ===
                    timestamp_ms = int(data.get("t", 0))
                    # Получаем или создаем объект TickerData
                    symbol_data = latest_tickers[MEXC_EXCHANGE_NAME].setdefault(symbol, None)
                    if symbol_data is None:
                        symbol_data = TickerData(exchange=MEXC_EXCHANGE_NAME, symbol=symbol, timestamp_ms=0)
                        latest_tickers[MEXC_EXCHANGE_NAME][symbol] = symbol_data

                    # Обновляем поля объекта
                    symbol_data.timestamp_ms = timestamp_ms
                    symbol_data.bid_price = best_bid_price # Имя переменной здесь best_bid_price
                    symbol_data.ask_price = best_ask_price # Имя переменной здесь best_ask_price
                    symbol_data.last_price = None

                    logger.debug(
                        f"[{MEXC_EXCHANGE_NAME}] Обновлен bookTicker [{symbol}]: "
                        f"B:{best_bid_price} A:{best_ask_price}" # Логируем правильные переменные
                    )

                    find_arbitrage_opportunities()
                    continue # Сообщение обработано

                # 4. Логирование необработанных сообщений
                # ... (остается как было) ...
                if not (isinstance(data, dict) and data.get("method") == "ping") \
                   and not is_response \
                   and not (channel.startswith("spot@public.bookTicker.v3.api@")): # <-- ИЗМЕНИЛИ ЗДЕСЬ
                   logger.debug(f"[{MEXC_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}")


            except json.JSONDecodeError:
                logger.warning(f"[{MEXC_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{MEXC_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(
            f"[{MEXC_EXCHANGE_NAME}] Соединение закрыто в handle_messages: "
            f"Код={e.code}, Причина='{e.reason}'"
        )
    except Exception as e:
        logger.error(
            f"[{MEXC_EXCHANGE_NAME}] Критическая ошибка в цикле handle_mexc_messages: {e}",
            exc_info=True
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
            # Увеличиваем таймаут пинга для надежности, интервал по умолчанию у сервера 30 сек
            async with websockets.connect(uri, ping_interval=None, ping_timeout=40) as websocket: # Отключаем автопинг клиента, т.к. отвечаем вручную
                logger.info(f"[{MEXC_EXCHANGE_NAME}] WebSocket соединение установлено.")
                await subscribe_mexc(websocket, symbols)
                await handle_mexc_messages(websocket)

        except websockets.exceptions.ConnectionClosedOK as e:
            logger.info(f"[{MEXC_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError) as e:
            logger.error(f"[{MEXC_EXCHANGE_NAME}] Ошибка соединения WebSocket перед переподключением: {e}")
        except Exception as e:
            logger.error(f"[{MEXC_EXCHANGE_NAME}] Непредвиденная ошибка в mexc_client перед переподключением: {e}", exc_info=True)

        logger.info(f"[{MEXC_EXCHANGE_NAME}] Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)