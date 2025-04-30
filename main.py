# main.py
import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation

import websockets
from websockets.client import WebSocketClientProtocol

# Импортируем наши модели и конфиг
from models import TickerData
from config import (
    BYBIT_SPOT_PUBLIC_V5_ENDPOINT,
    BYBIT_ORDERBOOK_DEPTH,  # Используем глубину стакана из конфига
    BINANCE_SPOT_STREAM_ENDPOINT,
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
)

# --- Настройка Логгирования ---
# Устанавливаем базовую конфигурацию
logging.basicConfig(
    level=logging.DEBUG,  # <--- Измени INFO на DEBUG
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Получаем логгер для текущего модуля
logger = logging.getLogger(__name__)
# Устанавливаем уровень INFO конкретно для логгера websockets, чтобы не спамил DEBUG сообщениями
logging.getLogger("websockets").setLevel(logging.INFO)


# --- Глобальное хранилище последних данных ---
# Структура: {exchange_name: {symbol: TickerData}}
latest_tickers: dict[str, dict[str, TickerData]] = {
    BYBIT_EXCHANGE_NAME: {},
    BINANCE_EXCHANGE_NAME: {},
}

# --- Порог для фиксации арбитражной ситуации ---
ARBITRAGE_THRESHOLD_PCT = 0.1  # 0.1% (Можно вынести в config.py)


# === Клиент для Bybit ===

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
                if data.get("op") == "subscribe":
                    if data.get("success"):
                        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Успешная подписка на args: {data.get('ret_msg')}")
                    else:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Ошибка подписки: {data.get('ret_msg')}")
                    continue

                # Обработка данных стакана (orderbook)
                if data.get("topic", "").startswith("orderbook.") and data.get("type") in ["snapshot", "delta"]:
                    orderbook_data = data.get("data", {})
                    symbol = orderbook_data.get("s")

                    if not symbol:
                        logger.warning(f"[{BYBIT_EXCHANGE_NAME}] Получены данные стакана без символа: {data}")
                        continue

                    best_ask_list = orderbook_data.get("a", [])
                    best_bid_list = orderbook_data.get("b", [])
                    best_ask_price = None
                    best_bid_price = None

                    if best_ask_list:
                        try:
                            best_ask_price = Decimal(best_ask_list[0][0])
                        except (IndexError, InvalidOperation, TypeError) as e:
                            logger.warning(f"[{BYBIT_EXCHANGE_NAME}][{symbol}] Не удалось извлечь best ask: {e} из {best_ask_list}")
                    if best_bid_list:
                        try:
                            best_bid_price = Decimal(best_bid_list[0][0])
                        except (IndexError, InvalidOperation, TypeError) as e:
                            logger.warning(f"[{BYBIT_EXCHANGE_NAME}][{symbol}] Не удалось извлечь best bid: {e} из {best_bid_list}")

                    # Обновляем или создаем TickerData
                    current_ticker = latest_tickers[BYBIT_EXCHANGE_NAME].get(symbol)
                    if not current_ticker:
                        current_ticker = TickerData(exchange=BYBIT_EXCHANGE_NAME, symbol=symbol, timestamp_ms=0)

                    # Обновляем данные
                    current_ticker.timestamp_ms = int(data.get("ts", 0))
                    current_ticker.bid_price = best_bid_price
                    current_ticker.ask_price = best_ask_price

                    latest_tickers[BYBIT_EXCHANGE_NAME][symbol] = current_ticker
                    logger.debug(f"[{BYBIT_EXCHANGE_NAME}] Обновлен стакан [{symbol}]: B:{best_bid_price} A:{best_ask_price}")

                    # Вызываем функцию сравнения после обновления
                    find_arbitrage_opportunities()

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

        except websockets.exceptions.ConnectionClosedOK:
            logger.info(f"[{BYBIT_EXCHANGE_NAME}]  WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError) as e:
            logger.error(f"[{BYBIT_EXCHANGE_NAME}] Ошибка соединения WebSocket перед переподключением: {e}")
        except Exception as e:
            logger.error(f"[{BYBIT_EXCHANGE_NAME}] Непредвиденная ошибка в client перед переподключением: {e}", exc_info=True)

        logger.info(f"[{BYBIT_EXCHANGE_NAME}] Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)


# === Клиент для Binance ===

async def handle_binance_messages(websocket: WebSocketClientProtocol):
    """Обработка сообщений от Binance (bookTicker)."""
    logger.info(f"[{BINANCE_EXCHANGE_NAME}] Запуск обработчика сообщений...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Получено raw сообщение: {data}")

                # Binance @bookTicker не требует ping/pong и не шлет явных ответов на подписку по URL
                # Обработка данных bookTicker
                # Формат: {'u': ..., 's': 'BTCUSDT', 'b': '...', 'B': '...', 'a': '...', 'A': '...'}
                if isinstance(data, dict) and "stream" in data and "data" in data: # Комбинированный поток
                    stream_data = data.get("data", {})
                    stream_name = data.get("stream", "")
                elif isinstance(data, dict) and 's' in data and 'b' in data and 'a' in data: # Одиночный поток
                     stream_data = data
                     stream_name = stream_data.get('s','').lower() + "@bookTicker" # Восстанавливаем имя потока
                else:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Получено необрабатываемое сообщение: {data}")
                    continue


                if "@bookTicker" not in stream_name:
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Сообщение не из bookTicker потока: {stream_name}")
                    continue

                symbol = stream_data.get("s") # 'BTCUSDT'
                if not symbol or symbol not in SYMBOLS_TO_TRACK:
                    continue

                try:
                    bid_price = Decimal(stream_data.get("b")) if stream_data.get("b") else None
                    ask_price = Decimal(stream_data.get("a")) if stream_data.get("a") else None
                    # Используем локальное время, т.к. bookTicker не содержит timestamp биржи
                    timestamp_ms = int(time.time() * 1000)

                    # Обновляем или создаем TickerData
                    current_ticker = latest_tickers[BINANCE_EXCHANGE_NAME].get(symbol)
                    if not current_ticker:
                        current_ticker = TickerData(exchange=BINANCE_EXCHANGE_NAME, symbol=symbol, timestamp_ms=0)

                    current_ticker.timestamp_ms = timestamp_ms
                    current_ticker.bid_price = bid_price
                    current_ticker.ask_price = ask_price
                    # last_price не доступен в bookTicker
                    current_ticker.last_price = None

                    latest_tickers[BINANCE_EXCHANGE_NAME][symbol] = current_ticker
                    logger.debug(f"[{BINANCE_EXCHANGE_NAME}] Обновлен стакан [{symbol}]: B:{bid_price} A:{ask_price}")

                    # Вызываем функцию сравнения после обновления
                    find_arbitrage_opportunities()

                except (InvalidOperation, ValueError, TypeError) as e:
                    logger.warning(f"[{BINANCE_EXCHANGE_NAME}][{symbol}] Не удалось обработать данные bookTicker: {e} - Данные: {stream_data}")
                except Exception as e:
                     logger.error(f"[{BINANCE_EXCHANGE_NAME}][{symbol}] Ошибка при создании TickerData: {e}", exc_info=True)


            except json.JSONDecodeError:
                logger.warning(f"[{BINANCE_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{BINANCE_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(f"[{BINANCE_EXCHANGE_NAME}] Соединение закрыто в handle_messages: {e}")
    except Exception as e:
        logger.error(f"[{BINANCE_EXCHANGE_NAME}] Критическая ошибка в цикле handle_binance_messages: {e}", exc_info=True)
    finally:
        logger.info(f"[{BINANCE_EXCHANGE_NAME}] Завершение обработчика сообщений.")


async def binance_client(symbols: list[str]):
    """Клиент WebSocket для Binance, подключающийся к bookTicker."""
    # Формируем URL для комбинированного потока
    streams = "/".join([f"{symbol.lower()}@bookTicker" for symbol in symbols])
    uri = f"{BINANCE_SPOT_STREAM_ENDPOINT}/{streams}"
    logger.info(f"[{BINANCE_EXCHANGE_NAME}] Инициализация клиента для {uri}...")

    while True:
        try:
            logger.info(f"[{BINANCE_EXCHANGE_NAME}] Попытка подключения к {uri}...")
            # Binance может закрыть соединение, если нет трафика ~3 минуты, ping_interval помогает
            async with websockets.connect(uri, ping_interval=30, ping_timeout=20) as websocket:
                logger.info(f"[{BINANCE_EXCHANGE_NAME}] WebSocket соединение установлено.")
                await handle_binance_messages(websocket) # Эта функция будет работать, пока соединение активно

        except websockets.exceptions.ConnectionClosedOK:
            logger.info(f"[{BINANCE_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError) as e:
            logger.error(f"[{BINANCE_EXCHANGE_NAME}] Ошибка соединения WebSocket перед переподключением: {e}")
        except Exception as e:
            logger.error(f"[{BINANCE_EXCHANGE_NAME}] Непредвиденная ошибка в client перед переподключением: {e}", exc_info=True)

        logger.info(f"[{BINANCE_EXCHANGE_NAME}]  Попытка переподключения через 10 секунд...")
        await asyncio.sleep(10)


# === Логика поиска арбитража ===

def find_arbitrage_opportunities():
    """Ищет возможности для арбитража на основе данных в latest_tickers."""
    for symbol in SYMBOLS_TO_TRACK:
        ticker_bybit = latest_tickers.get(BYBIT_EXCHANGE_NAME, {}).get(symbol)
        ticker_binance = latest_tickers.get(BINANCE_EXCHANGE_NAME, {}).get(symbol)

        # Проверяем наличие данных от обеих бирж
        if not ticker_bybit or not ticker_binance:
            logger.debug(f"[{symbol}] Данные от одной из бирж отсутствуют.")
            continue

        # Извлекаем цены Bid/Ask
        bybit_ask = ticker_bybit.ask_price
        bybit_bid = ticker_bybit.bid_price
        binance_ask = ticker_binance.ask_price
        binance_bid = ticker_binance.bid_price

        # Проверяем наличие всех необходимых цен
        if not all([bybit_ask, bybit_bid, binance_ask, binance_bid]):
            logger.debug(f"[{symbol}] Отсутствуют Bid/Ask цены: Bybit(B:{bybit_bid}, A:{bybit_ask}), Binance(B:{binance_bid}, A:{binance_ask})")
            continue

        # Считаем потенциальный процент прибыли для двух направлений
        # Важно: Покупаем по Ask (дороже), Продаем по Bid (дешевле)

        # 1. Купить на Binance (по binance_ask), Продать на Bybit (по bybit_bid)
        profit_bin_byb_pct = ((bybit_bid - binance_ask) / binance_ask) * 100

        # 2. Купить на Bybit (по bybit_ask), Продать на Binance (по binance_bid)
        profit_byb_bin_pct = ((binance_bid - bybit_ask) / bybit_ask) * 100

        # Логируем найденные возможности, если они превышают порог
        log_prices = f"Bybit(B:{bybit_bid} A:{bybit_ask}) | Binance(B:{binance_bid} A:{binance_ask})"
        arbitrage_found = False

        if profit_bin_byb_pct >= ARBITRAGE_THRESHOLD_PCT:
            logger.info(f"[АРБИТРАЖ] [{symbol}] Купить Binance @{binance_ask}, Продать Bybit @{bybit_bid}. Спред: {profit_bin_byb_pct:.4f}% | {log_prices}")
            arbitrage_found = True

        if profit_byb_bin_pct >= ARBITRAGE_THRESHOLD_PCT:
            logger.info(f"[АРБИТРАЖ] [{symbol}] Купить Bybit @{bybit_ask}, Продать Binance @{binance_bid}. Спред: {profit_byb_bin_pct:.4f}% | {log_prices}")
            arbitrage_found = True

        # Если арбитража нет, логируем с уровнем DEBUG для меньшего спама
        if not arbitrage_found:
            logger.debug(f"[{symbol}] Спред BB->BY: {profit_bin_byb_pct:.4f}%, BY->BB: {profit_byb_bin_pct:.4f}% | {log_prices}")


# === Основная функция запуска ===

async def main():
    """Запускает клиенты для всех бирж и управляет ими."""
    logger.info("Запуск арбитражного монитора...")
    logger.info(f"Отслеживаемые символы: {SYMBOLS_TO_TRACK}")
    logger.info(f"Порог арбитража: {ARBITRAGE_THRESHOLD_PCT}%")

    # Создаем задачи для каждого клиента биржи
    tasks = [
        asyncio.create_task(bybit_client(SYMBOLS_TO_TRACK), name=f"{BYBIT_EXCHANGE_NAME}_client"),
        asyncio.create_task(binance_client(SYMBOLS_TO_TRACK), name=f"{BINANCE_EXCHANGE_NAME}_client"),
    ]

    # Запускаем все задачи параллельно и ждем их завершения (что не должно произойти)
    # Либо можно использовать asyncio.wait или добавить обработку завершения задач
    logger.info(f"Запуск {len(tasks)} задач с помощью asyncio.gather...")
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.critical(f"Критическая ошибка в asyncio.gather: {e}", exc_info=True)

    # Если какая-то задача завершилась (а не должна при нормальной работе)
    for task in done:
        try:
            result = task.result() # Получаем результат (если был) или исключение
            logger.warning(f"Задача {task.get_name()} неожиданно завершилась с результатом: {result}")
        except Exception as e:
            logger.error(f"Задача {task.get_name()} неожиданно завершилась с ошибкой:", exc_info=e)

    # В идеале, мы сюда не должны попадать при штатной работе
    logger.warning("Одна из основных задач завершилась. Программа может работать некорректно.")
    # Можно добавить логику перезапуска завершенной задачи или остановки всего приложения


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена вручную (KeyboardInterrupt).")
    except Exception as e:
        logger.critical("Критическая неперехваченная ошибка в главном потоке:", exc_info=True)