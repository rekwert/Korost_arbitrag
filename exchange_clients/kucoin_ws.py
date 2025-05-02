import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation

import websockets
import aiohttp
from websockets.client import WebSocketClientProtocol

from models import TickerData
from config import (
    KUCOIN_EXCHANGE_NAME,
    KUCOIN_BASE_REST_URL,
    SYMBOLS_TO_TRACK,
)
# Import shared state and logic
try:
    from shared_data import latest_tickers, find_arbitrage_opportunities
except ImportError:
    # Заглушка на случай импорта не из main
    print("Не удалось импортировать latest_tickers/find_arbitrage_opportunities из shared_data")
    latest_tickers = {}
    def find_arbitrage_opportunities(): pass

logger = logging.getLogger(__name__)

# --- Вспомогательные функции ---

def format_symbol_to_kucoin(symbol: str) -> str:
    """Преобразует формат символа (напр., BTCUSDT) в формат KuCoin (BTC-USDT)."""
    # TODO: Добавить более надежное определение базовой и квотируемой валюты,
    # если будут не только USDT пары. Пока просто вставляем дефис.
    # Пример: 'BTCUSDT' -> 'BTC-USDT', 'ETHBTC' -> 'ETH-BTC'
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    elif symbol.endswith("BTC"):
         return f"{symbol[:-3]}-BTC"
    # Добавь другие популярные квотируемые валюты при необходимости
    else:
         # Простая эвристика для других пар (может быть неточной)
         # Ищем известные базовые валюты
         known_bases = ["BTC","ETH", "BNB", "XRP","SOL"] # Дополнить по необходимости
         for base in known_bases:
              if symbol.startswith(base):
                   quote = symbol[len(base):]
                   return f"{base}-{quote}"
         # Если не нашли, просто вставляем дефис посередине (не очень надежно)
         mid = len(symbol) // 2
         return f"{symbol[:mid]}-{symbol[mid:]}"


def format_symbol_from_kucoin(kucoin_symbol: str) -> str:
    """Преобразует формат KuCoin (BTC-USDT) обратно в наш формат (BTCUSDT)."""
    return kucoin_symbol.replace("-", "")

# --- Основные функции клиента KuCoin ---

async def get_kucoin_ws_details(session: aiohttp.ClientSession) -> dict | None:
    """
    Получает токен и данные для подключения к публичному WebSocket KuCoin.

    Args:
        session: Активная сессия aiohttp.ClientSession.

    Returns:
        Словарь с 'endpoint', 'token', 'ping_interval', 'ping_timeout',
        или None в случае ошибки.
    """
    rest_url = KUCOIN_BASE_REST_URL + "/api/v1/bullet-public"
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запрос данных для WebSocket к {rest_url}...")
    try:
        # KuCoin требует POST для этого эндпоинта, даже без тела запроса
        async with session.post(rest_url, timeout=10) as response: # Таймаут 10 секунд
            response.raise_for_status() # Проверяем на HTTP ошибки (4xx, 5xx)
            response_json = await response.json()
            logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Ответ от bullet-public: {response_json}")

            # Проверяем структуру ответа и код успеха KuCoin
            if response_json.get("code") == "200000" and "data" in response_json:
                data = response_json["data"]
                token = data.get("token")
                servers = data.get("instanceServers")

                if token and servers:
                    # Берем первый доступный сервер
                    server_info = servers[0]
                    endpoint = server_info.get("endpoint")
                    ping_interval = server_info.get("pingInterval")
                    ping_timeout = server_info.get("pingTimeout")
                    encrypt = server_info.get("encrypt") # Должно быть True для wss

                    if endpoint and ping_interval and ping_timeout and encrypt:
                        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Получены данные для WebSocket: endpoint='{endpoint}', interval={ping_interval}, timeout={ping_timeout}")
                        return {
                            "endpoint": endpoint,
                            "token": token,
                            "ping_interval": ping_interval,
                            "ping_timeout": ping_timeout,
                        }
                    else:
                        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Неполные данные сервера в ответе: {server_info}")
                else:
                    logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Отсутствует токен или список серверов в ответе: {data}")
            else:
                logger.error(
                    f"[{KUCOIN_EXCHANGE_NAME}] Ошибка в ответе API KuCoin (code={response_json.get('code')}): "
                    f"{response_json.get('msg', 'Нет сообщения об ошибке')}"
                )

    except aiohttp.ClientResponseError as e:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка HTTP при запросе токена: {e.status} {e.message}")
    except asyncio.TimeoutError:
         logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Таймаут при запросе токена к {rest_url}")
    except aiohttp.ClientConnectionError as e:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка соединения при запросе токена: {e}")
    except json.JSONDecodeError:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось декодировать JSON ответ от {rest_url}")
    except Exception as e:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Непредвиденная ошибка при получении данных WebSocket: {e}", exc_info=True)

    return None # Возвращаем None, если что-то пошло не так

async def subscribe_kucoin(websocket: WebSocketClientProtocol, symbols: list[str]):
    """
    Подписывается на поток /market/ticker:{symbol} для указанных символов на KuCoin.

    Args:
        websocket: Активное WebSocket соединение.
        symbols: Список символов в стандартном формате (напр., ['BTCUSDT', 'ETHUSDT']).
    """
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Вход в subscribe_kucoin для {symbols}")
    if not symbols:
        logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Нет символов для подписки.")
        return

    # Формируем список топиков в формате KuCoin
    topics = []
    for symbol in symbols:
        kucoin_symbol = format_symbol_to_kucoin(symbol)
        if kucoin_symbol:
            topics.append(f"/market/ticker:{kucoin_symbol}")
        else:
            logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось отформатировать символ {symbol} для KuCoin.")

    if not topics:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось сформировать топики для подписки.")
        return

    # Генерируем уникальный ID для запроса
    request_id = int(time.time() * 1000)

    # Формируем сообщение подписки (можно подписываться на несколько топиков сразу)
    subscription_message = {
        "id": request_id,
        "type": "subscribe",
        "topic": ",".join(topics),  # KuCoin позволяет подписываться на несколько топиков через запятую
        "privateChannel": False,   # Указываем, что это публичный канал
        "response": True           # Запрашиваем подтверждение (ack)
    }

    try:
        logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Попытка отправки подписки: {subscription_message}")
        await websocket.send(json.dumps(subscription_message))
        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Отправлена подписка на топики: {topics}")
    except websockets.exceptions.ConnectionClosed:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось отправить подписку: соединение закрыто.")
    except Exception as e:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка при отправке подписки: {e}", exc_info=True)
    logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Выход из subscribe_kucoin")

async def handle_kucoin_messages(websocket: WebSocketClientProtocol):
    """
    Обрабатывает сообщения от WebSocket KuCoin: welcome, ack, pong, ticker.
    """
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запуск обработчика сообщений KuCoin...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Получено raw сообщение: {data}")

                msg_type = data.get("type")
                msg_id = data.get("id")

                # 1. Обработка Welcome сообщения
                if msg_type == "welcome":
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Получено Welcome сообщение: {data}")
                    continue

                # 2. Обработка Pong ответа (на наш Ping)
                elif msg_type == "pong":
                    # ID в pong должен совпадать с ID отправленного ping
                    logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Получен Pong (ID: {msg_id})")
                    continue

                # 3. Обработка Ack (подтверждение подписки)
                elif msg_type == "ack":
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Получено Ack (ID: {msg_id})")
                    continue

                # 4. Обработка Ticker данных
                elif msg_type == "message" and data.get("topic", "").startswith("/market/ticker:"):
                    topic = data.get("topic")
                    subject = data.get("subject")
                    ticker_data = data.get("data")

                    if subject == "trade.ticker" and ticker_data:
                        kucoin_symbol = topic.split(":")[-1]
                        symbol = format_symbol_from_kucoin(kucoin_symbol)

                        if not symbol or symbol not in SYMBOLS_TO_TRACK:
                            logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Пропуск тикера для не отслеживаемого символа: {symbol}")
                            continue

                        try:
                            # Переменные называются best_bid_price / best_ask_price
                            best_bid = ticker_data.get("bestBid")
                            best_ask = ticker_data.get("bestAsk")
                            timestamp_ms = ticker_data.get("time")

                            best_bid_price = Decimal(best_bid) if best_bid else None
                            best_ask_price = Decimal(best_ask) if best_ask else None

                            if best_bid_price is None or best_ask_price is None:
                                logger.warning(f"[{KUCOIN_EXCHANGE_NAME}][{symbol}] Отсутствуют bid/ask в данных тикера: {ticker_data}")
                                continue

                            # === ИСПРАВЛЕННАЯ ЛОГИКА ОБНОВЛЕНИЯ ===
                            # Получаем или создаем объект TickerData
                            symbol_data = latest_tickers[KUCOIN_EXCHANGE_NAME].setdefault(symbol, None)
                            if symbol_data is None:
                                symbol_data = TickerData(exchange=KUCOIN_EXCHANGE_NAME, symbol=symbol, timestamp_ms=0)
                                latest_tickers[KUCOIN_EXCHANGE_NAME][symbol] = symbol_data

                            # Обновляем поля объекта
                            symbol_data.timestamp_ms = int(timestamp_ms) if timestamp_ms else 0
                            symbol_data.bid_price = best_bid_price # Имя переменной здесь best_bid_price
                            symbol_data.ask_price = best_ask_price # Имя переменной здесь best_ask_price
                            symbol_data.last_price = Decimal(ticker_data.get("price")) if ticker_data.get("price") else None

                            logger.debug(
                                f"[{KUCOIN_EXCHANGE_NAME}] Обновлен тикер [{symbol}]: "
                                f"B:{best_bid_price} A:{best_ask_price}" # Логируем правильные переменные
                            )

                            find_arbitrage_opportunities()
                            continue # Сообщение обработано

                            find_arbitrage_opportunities()
                            continue # Сообщение обработано

                        except (InvalidOperation, ValueError, TypeError) as e:
                            logger.warning(f"[{KUCOIN_EXCHANGE_NAME}][{symbol}] Не удалось обработать данные тикера: {e} - Данные: {ticker_data}")
                        except Exception as e:
                            logger.error(f"[{KUCOIN_EXCHANGE_NAME}][{symbol}] Ошибка при создании TickerData: {e}", exc_info=True)
                    else:
                        logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Неизвестный subject/отсутствуют данные в сообщении 'message': {data}")

                # 5. Логирование других типов сообщений
                else:
                    logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Получено сообщение неизвестного типа или структуры: {data}")

            except json.JSONDecodeError:
                logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(
            f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто в handle_messages: "
            f"Код={e.code}, Причина='{e.reason}'"
        )
    except Exception as e:
        logger.error(
            f"[{KUCOIN_EXCHANGE_NAME}] Критическая ошибка в цикле handle_kucoin_messages: {e}",
            exc_info=True
        )
    finally:
        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Завершение обработчика сообщений.")

async def kucoin_pinger(websocket: WebSocketClientProtocol, interval_ms: int):
    """
    Периодически отправляет PING-сообщения на сервер KuCoin для поддержания соединения.

    Args:
        websocket: Активное WebSocket соединение.
        interval_ms: Интервал отправки пингов в миллисекундах (полученный от API).
    """
    # Рассчитываем интервал в секундах для asyncio.sleep()
    # Рекомендуется отправлять пинг немного чаще, чем требует сервер
    ping_interval_sec = max(1, (interval_ms / 1000) - 5) # Отправляем за 5 сек до таймаута, но не чаще раза в секунду
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запуск пингера с интервалом {ping_interval_sec:.1f} сек.")

    while True:
        try:
            await asyncio.sleep(ping_interval_sec)

            ping_id = int(time.time() * 1000)
            ping_message = {"id": str(ping_id), "type": "ping"} # ID должен быть строкой по некоторым примерам

            logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Отправка PING: {ping_message}")
            await websocket.ping(json.dumps(ping_message).encode('utf-8')) # Используем встроенный метод ping(), он ожидает байты
            # Или можно использовать websocket.send(), если ping() не подходит:
            # await websocket.send(json.dumps(ping_message))

        except asyncio.CancelledError:
            logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Пингер остановлен.")
            break # Выходим из цикла при отмене задачи
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто во время ожидания/отправки PING.")
            break # Выходим из цикла, если соединение закрыто
        except Exception as e:
            logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка в пингере: {e}", exc_info=True)
            # Продолжаем пытаться пинговать после небольшой паузы
            await asyncio.sleep(5)

async def kucoin_client(symbols: list[str]):
    """
    Основной клиент WebSocket для KuCoin. Получает токен, подключается,
    обрабатывает сообщения и пинги, переподключается при необходимости.
    """
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Инициализация клиента KuCoin...")
    session = None
    try:
        # Создаем сессию aiohttp ОДИН раз
        session = aiohttp.ClientSession()

        while True: # Основной цикл для переподключения
            ping_task = None # Сбрасываем задачу пингера перед новой попыткой
            try:
                # 1. Получаем актуальные данные для подключения
                logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запрос данных для нового WebSocket соединения...")
                ws_details = await get_kucoin_ws_details(session)
                if not ws_details:
                    logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось получить данные для подключения. Повтор через 30 секунд...")
                    await asyncio.sleep(30)
                    continue # Начать цикл while сначала

                # Извлекаем параметры
                full_uri = f"{ws_details['endpoint']}?token={ws_details['token']}"
                ping_interval_ms = ws_details['ping_interval']
                ping_timeout_ms = ws_details['ping_timeout']
                connect_timeout_sec = (ping_timeout_ms / 1000) + 10 # Таймаут подключения и ожидания Pong

                logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Попытка подключения к {ws_details['endpoint']}...")

                # 2. Устанавливаем соединение
                async with websockets.connect(
                    full_uri,
                    ping_interval=None, # Пингуем вручную
                    ping_timeout=connect_timeout_sec
                ) as websocket:
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] WebSocket соединение установлено.")

                    # 3. Запускаем ручной пингер в фоновой задаче
                    ping_task = asyncio.create_task(
                        kucoin_pinger(websocket, ping_interval_ms),
                        name=f"{KUCOIN_EXCHANGE_NAME}_pinger"
                    )
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Задача пингера запущена.")

                    # 4. Отправляем подписку
                    await subscribe_kucoin(websocket, symbols)

                    # 5. Запускаем основной обработчик сообщений
                    await handle_kucoin_messages(websocket)

            # --- Обработка ошибок WebSocket ---
            except websockets.exceptions.ConnectionClosedOK as e:
                logger.info(
                    f"[{KUCOIN_EXCHANGE_NAME}] WebSocket соединение закрыто штатно. "
                    f"Код: {e.code}, Причина: {e.reason}"
                )
            except websockets.exceptions.ConnectionClosedError as e:
                 logger.error(
                     f"[{KUCOIN_EXCHANGE_NAME}] WebSocket соединение закрыто с ошибкой. "
                     f"Код: {e.code}, Причина: {e.reason}"
                 )
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e: # Добавили TimeoutError
                logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка соединения/таймаут WebSocket: {e}")
            except Exception as e:
                logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Непредвиденная ошибка в цикле подключения/обработки KuCoin: {e}", exc_info=True)
            finally:
                # Гарантированно останавливаем пингер при выходе из async with
                if ping_task and not ping_task.done():
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Отмена задачи пингера...")
                    ping_task.cancel()
                    try:
                        await ping_task # Даем задаче обработать отмену
                    except asyncio.CancelledError:
                        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Задача пингера успешно отменена.")
                    except Exception as e_ping:
                        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка при ожидании отмены пингера: {e_ping}")

            # Пауза перед следующей попыткой переподключения
            logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Попытка переподключения через 15 секунд...")
            await asyncio.sleep(15)

    except asyncio.CancelledError:
         logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Задача клиента KuCoin отменена.")
    except Exception as e:
         # Ловим ошибки, которые могли произойти вне основного цикла (например, при создании сессии)
         logger.critical(f"[{KUCOIN_EXCHANGE_NAME}] Критическая ошибка в kucoin_client: {e}", exc_info=True)
    finally:
        # Закрываем сессию aiohttp при завершении работы клиента
        if session and not session.closed:
            await session.close()
            logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Сессия aiohttp закрыта.")