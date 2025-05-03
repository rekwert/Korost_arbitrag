# exchange_clients/kucoin_ws.py
import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
import data_store
import websockets
import aiohttp
from websockets.client import WebSocketClientProtocol

from models import TickerData
from config import (
    KUCOIN_EXCHANGE_NAME,
    KUCOIN_BASE_REST_URL,
    SYMBOLS_TO_TRACK,
)


logger = logging.getLogger(__name__)

# --- Вспомогательные функции ---
def format_symbol_to_kucoin(symbol: str) -> str | None:
    """Преобразует формат символа (напр., BTCUSDT) в формат KuCoin (BTC-USDT)."""
    try:
        if symbol.endswith("USDT"): return f"{symbol[:-4]}-USDT"
        if symbol.endswith("USDC"): return f"{symbol[:-4]}-USDC"
        if symbol.endswith("BTC"): return f"{symbol[:-3]}-BTC"
        if symbol.endswith("ETH"): return f"{symbol[:-3]}-ETH"
        if symbol.endswith("KCS"): return f"{symbol[:-3]}-KCS"
        # Простая эвристика для других пар (может потребовать доработки)
        known_bases = ["BTC", "ETH", "LTC", "XRP", "ADA", "SOL", "DOT", "MATIC", "LINK", "DOGE","AVAX", "NEAR", "UNI", "TRX", "FTM", "BNB"]
        for base in known_bases:
             if symbol.startswith(base):
                  quote = symbol[len(base):]
                  if quote: return f"{base}-{quote}"
        # Если совсем не опознали, возвращаем None
        logger.warning(f"Не удалось определить формат KuCoin для символа: {symbol}")
        return None
    except Exception as e:
        logger.error(f"Ошибка форматирования символа {symbol} для KuCoin: {e}")
        return None


def format_symbol_from_kucoin(kucoin_symbol: str) -> str:
    """Преобразует формат KuCoin (BTC-USDT) обратно в наш формат (BTCUSDT)."""
    return kucoin_symbol.replace("-", "")

# --- Функции клиента KuCoin ---

async def get_kucoin_ws_details(session: aiohttp.ClientSession) -> dict | None:
    """Получает токен и данные для подключения к публичному WebSocket KuCoin."""
    rest_url = KUCOIN_BASE_REST_URL + "/api/v1/bullet-public"
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запрос данных для WebSocket к {rest_url}...")
    try:
        async with session.post(rest_url, timeout=10) as response:
            response.raise_for_status()
            response_json = await response.json()
            logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Ответ от bullet-public: {response_json}")

            if response_json.get("code") == "200000" and "data" in response_json:
                data = response_json["data"]
                token = data.get("token")
                servers = data.get("instanceServers")
                if token and servers:
                    server_info = servers[0]
                    endpoint = server_info.get("endpoint")
                    ping_interval = server_info.get("pingInterval")
                    ping_timeout = server_info.get("pingTimeout")
                    if endpoint and ping_interval and ping_timeout and server_info.get("encrypt"):
                        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Получены данные WS: interval={ping_interval}, timeout={ping_timeout}")
                        return {
                            "endpoint": endpoint, "token": token,
                            "ping_interval": ping_interval, "ping_timeout": ping_timeout,
                        }
                    else: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Неполные данные сервера: {server_info}")
                else: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Нет токена или серверов: {data}")
            else: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка API KuCoin (code={response_json.get('code')}): {response_json.get('msg')}")
    except Exception as e:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка получения данных WS: {e}", exc_info=True)
    return None

async def subscribe_kucoin(websocket: WebSocketClientProtocol, symbols: list[str]):
    """Подписывается на поток /market/ticker:{symbol} для каждого символа ОТДЕЛЬНО."""
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Вход в subscribe_kucoin для {symbols}")
    if not symbols: return
    base_id = int(time.time() * 1000)
    for i, symbol in enumerate(symbols):
        kucoin_symbol = format_symbol_to_kucoin(symbol)
        if not kucoin_symbol: continue
        request_id = base_id + i
        topic = f"/market/ticker:{kucoin_symbol}"
        subscription_message = {
            "id": str(request_id), "type": "subscribe", "topic": topic,
            "privateChannel": False, "response": True
        }
        try:
            logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Отправка подписки: {subscription_message}")
            await websocket.send(json.dumps(subscription_message))
            logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Отправлена подписка на: {topic} (ID: {request_id})")
            await asyncio.sleep(0.1)
        except websockets.exceptions.ConnectionClosed:
            logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто при подписке на {topic}.")
            break
        except Exception as e:
            logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка при подписке на {topic}: {e}", exc_info=True)
    logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Выход из subscribe_kucoin")

async def handle_kucoin_messages(websocket: WebSocketClientProtocol):
    """Обрабатывает сообщения от WebSocket KuCoin."""
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запуск обработчика сообщений KuCoin...")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Получено raw сообщение: {data}")

                if not isinstance(data, dict): continue

                msg_type = data.get("type")
                msg_id = data.get("id")

                if msg_type == "welcome":
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Получено Welcome (ID: {msg_id})")
                    continue
                elif msg_type == "pong":
                    logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Получен Pong (ID: {msg_id})")
                    continue
                elif msg_type == "ack":
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Получено Ack (ID: {msg_id})")
                    continue
                elif msg_type == "error": # Явно обрабатываем ошибки от KuCoin
                    logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Получена ошибка от сервера: {data}")
                    continue

                elif msg_type == "message" and data.get("topic", "").startswith("/market/ticker:"):
                    topic = data.get("topic")
                    subject = data.get("subject")
                    ticker_data = data.get("data")

                    if subject == "trade.ticker" and ticker_data:
                        kucoin_symbol = topic.split(":")[-1]
                        symbol = format_symbol_from_kucoin(kucoin_symbol)

                        if not symbol or symbol not in SYMBOLS_TO_TRACK:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Пропуск тикера: {symbol}")
                            continue

                        try:
                            best_bid = ticker_data.get("bestBid")
                            best_ask = ticker_data.get("bestAsk")
                            last_price = ticker_data.get("price")
                            timestamp_ms = ticker_data.get("time")
                            # Извлекаем размеры
                            best_bid_size_str = ticker_data.get("bestBidSize") # <-- Добавили
                            best_ask_size_str = ticker_data.get("bestAskSize") # <-- Добавили

                            bid_price = Decimal(best_bid) if best_bid else None
                            ask_price = Decimal(best_ask) if best_ask else None
                            last_p = Decimal(last_price) if last_price else None
                            # Конвертируем размеры
                            bid_size = Decimal(best_bid_size_str) if best_bid_size_str else None # <-- Добавили
                            ask_size = Decimal(best_ask_size_str) if best_ask_size_str else None # <-- Добавили

                            if bid_price is None or ask_price is None:
                                logger.warning(f"[{KUCOIN_EXCHANGE_NAME}][{symbol}] Отсутствуют bid/ask: {ticker_data}")
                                continue

                            # Создаем объект TickerData
                            ticker_obj = TickerData(
                                exchange=KUCOIN_EXCHANGE_NAME,
                                symbol=symbol,
                                timestamp_ms=int(timestamp_ms) if timestamp_ms else 0,
                                bid_price=bid_price,
                                ask_price=ask_price,
                                bid_size=bid_size,  # <-- Добавили
                                ask_size=ask_size,  # <-- Добавили
                                last_price=last_p
                            )
                            # Отправляем в Redis
                            await data_store.update_ticker_in_redis(ticker_obj)

                            logger.debug(
                                f"[{KUCOIN_EXCHANGE_NAME}] Обновлен тикер [{symbol}]: "
                                f"B:{bid_price} A:{ask_price}"
                            )
                            continue

                        except (InvalidOperation, ValueError, TypeError) as e:
                            logger.warning(f"[{KUCOIN_EXCHANGE_NAME}][{symbol}] Ошибка обработки тикера: {e} - Данные: {ticker_data}")
                        except Exception as e_inner:
                            logger.error(f"[{KUCOIN_EXCHANGE_NAME}][{symbol}] Ошибка обработки тикера: {e_inner}", exc_info=True)
                    else:
                        logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Неизвестный subject/нет данных: {data}")
                else:
                    logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Сообщение неизвестного типа: {data}")

            except json.JSONDecodeError:
                logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Не удалось декодировать JSON: {message}")
            except Exception as e:
                logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка при обработке сообщения: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed as e:
        logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто: Код={e.code}, Причина='{e.reason}'")
    except Exception as e:
        logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Критическая ошибка в цикле handle_kucoin_messages: {e}", exc_info=True)
    finally:
        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Завершение обработчика сообщений.")


async def kucoin_pinger(websocket: WebSocketClientProtocol, interval_ms: int):
    """Периодически отправляет PING на сервер KuCoin."""
    ping_interval_sec = max(1, (interval_ms / 1000) - 5)
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запуск пингера с интервалом {ping_interval_sec:.1f} сек.")
    while True:
        try:
            await asyncio.sleep(ping_interval_sec)
            ping_id = int(time.time() * 1000)
            ping_message = {"id": str(ping_id), "type": "ping"}
            logger.debug(f"[{KUCOIN_EXCHANGE_NAME}] Отправка PING: {ping_message}")
            # Используем send, так как ping ожидает байты, а KuCoin хочет JSON-строку
            await websocket.send(json.dumps(ping_message))
        except asyncio.CancelledError:
            logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Пингер остановлен.")
            break
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто для PING.")
            break
        except Exception as e:
            logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка в пингере: {e}", exc_info=True)
            await asyncio.sleep(5)


async def kucoin_client(symbols: list[str], session: aiohttp.ClientSession):
    """Основной клиент WebSocket для KuCoin."""
    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Инициализация клиента KuCoin...")
    # session = None # <-- УДАЛЕНО
    try:
        # session = aiohttp.ClientSession() # <-- УДАЛЕНО

        while True:
            ping_task = None
            try:
                logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Запрос данных для нового WebSocket соединения...")
                ws_details = await get_kucoin_ws_details(session) # <-- Используем переданную сессию
                if not ws_details:
                    logger.warning(f"[{KUCOIN_EXCHANGE_NAME}] Не получены данные WS, повтор через 30 сек...")
                    await asyncio.sleep(30)
                    continue

                full_uri = f"{ws_details['endpoint']}?token={ws_details['token']}"
                ping_interval_ms = ws_details['ping_interval']
                ping_timeout_ms = ws_details['ping_timeout']
                connect_timeout_sec = (ping_timeout_ms / 1000) + 10

                logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Попытка подключения к {ws_details['endpoint'][:20]}...")
                async with websockets.connect(
                    full_uri, ping_interval=None, ping_timeout=connect_timeout_sec
                ) as websocket:
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] WebSocket соединение установлено: {websocket.id}")
                    ping_task = asyncio.create_task(
                        kucoin_pinger(websocket, ping_interval_ms), name=f"{KUCOIN_EXCHANGE_NAME}_pinger"
                    )
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Задача пингера запущена.")
                    await subscribe_kucoin(websocket, symbols)
                    await handle_kucoin_messages(websocket)

            except websockets.exceptions.ConnectionClosedOK as e: logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто штатно. Код: {e.code}, Причина: {e.reason}")
            except websockets.exceptions.ConnectionClosedError as e: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Соединение закрыто с ошибкой. Код: {e.code}, Причина: {e.reason}")
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as e: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка/таймаут соединения WebSocket: {e}")
            except Exception as e: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка в цикле подключения/обработки KuCoin: {e}", exc_info=True)
            finally:
                if ping_task and not ping_task.done():
                    logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Отмена задачи пингера...")
                    ping_task.cancel()
                    try: await ping_task
                    except asyncio.CancelledError: logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Пингер отменен.")
                    except Exception as e_ping: logger.error(f"[{KUCOIN_EXCHANGE_NAME}] Ошибка отмены пингера: {e_ping}")

            logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Попытка переподключения через 15 секунд...")
            await asyncio.sleep(15)
    except asyncio.CancelledError:
         logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Задача клиента KuCoin отменена.")
    except Exception as e:
         logger.critical(f"[{KUCOIN_EXCHANGE_NAME}] Критическая ошибка в kucoin_client: {e}", exc_info=True)
    finally:
        # Сессию здесь НЕ закрываем, она закроется в main.py
        logger.info(f"[{KUCOIN_EXCHANGE_NAME}] Завершение работы клиента KuCoin.")