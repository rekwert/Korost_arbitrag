import asyncio
import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, List

# Используем redis-py >= 5.0 с поддержкой asyncio
import redis.asyncio as redis
# Импортируем синхронную версию ЛОКАЛЬНО в функции get_all_tickers_sync
# import redis as sync_redis

from models import TickerData
from config import REDIS_URL # Импортируем только URL Redis

logger = logging.getLogger(__name__)

# --- Клиент Redis (Асинхронный) ---
redis_client: Optional[redis.Redis] = None
redis_pool: Optional[redis.ConnectionPool] = None
try:
    # decode_responses=True автоматически декодирует ответы (ключи и значения) в строки UTF-8
    redis_pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True, max_connections=50)
    redis_client = redis.Redis.from_pool(redis_pool)
    logger.info(f"Асинхронный пул соединений Redis создан для {REDIS_URL}")
except NameError:
    logger.critical("Константа REDIS_URL не найдена в config.py!")
    redis_client = None
except Exception as e:
    logger.critical(f"Не удалось создать асинхронный пул соединений Redis: {e}", exc_info=True)
    redis_client = None

async def check_redis_connection():
    """Проверяет соединение с Redis при запуске."""
    if not redis_client:
        logger.error("Асинхронный клиент Redis не был инициализирован.")
        return False
    try:
        pong = await redis_client.ping()
        logger.info(f"Соединение с Redis успешно установлено (PING={pong}).")
        return True
    except Exception as e:
        logger.error(f"Ошибка соединения с Redis при запуске: {e}")
        return False

# --- Функции для работы с данными в Redis ---

def _get_ticker_key(exchange: str, symbol: str) -> str:
    """Генерирует ключ для хранения тикера в Redis."""
    return f"ticker:{exchange}:{symbol}"

async def update_ticker_in_redis(ticker: TickerData):
    """
    Обновляет данные тикера в Redis, устанавливая каждое поле ОТДЕЛЬНО.
    """
    if not redis_client or not isinstance(ticker, TickerData):
        logger.warning(f"Redis клиент не доступен или передан неверный тип данных: {type(ticker)}")
        return

    key = _get_ticker_key(ticker.exchange, ticker.symbol)
    try:
        ticker_hash_data = {
            "bid_price": str(ticker.bid_price) if ticker.bid_price is not None else "",
            "ask_price": str(ticker.ask_price) if ticker.ask_price is not None else "",
            "last_price": str(ticker.last_price) if ticker.last_price is not None else "",
            "timestamp_ms": str(ticker.timestamp_ms) if ticker.timestamp_ms is not None else "0",
        }

        # --- ИЗМЕНЕНИЕ: Устанавливаем поля по одному ---
        logger.debug(f"Установка полей по одному для {key}")
        success_count = 0
        for field, value in ticker_hash_data.items():
            try:
                # hset(key, field, value)
                await redis_client.hset(key, field, value)
                success_count += 1
            except redis.RedisError as e_field:
                 logger.error(f"Ошибка Redis при установке поля '{field}' для {key}: {e_field}")
                 # Можно решить, прерывать ли обновление или продолжать с другими полями
                 # continue # Продолжаем с другими полями
                 break # Прерываем обновление для этого тикера
            except Exception as e_field_other:
                 logger.error(f"Ошибка при установке поля '{field}' для {key}: {e_field_other}", exc_info=True)
                 break # Прерываем

        if success_count == len(ticker_hash_data):
            logger.debug(f"Данные для {key} обновлены в Redis (по полям).")
        else:
            logger.warning(f"Не все поля обновлены для {key} из-за ошибок.")
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    except redis.RedisError as e:
        # Эта ошибка маловероятна теперь, т.к. ошибки ловятся в цикле
        logger.error(f"Ошибка Redis при записи {key}: {e} (Args: {e.args if hasattr(e, 'args') else 'N/A'})")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при записи в Redis для {key}: {e}", exc_info=True)

# --- Синхронная функция для чтения данных (для Streamlit) ---
def get_all_tickers_sync(exchanges: List[str], symbols: List[str]) -> Dict[str, Dict[str, Optional[TickerData]]]:
    """
    СИНХРОННО получает все тикеры из Redis.
    Предназначена для использования в синхронном коде (Streamlit).
    """
    logger.debug("Запуск синхронного получения данных из Redis...")
    all_tickers_data: Dict[str, Dict[str, Optional[TickerData]]] = {ex: {} for ex in exchanges}
    sync_r = None
    try:
        # Создаем и импортируем синхронный клиент Redis ЛОКАЛЬНО
        import redis as sync_redis
        # decode_responses=True важен для получения строк
        sync_r = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
        sync_r.ping() # Проверка соединения
    except Exception as e:
         logger.error(f"Не удалось создать/подключиться синхронным клиентом Redis: {e}")
         # Возвращаем пустую структуру, если Redis недоступен
         return {ex: {sym: None for sym in symbols} for ex in exchanges}

    # Используем pipeline для эффективности (меньше запросов)
    pipe = sync_r.pipeline(transaction=False)
    keys_to_fetch = []
    key_to_ex_sym = {} # Для обратного маппинга ключа на биржу/символ

    for ex_name in exchanges:
        all_tickers_data[ex_name] = {}
        for symbol in symbols:
            key = _get_ticker_key(ex_name, symbol)
            keys_to_fetch.append(key)
            key_to_ex_sym[key] = (ex_name, symbol)
            pipe.hgetall(key) # Добавляем команду в pipeline

    try:
        # Выполняем все команды hgetall одним запросом
        results = pipe.execute()
    except redis.RedisError as e:
         logger.error(f"Синхронная ошибка Redis при выполнении pipeline: {e}")
         results = [None] * len(keys_to_fetch) # Заполняем None в случае ошибки
    except Exception as e_pipe:
        logger.error(f"Синхронная ошибка при выполнении pipeline Redis: {e_pipe}")
        results = [None] * len(keys_to_fetch)

    # Обрабатываем результаты
    for i, key in enumerate(keys_to_fetch):
        ex_name, symbol = key_to_ex_sym[key]
        ticker_hash_data = results[i]

        if isinstance(ticker_hash_data, dict) and ticker_hash_data:
            try:
                bid_str = ticker_hash_data.get("bid_price", "")
                ask_str = ticker_hash_data.get("ask_price", "")
                last_str = ticker_hash_data.get("last_price", "")
                ts_str = ticker_hash_data.get("timestamp_ms", "0")

                ticker = TickerData(
                    exchange=ex_name,
                    symbol=symbol,
                    bid_price=Decimal(bid_str) if bid_str else None,
                    ask_price=Decimal(ask_str) if ask_str else None,
                    last_price=Decimal(last_str) if last_str else None,
                    timestamp_ms=int(ts_str) if ts_str else 0,
                )
                all_tickers_data[ex_name][symbol] = ticker
            except (InvalidOperation, ValueError, TypeError) as e:
                 logger.warning(f"Синхронная ошибка конвертации данных из Redis для {key}: {e} - Данные: {ticker_hash_data}")
                 all_tickers_data[ex_name][symbol] = None
            except Exception as e_conv:
                 logger.error(f"Синхронная непредвиденная ошибка конвертации {key}: {e_conv}", exc_info=True)
                 all_tickers_data[ex_name][symbol] = None
        else:
             all_tickers_data[ex_name][symbol] = None # Ключ не найден или пустые данные

    # Закрываем синхронное соединение
    if sync_r:
        try:
            sync_r.close()
        except Exception as e_close:
             logger.warning(f"Ошибка при закрытии синхронного клиента Redis: {e_close}")

    logger.debug(f"Синхронное получение данных из Redis завершено. Получено ключей: {len(results)}")
    return all_tickers_data