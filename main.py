import asyncio
import logging
import sys
import math
import aiohttp

# Импорт НОВОГО модуля данных, конфига и модели
import data_store
from config import (
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
    MEXC_EXCHANGE_NAME,
    KUCOIN_EXCHANGE_NAME,
    WS_CHUNK_SIZE, # Убедись, что эта константа есть в config.py
    # ARBITRAGE_THRESHOLD_PCT, # Не нужен здесь
    # HTX_EXCHANGE_NAME,
)
from models import TickerData

from exchange_clients.bybit_ws import bybit_client
from exchange_clients.binance_ws import binance_client
from exchange_clients.mexc_ws import mexc_client
from exchange_clients.kucoin_ws import kucoin_client
# from exchange_clients.htx_ws import htx_client


# --- Настройка Логгирования ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logging.getLogger("websockets").setLevel(logging.INFO)
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("redis").setLevel(logging.INFO)


# --- Установка правильного цикла событий для Windows ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# --- Вспомогательная функция для разбивки на чанки ---
def create_symbol_chunks(symbols: list[str], chunk_size: int) -> list[list[str]]:
    """Разбивает список символов на чанки заданного размера."""
    if not symbols or chunk_size <= 0:
        logger.warning(f"Неверные входные данные для create_symbol_chunks: symbols={len(symbols)}, chunk_size={chunk_size}")
        return [symbols] if symbols else []
    num_chunks = math.ceil(len(symbols) / chunk_size)
    logger.info(f"Разбиение {len(symbols)} символов на {num_chunks} чанков размером до {chunk_size}")
    return [symbols[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]


# === Основная функция запуска ===
async def main():
    """Запускает клиенты бирж и задачу обновления CoinGecko."""
    logger.info("Запуск арбитражного монитора (Запись в Redis)...")
    logger.info(f"Отслеживаемые символы ({len(SYMBOLS_TO_TRACK)} шт.)")
    logger.info(f"Размер чанка для WS: {WS_CHUNK_SIZE} символов на соединение")

    if not await data_store.check_redis_connection():
         logger.critical("Не удалось подключиться к Redis. Завершение работы.")
         return

    # Создаем сессию aiohttp ОДИН РАЗ для всех HTTP запросов
    http_session = aiohttp.ClientSession() # <-- Создаем сессию

    # Разбиваем символы на чанки для WebSocket
    symbol_chunks = create_symbol_chunks(list(SYMBOLS_TO_TRACK), WS_CHUNK_SIZE)
    if not symbol_chunks:
         logger.error("Не удалось создать чанки символов.")
         await http_session.close() # Закрываем сессию перед выходом
         return

    tasks = []
    client_map = {
        BYBIT_EXCHANGE_NAME: bybit_client,
        BINANCE_EXCHANGE_NAME: binance_client,
        MEXC_EXCHANGE_NAME: mexc_client,
        KUCOIN_EXCHANGE_NAME: kucoin_client,
        # HTX_EXCHANGE_NAME: htx_client,
    }

    # --- Создаем задачи для клиентов бирж ---
    for exchange_name, client_func in client_map.items():
        # Передаем HTTP сессию в клиент KuCoin
        client_kwargs = {"session": http_session} if exchange_name == KUCOIN_EXCHANGE_NAME else {}

        # Определяем чанки для текущей биржи
        if exchange_name == KUCOIN_EXCHANGE_NAME and len(SYMBOLS_TO_TRACK) <= 100:
             chunks_for_exchange = [list(SYMBOLS_TO_TRACK)]
             logger.info(f"Для {exchange_name} используется 1 соединение для всех {len(SYMBOLS_TO_TRACK)} символов.")
        else:
             chunks_for_exchange = symbol_chunks
             logger.info(f"Для {exchange_name} используется {len(chunks_for_exchange)} соединений (чанков).")

        # Создаем задачи для каждого чанка
        for i, chunk in enumerate(chunks_for_exchange):
             if not chunk: continue
             task_name = f"{exchange_name}_client_{i+1}"
             # Передаем чанк и доп. аргументы (сессию для KuCoin)
             tasks.append(asyncio.create_task(client_func(list(chunk), **client_kwargs), name=task_name))
             logger.debug(f"Создана задача {task_name} для {len(chunk)} символов.")

    if not tasks:
         logger.warning("Не создано ни одной задачи для запуска."); await http_session.close(); return

    logger.info(f"Запуск {len(tasks)} задач...")

    # --- Ожидание завершения ---
    done, pending = set(), set()
    try:
        await asyncio.gather(*tasks)
        logger.warning("asyncio.gather завершился неожиданно.")
    except asyncio.CancelledError:
         logger.info("Главная задача была отменена (Ctrl+C)."); pending = tasks
    except Exception as e:
         logger.critical(f"Критическая ошибка в основном цикле: {e}", exc_info=True); pending = tasks
    finally:
        # --- Остановка и очистка ---
        logger.info("Начало процесса остановки...")
        tasks_to_cancel = pending.union(t for t in tasks if t and not t.done())
        if tasks_to_cancel:
            logger.info(f"Отмена {len(tasks_to_cancel)} работающих задач..."); [t.cancel() for t in tasks_to_cancel]; await asyncio.gather(*tasks_to_cancel, return_exceptions=True); logger.info("Задачи отменены.")
        else: logger.info("Нет работающих задач для отмены.")

        # Закрываем HTTP сессию
        if http_session and not http_session.closed:
            await http_session.close()
            logger.info("Сессия aiohttp закрыта.")

        # Закрываем Redis
        if data_store.redis_client:
            try:
                logger.info("Закрытие пула соединений Redis...")
                await data_store.redis_client.close()
                if data_store.redis_pool: await data_store.redis_pool.disconnect()
                logger.info("Пул соединений Redis закрыт.")
            except Exception as e_redis_close: logger.error(f"Ошибка при закрытии Redis: {e_redis_close}")

        logger.warning("Основная функция main сборщика данных завершилась.")


# --- Точка входа ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена вручную (KeyboardInterrupt).")
