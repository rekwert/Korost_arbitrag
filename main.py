# main.py
import asyncio
import logging
import sys
import math # Добавили для деления списка

# Импорт НОВОГО модуля данных, конфига и модели
import data_store
from config import (
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
    MEXC_EXCHANGE_NAME,
    KUCOIN_EXCHANGE_NAME,
    WS_CHUNK_SIZE, # Добавили размер чанка
    # ARBITRAGE_THRESHOLD_PCT, # Не нужен здесь
    # HTX_EXCHANGE_NAME,
)
from models import TickerData

# Импорт КЛИЕНТСКИХ функций
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
        logger.warning(f"Неверные входные данные для create_symbol_chunks: symbols={symbols}, chunk_size={chunk_size}")
        return [symbols] if symbols else [] # Возвращаем один чанк или пустой список
    # Используем math.ceil для корректного расчета количества чанков
    num_chunks = math.ceil(len(symbols) / chunk_size)
    logger.info(f"Разбиение {len(symbols)} символов на {num_chunks} чанков размером до {chunk_size}")
    return [symbols[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]


# === Основная функция запуска ===
async def main():
    """Запускает КЛИЕНТЫ бирж (по чанкам) для сбора данных и записи в Redis."""
    logger.info("Запуск арбитражного монитора (Запись в Redis)...")
    logger.info(f"Отслеживаемые символы ({len(SYMBOLS_TO_TRACK)} шт.)") # Убрали вывод списка
    logger.info(f"Размер чанка для WS: {WS_CHUNK_SIZE} символов на соединение")

    if not await data_store.check_redis_connection():
         logger.critical("Не удалось подключиться к Redis. Завершение работы.")
         return

    # Разбиваем символы на чанки
    symbol_chunks = create_symbol_chunks(list(SYMBOLS_TO_TRACK), WS_CHUNK_SIZE)
    if not symbol_chunks:
         logger.error("Не удалось создать чанки символов. Завершение работы.")
         return

    tasks = []
    # Словарь для удобного сопоставления имени биржи и функции клиента
    client_map = {
        BYBIT_EXCHANGE_NAME: bybit_client,
        BINANCE_EXCHANGE_NAME: binance_client,
        MEXC_EXCHANGE_NAME: mexc_client,
        KUCOIN_EXCHANGE_NAME: kucoin_client,
        # HTX_EXCHANGE_NAME: htx_client,
    }

    # --- Создаем задачи для КАЖДОГО чанка и КАЖДОЙ биржи ---
    for exchange_name, client_func in client_map.items():
        # KuCoin имеет ограничение на кол-во подписок (100) на одно соединение,
        # но также и на кол-во соединений с IP. Пока оставляем один чанк для KuCoin,
        # если символов <= 100. Если > 100, нужно будет делить и для KuCoin.
        if exchange_name == KUCOIN_EXCHANGE_NAME and len(SYMBOLS_TO_TRACK) <= 100:
             chunks_for_exchange = [list(SYMBOLS_TO_TRACK)] # Один чанк для KuCoin
             logger.info(f"Для {exchange_name} используется 1 соединение для всех {len(SYMBOLS_TO_TRACK)} символов.")
        else:
             chunks_for_exchange = symbol_chunks # Используем стандартные чанки
             logger.info(f"Для {exchange_name} используется {len(chunks_for_exchange)} соединений (чанков).")

        for i, chunk in enumerate(chunks_for_exchange):
             if not chunk: continue # Пропускаем пустые чанки
             task_name = f"{exchange_name}_client_{i+1}"
             # Передаем КОПИЮ чанка в задачу на всякий случай
             tasks.append(asyncio.create_task(client_func(list(chunk)), name=task_name))
             logger.debug(f"Создана задача {task_name} для {len(chunk)} символов.")

    if not tasks:
         logger.warning("Не создано ни одной задачи для клиентов бирж.")
         return

    logger.info(f"Запуск {len(tasks)} задач клиентов бирж...")

    # --- Ожидание завершения ---
    done, pending = set(), set()
    try:
        # Ждем вечно, пока все задачи не завершатся (что не должно случиться)
        # или пока не возникнет необработанное исключение в одной из задач.
        await asyncio.gather(*tasks)
        logger.warning("asyncio.gather завершился неожиданно (все задачи завершились?).")

    except asyncio.CancelledError:
         logger.info("Главная задача была отменена (Ctrl+C).")
         pending = tasks # Помечаем все задачи как ожидающие отмены
    except Exception as e:
         logger.critical(f"Критическая ошибка в основном цикле ожидания: {e}", exc_info=True)
         pending = tasks # Помечаем все задачи как ожидающие отмены
    finally:
        # --- Остановка и очистка ---
        logger.info("Начало процесса остановки клиентов...")
        # Собираем задачи, которые еще не завершены
        tasks_to_cancel = pending.union(t for t in tasks if t and not t.done())
        if tasks_to_cancel:
            logger.info(f"Отмена {len(tasks_to_cancel)} работающих задач...")
            for task in tasks_to_cancel:
                task.cancel()
            # Ожидаем завершения отмены
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("Работающие задачи отменены.")
        else:
             logger.info("Нет работающих задач для отмены.")

        # Закрываем пул соединений Redis
        if data_store.redis_client:
            try:
                logger.info("Закрытие пула соединений Redis...")
                await data_store.redis_client.close()
                if data_store.redis_pool:
                    await data_store.redis_pool.disconnect()
                logger.info("Пул соединений Redis закрыт.")
            except Exception as e_redis_close:
                 logger.error(f"Ошибка при закрытии Redis: {e_redis_close}")

        logger.warning("Основная функция main сборщика данных завершилась.")


# --- Точка входа ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена вручную (KeyboardInterrupt).")
    # finally здесь не нужен, т.к. очистка Redis происходит в finally функции main