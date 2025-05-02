import asyncio
import logging
import sys

# Импорт НОВОГО модуля данных, конфига и модели
import data_store
from config import (
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
    MEXC_EXCHANGE_NAME,
    KUCOIN_EXCHANGE_NAME,
    # ARBITRAGE_THRESHOLD_PCT, # Больше не нужен здесь
    # HTX_EXCHANGE_NAME,
)
from models import TickerData # Нужен клиентам, которых импортируем

# Импорт КЛИЕНТСКИХ функций
from exchange_clients.bybit_ws import bybit_client
from exchange_clients.binance_ws import binance_client
from exchange_clients.mexc_ws import mexc_client
from exchange_clients.kucoin_ws import kucoin_client
# from exchange_clients.htx_ws import htx_client # HTX пока отключен


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


# === Основная функция запуска ===
async def main():
    """Запускает клиенты бирж для сбора данных и записи в Redis."""
    logger.info("Запуск арбитражного монитора (Запись в Redis)...")
    logger.info(f"Отслеживаемые символы: {SYMBOLS_TO_TRACK}")

    # Проверяем соединение с Redis перед запуском клиентов
    if not await data_store.check_redis_connection():
         logger.critical("Не удалось подключиться к Redis. Завершение работы.")
         return

    # --- Задачи для запуска клиентов бирж ---
    tasks = [
        asyncio.create_task(bybit_client(SYMBOLS_TO_TRACK), name=f"{BYBIT_EXCHANGE_NAME}_client"),
        asyncio.create_task(binance_client(SYMBOLS_TO_TRACK), name=f"{BINANCE_EXCHANGE_NAME}_client"),
        asyncio.create_task(mexc_client(SYMBOLS_TO_TRACK), name=f"{MEXC_EXCHANGE_NAME}_client"),
        asyncio.create_task(kucoin_client(SYMBOLS_TO_TRACK), name=f"{KUCOIN_EXCHANGE_NAME}_client"),
        # asyncio.create_task(htx_client(SYMBOLS_TO_TRACK), name=f"{HTX_EXCHANGE_NAME}_client"),
    ]

    logger.info(f"Запуск {len(tasks)} клиентов бирж...")

    # --- Ожидание завершения (не должно произойти) ---
    # Используем gather для простоты, так как клиенты сами переподключаются
    done, pending = set(), set() # Инициализируем для finally
    try:
        # Ждем завершения всех задач (что маловероятно) или первой ошибки
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        # Если вышли отсюда без исключения, значит одна задача завершилась штатно (что странно)
        for task in done:
             logger.warning(f"Задача {task.get_name()} завершилась штатно (неожиданно).")

    except asyncio.CancelledError:
         logger.info("Главная задача была отменена (возможно, Ctrl+C).")
         # Собираем все исходные задачи для отмены
         pending = tasks
    except Exception as e:
         logger.critical(f"Критическая ошибка в основном цикле ожидания: {e}", exc_info=True)
         # Собираем все исходные задачи для отмены
         pending = tasks
    finally:
        # --- Остановка и очистка ---
        logger.info("Начало процесса остановки клиентов...")
        # Отменяем все задачи, которые еще не завершены (включая pending из wait или все задачи при исключении)
        tasks_to_cancel = pending.union(t for t in tasks if not t.done())
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
                # Закрытие клиента автоматически возвращает соединение в пул
                await data_store.redis_client.close()
                # Закрытие пула (может потребоваться для полного освобождения ресурсов)
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