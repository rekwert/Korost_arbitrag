import asyncio
import logging

from config import (
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
    MEXC_EXCHANGE_NAME,
    ARBITRAGE_THRESHOLD_PCT, # Порог теперь в config
    # Добавь импорты имен для KuCoin/HTX позже
)

from exchange_clients.bybit_ws import bybit_client
from exchange_clients.binance_ws import binance_client
from exchange_clients.mexc_ws import mexc_client
# from exchange_clients.kucoin_ws import kucoin_client # Для будущего
# from exchange_clients.htx_ws import htx_client     # Для будущего


# --- Настройка Логгирования ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logging.getLogger("websockets").setLevel(logging.INFO)
logging.getLogger("asyncio").setLevel(logging.INFO)




# === Основная функция запуска ===

async def main():
    """Запускает клиенты для всех бирж и управляет ими."""
    logger.info("Запуск арбитражного монитора...")
    logger.info(f"Отслеживаемые символы: {SYMBOLS_TO_TRACK}")
    logger.info(f"Порог арбитража: {ARBITRAGE_THRESHOLD_PCT}%")

    # Задачи для запуска клиентов
    tasks = [
        asyncio.create_task(
            bybit_client(SYMBOLS_TO_TRACK),
            name=f"{BYBIT_EXCHANGE_NAME}_client"
        ),
        asyncio.create_task(
            binance_client(SYMBOLS_TO_TRACK),
            name=f"{BINANCE_EXCHANGE_NAME}_client"
        ),
        asyncio.create_task(
            mexc_client(SYMBOLS_TO_TRACK),
            name=f"{MEXC_EXCHANGE_NAME}_client"
        ),

    ]

    logger.info(f"Запуск {len(tasks)} задач с помощью asyncio.gather...")
    try:

        await asyncio.gather(*tasks)
    except Exception as e:
        logger.critical(f"Критическая ошибка в asyncio.gather: {e}", exc_info=True)

    logger.warning("Основная функция main неожиданно завершилась.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена вручную (KeyboardInterrupt).")
    except Exception as e:
        # Ловим любые другие неперехваченные исключения на верхнем уровне
        logger.critical("Критическая неперехваченная ошибка в главном потоке:", exc_info=True)