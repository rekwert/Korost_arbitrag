import asyncio
import logging
import sys
from aiohttp import web
import json
from decimal import Decimal
import shared_data

from config import (
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
    MEXC_EXCHANGE_NAME,
    KUCOIN_EXCHANGE_NAME,
    ARBITRAGE_THRESHOLD_PCT,

)

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


if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def prepare_tickers_for_json(tickers_dict):
    """Преобразует словарь тикеров в JSON-совместимый формат."""
    prepared = {}
    for exchange, symbols in tickers_dict.items():
        prepared[exchange] = {}
        for symbol, ticker_data in symbols.items():
            if ticker_data: # Проверяем, что данные тикера существуют
                prepared[exchange][symbol] = {
                    # Конвертируем Decimal в строку для JSON
                    "bid_price": str(ticker_data.bid_price) if ticker_data.bid_price is not None else None,
                    "ask_price": str(ticker_data.ask_price) if ticker_data.ask_price is not None else None,
                    "last_price": str(ticker_data.last_price) if ticker_data.last_price is not None else None,
                    "timestamp_ms": ticker_data.timestamp_ms,
                    # Добавим имя биржи и символ для удобства на клиенте
                    "exchange": ticker_data.exchange,
                    "symbol": ticker_data.symbol
                }
            else:
                 prepared[exchange][symbol] = None # Если по какой-то причине там None
    return prepared

async def handle_get_tickers(request: web.Request):
    """Возвращает текущие данные тикеров в формате JSON."""
    logger.debug("Получен HTTP запрос /tickers")
    try:
        # Используем shared_data.latest_tickers для получения актуальных данных
        tickers_to_send = prepare_tickers_for_json(shared_data.latest_tickers)
        return web.json_response(tickers_to_send)
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса /tickers: {e}", exc_info=True)
        # Возвращаем ошибку сервера
        return web.json_response({"error": "Internal server error"}, status=500)

async def main():
    """Запускает клиенты бирж и веб-сервер для дашборда."""
    logger.info("Запуск арбитражного монитора...")
    logger.info(f"Отслеживаемые символы: {SYMBOLS_TO_TRACK}")
    logger.info(f"Порог арбитража: {ARBITRAGE_THRESHOLD_PCT}%")

    # --- Настройка Веб-сервера ---
    app = web.Application()
    app.router.add_get('/tickers', handle_get_tickers)
    runner = web.AppRunner(app)
    try:
        await runner.setup()
        logger.info("AppRunner успешно настроен.")
    except Exception as e_setup:
         logger.critical(f"Критическая ошибка при настройке AppRunner: {e_setup}", exc_info=True)
         return

    api_port = 9000 # Используем порт 9000, раз другие не работали
    site = web.TCPSite(runner, 'localhost', api_port)

    # --- Задачи для запуска ---
    ws_client_tasks = [
        asyncio.create_task(bybit_client(SYMBOLS_TO_TRACK), name=f"{BYBIT_EXCHANGE_NAME}_client"),
        asyncio.create_task(binance_client(SYMBOLS_TO_TRACK), name=f"{BINANCE_EXCHANGE_NAME}_client"),
        asyncio.create_task(mexc_client(SYMBOLS_TO_TRACK), name=f"{MEXC_EXCHANGE_NAME}_client"),
        asyncio.create_task(kucoin_client(SYMBOLS_TO_TRACK), name=f"{KUCOIN_EXCHANGE_NAME}_client"),
    ]

    # Создаем и ПРОВЕРЯЕМ задачу сервера
    server_task = asyncio.create_task(site.start(), name="API_Server")
    logger.info(f"Задача API_Server создана: {server_task}")
    await asyncio.sleep(0.1) # Пауза

    if server_task.done():
        logger.warning(f"Задача API_Server завершилась СРАЗУ после создания!")
        try:
            server_task.result() # Попробуем получить исключение
        except Exception as e_server_start:
            # Логируем ошибку, которая вызвала завершение задачи
            logger.error(f"Ошибка при запуске API_Server: {e_server_start}", exc_info=True)
        # Не выходим, попробуем запустить без сервера, если он упал сразу
        logger.warning("API Сервер не запустился, продолжаю без него...")
        tasks = ws_client_tasks # Запускаем только WebSocket клиенты
    else:
        logger.info(f"API сервер успешно запущен на http://localhost:{api_port}/tickers ...")
        tasks = ws_client_tasks + [server_task] # Добавляем рабочую задачу сервера

    logger.info(f"Запуск {len(ws_client_tasks)} клиентов бирж...")

    # --- Основной цикл ожидания ---
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # --- Остановка и очистка ---
    for task in done:
        # ... (логирование завершенных задач) ...
        pass

    logger.info("Начало процесса остановки...")
    logger.info(f"Отмена {len(pending)} ожидающих задач...")
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("Ожидающие задачи отменены.")

    # Останавливаем веб-сервер (только если он был запущен и не упал сразу)
    if not server_task.done():
        logger.info("Остановка API сервера...")
        try:
            await runner.cleanup()
            logger.info("API сервер успешно остановлен.")
        except Exception as e_cleanup:
            logger.error(f"Ошибка при остановке API сервера: {e_cleanup}", exc_info=True)
    else:
        logger.info("API сервер не был запущен или упал, очистка не требуется.")


    logger.warning("Основная функция main завершилась.")


if __name__ == "__main__":
    try:
        # Переопределяем обработчик JSON для Decimal
        original_dumps = json.dumps
        def decimal_default_serializer(obj):
            if isinstance(obj, Decimal):
                # Конвертируем Decimal в строку
                return str(obj)
            # Если не Decimal, используем стандартный обработчик
            # (но стандартный обработчик вызовет TypeError, если есть другие несериализуемые типы)
            # Лучше выбросить исключение, чтобы понять, какие еще типы нужно обработать
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        # Это глобально изменит поведение json.dumps, что не всегда хорошо,
        # но для простого случая сработает.
        # json.dumps = lambda obj, **kwargs: original_dumps(obj, default=decimal_default_serializer, **kwargs)

        # Более безопасный подход - передавать default в web.json_response,
        # но это требует кастомизации самого json_response или подготовки данных заранее.
        # Пока оставим prepare_tickers_for_json.

        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена вручную (KeyboardInterrupt).")
    except Exception as e:
        logger.critical("Критическая неперехваченная ошибка в главном потоке:", exc_info=True)