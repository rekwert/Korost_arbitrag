import logging
from decimal import Decimal

# Импортируем нужные типы и константы
from models import TickerData
from config import (
    SYMBOLS_TO_TRACK,
    BYBIT_EXCHANGE_NAME,
    BINANCE_EXCHANGE_NAME,
    MEXC_EXCHANGE_NAME,
    # Добавь KuCoin/HTX позже
    ARBITRAGE_THRESHOLD_PCT, # Перенесем порог сюда
)

logger = logging.getLogger(__name__)

# --- Глобальное хранилище последних данных ---
# Теперь это центральное хранилище, доступное через этот модуль
latest_tickers: dict[str, dict[str, TickerData]] = {
    BYBIT_EXCHANGE_NAME: {},
    BINANCE_EXCHANGE_NAME: {},
    MEXC_EXCHANGE_NAME: {},
    # Добавь сюда KuCoin/HTX по мере их реализации
}

# === Логика поиска арбитража ===
# Теперь эта функция тоже живет здесь и работает с latest_tickers из этого модуля
def find_arbitrage_opportunities():
    """Ищет возможности для арбитража на основе данных в latest_tickers."""
    # Собираем все биржи, для которых есть данные по символам
    active_exchanges = list(latest_tickers.keys()) # Берем ключи прямо отсюда

    for symbol in SYMBOLS_TO_TRACK:
        # Получаем данные по символу для всех активных бирж
        symbol_tickers = {}
        valid_exchanges_for_symbol = []

        for ex_name in active_exchanges:
            # Проверяем наличие ключа биржи перед доступом
            if ex_name not in latest_tickers:
                 logger.warning(f"Биржа {ex_name} есть в списке, но отсутствует в latest_tickers.")
                 continue

            ticker = latest_tickers[ex_name].get(symbol)
            if ticker and ticker.bid_price is not None and ticker.ask_price is not None:
                symbol_tickers[ex_name] = ticker
                valid_exchanges_for_symbol.append(ex_name)
            else:
                logger.debug(f"[{symbol}] Отсутствуют полные данные Bid/Ask для {ex_name}.")

        if len(valid_exchanges_for_symbol) < 2:
             logger.debug(f"[{symbol}] Недостаточно данных для сравнения (нужно >= 2 бирж с ценами).")
             continue

        # Сравниваем все пары бирж
        for i in range(len(valid_exchanges_for_symbol)):
            for j in range(i + 1, len(valid_exchanges_for_symbol)):
                ex1_name = valid_exchanges_for_symbol[i]
                ex2_name = valid_exchanges_for_symbol[j]
                ticker1 = symbol_tickers[ex1_name]
                ticker2 = symbol_tickers[ex2_name]

                try:
                    profit_1_2_pct = ((ticker2.bid_price - ticker1.ask_price) / ticker1.ask_price) * 100
                except ZeroDivisionError:
                     profit_1_2_pct = Decimal('-inf')
                try:
                    profit_2_1_pct = ((ticker1.bid_price - ticker2.ask_price) / ticker2.ask_price) * 100
                except ZeroDivisionError:
                    profit_2_1_pct = Decimal('-inf')

                log_prices = (
                    f"{ex1_name}(B:{ticker1.bid_price} A:{ticker1.ask_price}) | "
                    f"{ex2_name}(B:{ticker2.bid_price} A:{ticker2.ask_price})"
                )
                arbitrage_found = False

                if profit_1_2_pct >= ARBITRAGE_THRESHOLD_PCT:
                    logger.info(
                        f"[АРБИТРАЖ] [{symbol}] Купить {ex1_name} @{ticker1.ask_price}, "
                        f"Продать {ex2_name} @{ticker2.bid_price}. "
                        f"Спред: {profit_1_2_pct:.4f}% | {log_prices}"
                    )
                    arbitrage_found = True
                if profit_2_1_pct >= ARBITRAGE_THRESHOLD_PCT:
                    logger.info(
                        f"[АРБИТРАЖ] [{symbol}] Купить {ex2_name} @{ticker2.ask_price}, "
                        f"Продать {ex1_name} @{ticker1.bid_price}. "
                        f"Спред: {profit_2_1_pct:.4f}% | {log_prices}"
                    )
                    arbitrage_found = True

                if not arbitrage_found:
                    logger.debug(
                        f"[{symbol}] Спред {ex1_name}->{ex2_name}: {profit_1_2_pct:.4f}%, "
                        f"{ex2_name}->{ex1_name}: {profit_2_1_pct:.4f}% | {log_prices}"
                    )