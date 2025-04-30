from dataclasses import dataclass
from decimal import Decimal # Используем Decimal для цен!

@dataclass
class TickerData:
    """Структура для хранения данных тикера."""
    exchange: str
    symbol: str
    timestamp_ms: int  # Время получения данных от биржи (milliseconds)
    last_price: Decimal | None = None # Последняя цена сделки
    bid_price: Decimal | None = None # Лучшая цена покупки (кто-то хочет купить)
    ask_price: Decimal | None = None # Лучшая цена продажи (кто-то хочет продать)

    # Дополнительные поля можно добавить по необходимости
    # volume_24h: Decimal | None = None