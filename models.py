from dataclasses import dataclass, field # Импортируем field
from decimal import Decimal

@dataclass
class TickerData:
    """Структура для хранения данных тикера."""
    exchange: str
    symbol: str
    timestamp_ms: int
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    last_price: Decimal | None = None

    # Добавляем размеры лучших заявок
    # Используем field(default=None), чтобы избежать проблем с порядком аргументов
    bid_size: Decimal | None = field(default=None)
    ask_size: Decimal | None = field(default=None)