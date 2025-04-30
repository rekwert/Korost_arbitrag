# === WebSocket Endpoints ===
BYBIT_SPOT_PUBLIC_V5_ENDPOINT = "wss://stream.bybit.com/v5/public/spot"
BINANCE_SPOT_STREAM_ENDPOINT = "wss://stream.binance.com:9443/ws" # Базовый URL для Binance
MEXC_SPOT_PUBLIC_V3_ENDPOINT = "wss://wbs.mexc.com/ws"
KUCOIN_BASE_REST_URL = "https://api.kucoin.com"

# === Отслеживаемые символы ===
# Пока только один для простоты
SYMBOLS_TO_TRACK = ["BTCUSDT"]

# === Константы для Бирж ===
BYBIT_EXCHANGE_NAME = "Bybit"
BINANCE_EXCHANGE_NAME = "Binance"
MEXC_EXCHANGE_NAME = "MEXC"
KUCOIN_EXCHANGE_NAME = "KuCoin"

BYBIT_ORDERBOOK_DEPTH = 1

ARBITRAGE_THRESHOLD_PCT = 0.1  # 0.1%