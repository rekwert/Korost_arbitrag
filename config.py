from decimal import Decimal



BYBIT_SPOT_PUBLIC_V5_ENDPOINT = "wss://stream.bybit.com/v5/public/spot"
BINANCE_SPOT_STREAM_ENDPOINT = "wss://stream.binance.com:9443/ws"
MEXC_SPOT_PUBLIC_V3_ENDPOINT = "wss://wbs.mexc.com/ws"
KUCOIN_BASE_REST_URL = "https://api.kucoin.com"
#HTX_SPOT_WS_ENDPOINT = "wss://api.huobi.pro/ws"

# === Отслеживаемые символы ===
# Пока только один для простоты
SYMBOLS_TO_TRACK = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT","ADAUSDT", "DOGEUSDT",  "DOTUSDT",  "LTCUSDT","AVAXUSDT", "NEARUSDT", "UNIUSDT", "TRXUSDT", "BNBUSDT","KCSUSDT", "SUIUSDT", "TONUSDT", "PEPEUSDT","APTUSDT", "TRUMPUSDT", "MNTUSDT", "ADAUSDT", "LINKUSDT","ETCUSDT", "ATOMUSDT","ARBUSDT", "BONKUSDT", "POLUSDT", "ENAUSDT", "JLPUSDT", "RAYUSDT","VIRTUALUSDT","BCHUSDT","XLMUSDT","AAVEUSDT","RENDERUSDT", "UNIUSDT", "SEIUSDT", "TAOUSDT"]

# === Константы для Бирж ===
BYBIT_EXCHANGE_NAME = "Bybit"
BINANCE_EXCHANGE_NAME = "Binance"
MEXC_EXCHANGE_NAME = "MEXC"
KUCOIN_EXCHANGE_NAME = "KuCoin"
#HTX_EXCHANGE_NAME = "HTX"

BYBIT_ORDERBOOK_DEPTH = 1

# === Настройки Redis ===
REDIS_URL = "redis://localhost:6379/0" # Стандартный URL для локального Redis, база 0

# === Порог для арбитража ===
ARBITRAGE_THRESHOLD_PCT = Decimal('0.1') # Используем Decimal здесь

WS_CHUNK_SIZE = 8