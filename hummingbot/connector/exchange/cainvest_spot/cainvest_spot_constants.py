from hummingbot.core.api_throttler.data_types import RateLimit

DEFAULT_DOMAIN = "production"

HBOT_ORDER_ID_PREFIX = "CS"
MAX_ORDER_ID_LEN = 36

# Environment URLs
ENVIRONMENTS = {
    "sandbox": {
        "rest_url": "https://api.sandbox.spot.cainvest.io/api/v1",
        "ws_url": "wss://api.sandbox.spot.cainvest.io/ws",
    },
    "production": {
        "rest_url": "https://api.spot.cainvest.io/api/v1",
        "ws_url": "wss://api.spot.cainvest.io/ws",
    },
}

# REST endpoints
BALANCE_PATH_URL = "account/balance"
OPERATIONAL_DATES_PATH_URL = "fx/operational-dates"
BUY_QUOTE_PATH_URL = "fx/buy/quote"
SELL_QUOTE_PATH_URL = "fx/sell/quote"
BUY_ACCEPT_QUOTE_PATH_URL = "fx/buy/accept-quote/{}"
SELL_ACCEPT_QUOTE_PATH_URL = "fx/sell/accept-quote/{}"
ORGANIZATION_PATH_URL = "organizations/me"

# Auth headers
API_KEY_HEADER = "X-API-Key"
API_SECRET_HEADER = "X-API-Secret"

# WS message types
WS_LOGIN_REQUEST = "login_request"
WS_LOGIN = "login"
WS_SUCCESS = "success"
WS_ERROR = "error"
WS_PING = "ping"
WS_PONG = "pong"
WS_SUBSCRIBE_REFERENCE = "subscribe_reference"
WS_UNSUBSCRIBE_REFERENCE = "unsubscribe_reference"
WS_REFERENCE_UPDATE = "reference_update"

# WS connection parameters
WS_AUTH_TIMEOUT = 60
WS_PING_INTERVAL = 50
WS_PONG_TIMEOUT = 60
WS_CONNECTION_LIFETIME = 24 * 3600

# Cainvest currency pair format uses "/" (e.g. "USDC/BRL")
# Hummingbot uses "-" (e.g. "USDC-BRL")
AVAILABLE_PAIRS = ["USDC-BRL", "USDT-BRL", "USDC-BRL1", "USDT-BRL1"]
CAINVEST_PAIR_MAP = {
    "USDC-BRL":  "USDC/BRL",
    "USDT-BRL":  "USDT/BRL",
    "USDC-BRL1": "USDC/BRL",   # BRL1 == BRL for minters; same underlying Cainvest pair
    "USDT-BRL1": "USDT/BRL",   # BRL1 == BRL for minters; same underlying Cainvest pair
}
# Explicit reverse map (canonical HB pair per Cainvest symbol).
# BRL1 aliases intentionally omitted here; the order book data source resolves
# the correct alias dynamically from the connector's configured trading_pairs.
HB_PAIR_MAP = {
    "USDC/BRL": "USDC-BRL",
    "USDT/BRL": "USDT-BRL",
}

# Synthetic book configuration defaults (3 levels total: 1 top-of-book + 2 stepped)
BOOK_LEVELS = 2  # stepped levels; top-of-book adds one more = 3 total per side
BOOK_STEP_BPS = 1  # basis points between levels
BOOK_DEFAULT_SIZES = [1_000_000, 500_000]  # [top/step1, step2]
BOOK_PRICE_DECIMALS = 4  # max decimal places for prices
BOOK_SIZE_UNIT_BASE = "base"
BOOK_SIZE_UNIT_QUOTE = "quote"

# Rate limits
ONE_SECOND = 1
ONE_MINUTE = 60

RATE_LIMITS = [
    RateLimit(limit_id=BALANCE_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=OPERATIONAL_DATES_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=BUY_QUOTE_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=SELL_QUOTE_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=BUY_ACCEPT_QUOTE_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=SELL_ACCEPT_QUOTE_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORGANIZATION_PATH_URL, limit=5, time_interval=ONE_SECOND),
    RateLimit(limit_id="general", limit=30, time_interval=ONE_SECOND),
]
