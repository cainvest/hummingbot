"""
Cainvest Spot OTC Exchange connector.

Hybrid model: synthetic order book from WS reference prices,
execution via RFQ quote + accept on REST API.
Balance from /account/balance including trading line.
Settlement date auto-selected via /fx/operational-dates (D0 preferred, D1 fallback).
"""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Tuple

import aiohttp
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.cainvest_spot import cainvest_spot_balance_logic as cainvest_balance_logic
from hummingbot.connector.exchange.cainvest_spot import cainvest_spot_constants as CONSTANTS
from hummingbot.connector.exchange.cainvest_spot.cainvest_spot_api_order_book_data_source import (
    CainvestSpotOrderBookDataSource,
)
from hummingbot.connector.exchange.cainvest_spot.cainvest_spot_auth import CainvestSpotAuth
from hummingbot.connector.exchange.cainvest_spot import cainvest_spot_web_utils as web_utils
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

s_logger = None
s_decimal_0 = Decimal("0")


class CainvestSpotExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = web_utils

    def __init__(
        self,
        cainvest_spot_api_key: str = "",
        cainvest_spot_api_secret: str = "",
        cainvest_spot_environment: str = "production",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        **kwargs,
    ):
        self._api_key = cainvest_spot_api_key
        self._api_secret = cainvest_spot_api_secret
        self._environment = cainvest_spot_environment if cainvest_spot_environment in ("sandbox", "production") else "production"
        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required
        self._domain = domain

        # Synthetic book config
        self._book_sizes = list(CONSTANTS.BOOK_DEFAULT_SIZES)
        self._book_size_unit = CONSTANTS.BOOK_SIZE_UNIT_BASE

        # Cached operational dates
        self._cached_settlement: Optional[str] = None
        self._settlement_cache_ts: float = 0
        self._settlement_cache_ttl: float = 300  # refresh every 5 min

        # Trading line info
        self._trading_line_available_brl: Decimal = Decimal("0")
        self._trading_line_limit_brl: Decimal = Decimal("0")
        self._executions_by_client_order_id: Dict[str, Dict[str, Any]] = {}
        self._warned_missing_ws_mid_for_trading_line: bool = False

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ── Properties ────────────────────────────────────────────────────

    @property
    def authenticator(self) -> AuthBase:
        return CainvestSpotAuth(
            api_key=self._api_key,
            api_secret=self._api_secret,
            time_provider=self._time_synchronizer,
        )

    @property
    def name(self) -> str:
        return "cainvest_spot"

    @property
    def environment(self) -> str:
        return self._environment

    @property
    def rest_url(self) -> str:
        return CONSTANTS.ENVIRONMENTS.get(self._environment, CONSTANTS.ENVIRONMENTS["production"])["rest_url"]

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.ORGANIZATION_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.ORGANIZATION_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.OPERATIONAL_DATES_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def book_sizes(self) -> List[int]:
        return self._book_sizes

    @property
    def book_size_unit(self) -> str:
        return self._book_size_unit

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "symbols_mapping_initialized": True,
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": not self.is_trading_required or len(self._account_balances) > 0,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self.is_trading_required else True,
            "user_stream_initialized": True,
        }

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.MARKET, OrderType.LIMIT]

    # ── Factory methods ───────────────────────────────────────────────

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return WebAssistantsFactory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CainvestSpotOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> Optional[UserStreamTrackerDataSource]:
        return None

    def _create_user_stream_tracker(self):
        return None

    def _create_user_stream_tracker_task(self):
        return None

    def _is_user_stream_initialized(self) -> bool:
        return True

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    # ── REST helpers ──────────────────────────────────────────────────

    async def _api_get(self, path: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.rest_url}/{path}"
        headers = {
            CONSTANTS.API_KEY_HEADER: self._api_key,
            CONSTANTS.API_SECRET_HEADER: self._api_secret,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _api_post(self, path: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.rest_url}/{path}"
        headers = {
            CONSTANTS.API_KEY_HEADER: self._api_key,
            CONSTANTS.API_SECRET_HEADER: self._api_secret,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()

    @staticmethod
    def _extract_payload(response: Any) -> Dict[str, Any]:
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, dict):
                return data
            return response
        return {}

    # ── Settlement date logic ─────────────────────────────────────────

    async def _get_settlement_date(self) -> str:
        """Return D0 if allowed, otherwise D1 when allowed."""
        now = time.time()
        if self._cached_settlement and (now - self._settlement_cache_ts) < self._settlement_cache_ttl:
            return self._cached_settlement

        try:
            data = await self._api_get(CONSTANTS.OPERATIONAL_DATES_PATH_URL)
            if isinstance(data, dict):
                payload = data.get("data", data)
                d0_allowed = bool(payload.get("d0_allowed"))
                d1_allowed = bool(payload.get("d1_allowed"))
                if d0_allowed:
                    self._cached_settlement = "D0"
                elif d1_allowed:
                    self._cached_settlement = "D1"
                else:
                    raise ValueError(
                        f"No operational settlement date available in payload: {payload}"
                    )
            else:
                raise ValueError(
                    f"Unexpected operational-dates response format: {type(data).__name__}"
                )
        except Exception as e:
            self.logger().warning(f"Failed to fetch operational-dates, defaulting to D1: {e}")
            self._cached_settlement = "D1"

        self._settlement_cache_ts = now
        return self._cached_settlement

    def get_available_balance(self, currency: str) -> Decimal:
        """
        Wallet snapshot from REST plus, for base assets of *-BRL pairs, notional base from
        trading_line.available_brl using current WS mid (so BudgetChecker is not stuck at
        wallet USDC until the next balance poll after reference prices arrive).
        """
        available = super().get_available_balance(currency)
        if self._trading_line_available_brl <= 0:
            return available
        ob_ds = getattr(self, "_orderbook_ds", None)
        if ob_ds is None:
            return available
        last_prices = getattr(ob_ds, "last_prices", None) or {}
        ref = cainvest_balance_logic.reference_mid_brl_per_base_for_trading_line(
            self._trading_pairs, last_prices
        )
        if ref is None:
            return available
        mid_brl_per_base, base_symbol = ref
        if base_symbol != str(currency).upper():
            return available
        if mid_brl_per_base <= 0:
            return available
        synthetic = self._trading_line_available_brl / mid_brl_per_base
        return max(available, synthetic)

    # ── Balance ───────────────────────────────────────────────────────

    async def _update_balances(self):
        try:
            data = await self._api_get(CONSTANTS.BALANCE_PATH_URL)
            payload = data.get("data", data) if isinstance(data, dict) else {}

            local_balances = {}

            settlement = payload.get("settlement_balances", {})
            free_balances = settlement.get("free_balances", [])
            for entry in free_balances:
                currency = str(entry.get("currency", "")).upper()
                amount = Decimal(str(entry.get("amount", 0)))
                local_balances[currency] = local_balances.get(currency, Decimal("0")) + amount

            for entry in payload.get("balances", []):
                currency = str(entry.get("currency", "")).upper()
                amount = Decimal(str(entry.get("amount", 0)))
                if currency not in local_balances:
                    local_balances[currency] = amount

            trading_line = payload.get("trading_line", {})
            if isinstance(trading_line, dict):
                self._trading_line_available_brl = Decimal(str(trading_line.get("available_brl", 0)))
                self._trading_line_limit_brl = Decimal(str(trading_line.get("limit_brl", 0)))
                if "BRL" not in local_balances or local_balances["BRL"] < self._trading_line_available_brl:
                    local_balances["BRL"] = self._trading_line_available_brl

            # Notional base (USDC/USDT) from trading line is applied in get_available_balance
            # using live WS mids, not here — avoids stale ~1 USDC until the next REST poll.

            self._account_available_balances.clear()
            self._account_balances.clear()
            for currency, amount in local_balances.items():
                self._account_balances[currency] = amount
                self._account_available_balances[currency] = amount

            blocked_balances = settlement.get("blocked_balances", [])
            for entry in blocked_balances:
                currency = str(entry.get("currency", "")).upper()
                amount = Decimal(str(entry.get("amount", 0)))
                self._account_balances[currency] = self._account_balances.get(currency, Decimal("0")) + amount

        except Exception as e:
            self.logger().error(f"Error updating balances: {e}", exc_info=True)

    # ── Trading rules ─────────────────────────────────────────────────

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        rules = []
        for pair in self._trading_pairs:
            rules.append(TradingRule(
                trading_pair=pair,
                min_order_size=Decimal("1"),
                max_order_size=Decimal("10000000"),
                min_price_increment=Decimal("0.0001"),
                min_base_amount_increment=Decimal("0.01"),
                min_notional_size=Decimal("1"),
                supports_limit_orders=True,
                supports_market_orders=True,
            ))
        return rules

    async def _update_trading_rules(self):
        try:
            rules = await self._format_trading_rules({})
            self._trading_rules.clear()
            for rule in rules:
                self._trading_rules[rule.trading_pair] = rule
        except Exception as e:
            self.logger().error(f"Error updating trading rules: {e}")

    # ── Trading pair symbol map ───────────────────────────────────────

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """Build symbol map from the connector's configured trading pairs (exchange_info can be ignored).
        Using self._trading_pairs (instead of AVAILABLE_PAIRS) avoids bidict key collisions when
        two HB aliases (e.g. USDC-BRL and USDC-BRL1) map to the same Cainvest symbol."""
        mapping = bidict()
        for pair in self._trading_pairs:
            cainvest_pair = CONSTANTS.CAINVEST_PAIR_MAP.get(pair, pair.replace("-", "/"))
            mapping[cainvest_pair] = pair
        self._set_trading_pair_symbol_map(mapping)

    async def _initialize_trading_pair_symbol_map(self):
        exchange_info = await self._make_trading_pairs_request()
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    def trading_pair_symbol_map_ready(self) -> bool:
        return True

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        ob_ds: CainvestSpotOrderBookDataSource = self._orderbook_ds
        prices = ob_ds.last_prices.get(trading_pair)
        if prices and prices.get("buy") and prices.get("sell"):
            mid = (prices["buy"] + prices["sell"]) / 2
            return float(mid)
        return 0.0

    async def get_last_traded_prices(self, trading_pairs: List[str] = None) -> Dict[str, float]:
        trading_pairs = trading_pairs or self._trading_pairs
        if not trading_pairs:
            return {}

        # Ensure the requested pairs are tracked by this connector instance.
        for trading_pair in trading_pairs:
            if trading_pair not in self._trading_pairs:
                self._trading_pairs.append(trading_pair)

        tracker = self.order_book_tracker
        stream_task = getattr(tracker, "_order_book_stream_listener_task", None)
        if stream_task is None or stream_task.done():
            tracker.start()

        # Ensure dynamic pair subscription happens on the active WS when needed.
        for trading_pair in trading_pairs:
            if trading_pair not in tracker.order_books:
                await tracker.add_trading_pair(trading_pair)

        ob_ds: CainvestSpotOrderBookDataSource = self._orderbook_ds
        await asyncio.gather(*(ob_ds.wait_for_price(trading_pair) for trading_pair in trading_pairs))

        result: Dict[str, float] = {}
        for trading_pair in trading_pairs:
            result[trading_pair] = await self._get_last_traded_price(trading_pair)
        return result

    # ── Order placement (RFQ) ─────────────────────────────────────────

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        settlement = await self._get_settlement_date()
        cainvest_pair = CONSTANTS.CAINVEST_PAIR_MAP.get(trading_pair, trading_pair.replace("-", "/"))

        if trade_type == TradeType.BUY:
            quote_path = CONSTANTS.BUY_QUOTE_PATH_URL
            accept_path = CONSTANTS.BUY_ACCEPT_QUOTE_PATH_URL
        else:
            quote_path = CONSTANTS.SELL_QUOTE_PATH_URL
            accept_path = CONSTANTS.SELL_ACCEPT_QUOTE_PATH_URL

        quote_params = {
            "currency_pair": cainvest_pair,
            "settlement_date": settlement,
            "size": str(amount),
        }

        self.logger().info(
            f"RFQ {trade_type.name} | pair={cainvest_pair} size={amount} settlement={settlement}"
        )

        quote_raw = await self._api_get(quote_path, params=quote_params)
        quote = self._extract_payload(quote_raw)
        quote_id = quote.get("id")
        if not quote_id:
            raise ValueError(f"Quote response missing id: {quote_raw}")

        rate = Decimal(str(quote.get("rate", "0")))
        self.logger().info(f"RFQ quote received | id={quote_id} rate={rate}")

        accept_url = accept_path.format(quote_id)
        trade_raw = await self._api_post(accept_url)
        trade = self._extract_payload(trade_raw)

        trade_uuid = trade.get("trade_uuid", quote_id)
        fill_timestamp = time.time()
        self._executions_by_client_order_id[order_id] = {
            "exchange_order_id": str(trade_uuid),
            "rate": rate,
            "fill_timestamp": fill_timestamp,
        }
        self.logger().info(f"RFQ accepted | trade_uuid={trade_uuid}")

        return str(trade_uuid), fill_timestamp

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        """
        OTC orders are immediately executed after quote acceptance.
        We process OPEN -> FILLED synchronously so order state never remains stale/open.
        """
        exchange_order_id, update_timestamp = await self._place_order(
            order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            amount=order.amount,
            trade_type=order.trade_type,
            order_type=order.order_type,
            price=order.price,
            **kwargs,
        )

        open_update = OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id),
            trading_pair=order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=OrderState.OPEN,
        )
        await self._order_tracker._process_order_update(open_update)

        execution = self._executions_by_client_order_id.get(order.client_order_id, {})
        fill_price = Decimal(str(execution.get("rate", "0")))
        if fill_price <= 0:
            fallback_price = await self._get_last_traded_price(order.trading_pair)
            fill_price = Decimal(str(fallback_price))
        fill_timestamp = float(execution.get("fill_timestamp", update_timestamp))
        base, quote = order.trading_pair.split("-")
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount(token=quote, amount=Decimal("0"))])
        trade_update = TradeUpdate(
            trade_id=f"{exchange_order_id}_fill",
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=order.amount,
            fill_quote_amount=order.amount * fill_price,
            fill_price=fill_price,
            fill_timestamp=fill_timestamp,
        )
        self._order_tracker.process_trade_update(trade_update)

        filled_update = OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id),
            trading_pair=order.trading_pair,
            update_timestamp=fill_timestamp,
            new_state=OrderState.FILLED,
        )
        await self._order_tracker._process_order_update(filled_update)

        return str(exchange_order_id)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        return True

    # ── Order updates (OTC orders are instant) ────────────────────────

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.current_state in (OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED):
            return []

        _, quote = order.trading_pair.split("-")
        execution = self._executions_by_client_order_id.get(order.client_order_id, {})
        fill_price = Decimal(str(execution.get("rate", "0")))
        if fill_price <= 0:
            fallback_price = order.price if order.price and order.price > 0 else await self._get_last_traded_price(order.trading_pair)
            fill_price = Decimal(str(fallback_price))
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount(token=quote, amount=Decimal("0"))])
        fill_timestamp = float(execution.get("fill_timestamp", order.creation_timestamp))
        exchange_order_id = str(execution.get("exchange_order_id", order.exchange_order_id))

        return [TradeUpdate(
            trade_id=f"{exchange_order_id}_fill",
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=order.amount,
            fill_quote_amount=order.amount * fill_price,
            fill_price=fill_price,
            fill_timestamp=fill_timestamp,
        )]

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=time.time(),
            new_state=OrderState.FILLED,
        )

    async def _update_order_fills_from_trades(self):
        pass

    async def _user_stream_event_listener(self):
        pass

    # ── Network ───────────────────────────────────────────────────────

    async def _make_network_check_request(self):
        await self._api_get(CONSTANTS.OPERATIONAL_DATES_PATH_URL)

    async def _make_trading_rules_request(self) -> Any:
        return {}

    async def _make_trading_pairs_request(self) -> Any:
        return {}

    # ── Misc required overrides ───────────────────────────────────────

    async def _update_trading_fees(self):
        pass

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> AddedToCostTradeFee:
        return AddedToCostTradeFee(
            percent=Decimal("0"),
            flat_fees=[],
        )

    async def cancel_all(self, timeout_seconds: float) -> List:
        return []
