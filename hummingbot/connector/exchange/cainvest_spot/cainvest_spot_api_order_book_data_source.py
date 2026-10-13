"""
Cainvest Spot order book data source.

Connects to the Cainvest WS to receive reference prices and builds a synthetic
3-level order book per side, anchored on the real OTC bid/ask with 1 bps steps.
"""
import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import websockets

from hummingbot.connector.exchange.cainvest_spot import cainvest_spot_constants as CONSTANTS
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.cainvest_spot.cainvest_spot_exchange import CainvestSpotExchange

ONE_BPS = Decimal("0.0001")


class CainvestSpotOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CainvestSpotExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        self._ws_task: Optional[asyncio.Task] = None
        self._active_ws = None
        self._last_prices: Dict[str, Dict[str, Decimal]] = {}
        self._update_id = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(cls.__name__)
        return cls._logger

    @property
    def last_prices(self) -> Dict[str, Dict[str, Decimal]]:
        return self._last_prices

    def _round_price(self, price: Decimal) -> float:
        """Round price to BOOK_PRICE_DECIMALS (4 decimal places)."""
        decimals = CONSTANTS.BOOK_PRICE_DECIMALS
        quantize = Decimal("0.0001") if decimals == 4 else Decimal(10) ** (-decimals)
        return float(price.quantize(quantize))

    def _build_synthetic_book(self, buy_price: Decimal, sell_price: Decimal) -> Dict[str, List]:
        """
        Build a 3-level synthetic book (1 top-of-book + 2 stepped) anchored on OTC bid/ask.

        In the Cainvest OTC context:
        - buy_price = rate at which client can BUY (ask in HB terms)
        - sell_price = rate at which client can SELL (bid in HB terms)
        Prices are rounded to 4 decimal places.
        """
        bids = []
        asks = []
        sizes = self._connector.book_sizes

        for i in range(CONSTANTS.BOOK_LEVELS):
            step = ONE_BPS * (i + 1)
            bid_price = sell_price * (Decimal("1") - step)
            ask_price = buy_price * (Decimal("1") + step)
            size = Decimal(str(sizes[i])) if i < len(sizes) else Decimal(str(sizes[-1]))

            if self._connector.book_size_unit == CONSTANTS.BOOK_SIZE_UNIT_QUOTE:
                bid_size = size / bid_price if bid_price > 0 else Decimal("0")
                ask_size = size / ask_price if ask_price > 0 else Decimal("0")
            else:
                bid_size = size
                ask_size = size

            bids.append([self._round_price(bid_price), float(bid_size)])
            asks.append([self._round_price(ask_price), float(ask_size)])

        # Level 0 (top-of-book) = the actual OTC reference
        top_size = Decimal(str(sizes[0])) if sizes else Decimal("1000000")
        if self._connector.book_size_unit == CONSTANTS.BOOK_SIZE_UNIT_QUOTE:
            top_bid_size = top_size / sell_price if sell_price > 0 else Decimal("0")
            top_ask_size = top_size / buy_price if buy_price > 0 else Decimal("0")
        else:
            top_bid_size = top_size
            top_ask_size = top_size

        bids.insert(0, [self._round_price(sell_price), float(top_bid_size)])
        asks.insert(0, [self._round_price(buy_price), float(top_ask_size)])

        return {"bids": bids, "asks": asks}

    async def get_new_order_book(self, trading_pair: str):
        from hummingbot.core.data_type.order_book import OrderBook
        snapshot = await self._request_order_book_snapshot(trading_pair)
        snapshot_msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": self._next_update_id(),
                "bids": snapshot.get("bids", []),
                "asks": snapshot.get("asks", []),
            },
            timestamp=time.time(),
        )
        order_book = OrderBook()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        if trading_pair not in self._trading_pairs:
            self._trading_pairs.append(trading_pair)
            if self._active_ws is not None:
                await self.subscribe_to_trading_pair(trading_pair)

        prices = self._last_prices.get(trading_pair)
        if prices and prices.get("buy") and prices.get("sell"):
            return self._build_synthetic_book(prices["buy"], prices["sell"])
        return {"bids": [], "asks": []}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": self._next_update_id(),
                "bids": snapshot.get("bids", []),
                "asks": snapshot.get("asks", []),
            },
            timestamp=time.time(),
        )

    async def listen_for_subscriptions(self):
        while True:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"WS error, reconnecting in 5s: {e}")
                await asyncio.sleep(5)

    async def _connect_and_listen(self):
        env = self._connector.environment
        ws_url = CONSTANTS.ENVIRONMENTS.get(env, CONSTANTS.ENVIRONMENTS["production"])["ws_url"]

        async with websockets.connect(ws_url) as ws:
            self._active_ws = ws
            # Keep compatibility with API diagnostics that checks _ws_assistant.
            self._ws_assistant = ws
            # Wait for login_request
            raw = await asyncio.wait_for(ws.recv(), timeout=CONSTANTS.WS_AUTH_TIMEOUT)
            msg = json.loads(raw)
            if msg.get("type") != CONSTANTS.WS_LOGIN_REQUEST:
                self.logger().warning(f"Expected login_request, got: {msg.get('type')}")

            # Send credentials
            auth_payload = self._connector.authenticator.get_ws_auth_payload()
            await ws.send(json.dumps(auth_payload))

            # Wait for auth response
            raw = await asyncio.wait_for(ws.recv(), timeout=CONSTANTS.WS_AUTH_TIMEOUT)
            msg = json.loads(raw)
            if msg.get("type") == CONSTANTS.WS_ERROR:
                self.logger().error(f"WS auth failed: {msg.get('message')}")
                await asyncio.sleep(30)
                return

            # Subscribe to reference updates
            cainvest_pairs = [
                CONSTANTS.CAINVEST_PAIR_MAP.get(tp, tp.replace("-", "/"))
                for tp in self._trading_pairs
            ]
            await ws.send(json.dumps({
                "type": CONSTANTS.WS_SUBSCRIBE_REFERENCE,
                "currency_pairs": cainvest_pairs,
            }))

            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            msg = json.loads(raw)
            if msg.get("type") == CONSTANTS.WS_ERROR:
                self.logger().error(f"WS subscribe failed: {msg.get('message')}")
                return

            self.logger().info(f"Cainvest WS connected, subscribed to {cainvest_pairs}")

            last_ping = time.time()
            async for raw in ws:
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == CONSTANTS.WS_REFERENCE_UPDATE:
                    rates = msg.get("rates", {})
                    for cainvest_pair, rate_data in rates.items():
                        # Resolve dynamically so alias pairs (e.g. USDC-BRL1 → USDC/BRL) are matched
                        hb_pair = next(
                            (tp for tp in self._trading_pairs
                             if CONSTANTS.CAINVEST_PAIR_MAP.get(tp) == cainvest_pair),
                            None
                        )
                        if hb_pair:
                            buy_price = Decimal(str(rate_data["buy"]))
                            sell_price = Decimal(str(rate_data["sell"]))
                            self._last_prices[hb_pair] = {
                                "buy": buy_price,
                                "sell": sell_price,
                            }
                            book = self._build_synthetic_book(buy_price, sell_price)
                            snapshot_msg = OrderBookMessage(
                                OrderBookMessageType.SNAPSHOT,
                                {
                                    "trading_pair": hb_pair,
                                    "update_id": self._next_update_id(),
                                    "bids": book["bids"],
                                    "asks": book["asks"],
                                },
                                timestamp=time.time(),
                            )
                            self._message_queue[self._snapshot_messages_queue_key].put_nowait(snapshot_msg)

                elif msg_type == CONSTANTS.WS_ERROR:
                    self.logger().warning(f"WS error: {msg.get('message')}")

                now = time.time()
                if now - last_ping >= CONSTANTS.WS_PING_INTERVAL:
                    await ws.send(json.dumps({"type": CONSTANTS.WS_PING}))
                    last_ping = now
        self._active_ws = None
        self._ws_assistant = None

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                msg = await self._message_queue[self._snapshot_messages_queue_key].get()
                output.put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in snapshot listener: {e}")
                await asyncio.sleep(1)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # OTC exchange has no public trade feed
        while True:
            await asyncio.sleep(3600)

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for trading_pair in trading_pairs:
            pair_prices = self._last_prices.get(trading_pair)
            if pair_prices and pair_prices.get("buy") and pair_prices.get("sell"):
                mid = (pair_prices["buy"] + pair_prices["sell"]) / 2
                prices[trading_pair] = float(mid)
            else:
                prices[trading_pair] = 0.0
        return prices

    async def wait_for_price(self, trading_pair: str, timeout: float = 12.0) -> bool:
        """
        Wait for the first WS tick for a trading pair.
        Cainvest sends an initial reference_update after subscription, even with no change.
        """
        start = time.time()
        while time.time() - start < timeout:
            pair_prices = self._last_prices.get(trading_pair)
            if pair_prices and pair_prices.get("buy") and pair_prices.get("sell"):
                return True
            await asyncio.sleep(0.2)
        return False

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if trading_pair not in self._trading_pairs:
            self._trading_pairs.append(trading_pair)

        if self._active_ws is not None:
            try:
                cainvest_pair = CONSTANTS.CAINVEST_PAIR_MAP.get(trading_pair, trading_pair.replace("-", "/"))
                await self._active_ws.send(json.dumps({
                    "type": CONSTANTS.WS_SUBSCRIBE_REFERENCE,
                    "currency_pairs": [cainvest_pair],
                }))
            except Exception as e:
                self.logger().warning(f"Failed to subscribe {trading_pair} on live WS: {e}")
                return False
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if trading_pair in self._trading_pairs:
            self._trading_pairs.remove(trading_pair)
        self._last_prices.pop(trading_pair, None)

        if self._active_ws is not None:
            try:
                cainvest_pair = CONSTANTS.CAINVEST_PAIR_MAP.get(trading_pair, trading_pair.replace("-", "/"))
                await self._active_ws.send(json.dumps({
                    "type": CONSTANTS.WS_UNSUBSCRIBE_REFERENCE,
                    "currency_pairs": [cainvest_pair],
                }))
            except Exception as e:
                self.logger().warning(f"Failed to unsubscribe {trading_pair} on live WS: {e}")
                return False
        return True

    def _next_update_id(self) -> int:
        self._update_id += 1
        return self._update_id
