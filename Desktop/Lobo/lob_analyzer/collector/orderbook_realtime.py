import asyncio
import json
import time
from typing import List, Callable, Tuple, Optional

import websockets
from loguru import logger
from sortedcontainers import SortedDict


class RealTimeOrderBook:
    """
    Manages a real-time order book for a given symbol from Binance Futures,
    handling WebSocket connections, updates, and providing data access.
    """

    def __init__(self, symbol: str, depth_pct: float = 0.0025):
        self.symbol = symbol.lower()
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth@100ms"
        self.depth_pct = depth_pct

        self.bids: SortedDict[float, float] = SortedDict(lambda x: -x)
        self.asks: SortedDict[float, float] = SortedDict()

        self._callbacks: List[Callable[[RealTimeOrderBook], None]] = []
        self._running = False
        self._ws_task: Optional[asyncio.Task] = None
        self.last_update_ts: Optional[int] = None
        self.logger = logger.bind(symbol=self.symbol)

    def on_update(self, callback: Callable[['RealTimeOrderBook'], None]):
        """Registers a callback to be invoked on each order book update."""
        self._callbacks.append(callback)

    def get_best_bid(self) -> Optional[float]:
        """Returns the highest bid price."""
        return self.bids.peekitem(0)[0] if self.bids else None

    def get_best_ask(self) -> Optional[float]:
        """Returns the lowest ask price."""
        return self.asks.peekitem(0)[0] if self.asks else None

    def get_mid_price(self) -> Optional[float]:
        """Calculates the mid-price from the best bid and ask."""
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid and ask:
            return (bid + ask) / 2
        return None

    def get_imbalance(self, depth_pct: Optional[float] = None) -> Tuple[float, float, float]:
        """
        Calculates the order book imbalance within a given percentage of the mid-price.
        Returns: (imbalance_ratio, bid_volume, ask_volume)
        """
        depth_pct = depth_pct or self.depth_pct
        mid = self.get_mid_price()
        if not mid:
            return 0.0, 0.0, 0.0

        bid_limit = mid * (1 - depth_pct)
        ask_limit = mid * (1 + depth_pct)

        bid_vol = sum(v for p, v in self.bids.items() if p >= bid_limit)
        ask_vol = sum(v for p, v in self.asks.items() if p <= ask_limit)

        total_vol = bid_vol + ask_vol
        if total_vol == 0:
            return 0.0, bid_vol, ask_vol

        imbalance = (bid_vol - ask_vol) / total_vol
        return imbalance, bid_vol, ask_vol

    def get_near_price_volume(self, distance_pct: float = 0.0025) -> dict:
        """
        Calculates the total volume of bids and asks within a certain percentage
        distance from the mid-price.
        """
        mid_price = self.get_mid_price()
        if not mid_price:
            return {'bid_volume_near': 0, 'ask_volume_near': 0}

        price_threshold_low = mid_price * (1 - distance_pct)
        price_threshold_high = mid_price * (1 + distance_pct)

        bid_volume_near = sum(volume for price, volume in self.bids.items()
                              if price >= price_threshold_low)
        ask_volume_near = sum(volume for price, volume in self.asks.items()
                              if price <= price_threshold_high)
        
        return {
            'bid_volume_near': bid_volume_near,
            'ask_volume_near': ask_volume_near
        }

    def _update_from_message(self, data: dict):
        """Updates the order book with data from a WebSocket message."""
        self.last_update_ts = data.get('E')  # 'E' is the event time

        for price, qty in data.get('b', []):
            p, q = float(price), float(qty)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for price, qty in data.get('a', []):
            p, q = float(price), float(qty)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        # Notify all registered callbacks
        for cb in self._callbacks:
            try:
                cb(self)
            except Exception as e:
                self.logger.opt(exception=True).error(f"Error in order book update callback: {e}")

    async def _run_ws_loop(self):
        """The main loop for connecting and handling WebSocket messages."""
        self.logger.info("Starting order book WebSocket loop...")
        reconnect_delay = 1
        while self._running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.logger.info("Order book WebSocket connected.")
                    reconnect_delay = 1  # Reset delay on successful connection
                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            data = json.loads(msg)
                            self._update_from_message(data)
                        except json.JSONDecodeError:
                            self.logger.warning(f"Failed to decode JSON: {msg}")
                        except Exception:
                            self.logger.exception("Error processing order book message.")
            except asyncio.CancelledError:
                self.logger.info("Order book WebSocket task cancelled.")
                break
            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                self.logger.warning(f"Connection closed: {e}. Reconnecting in {reconnect_delay}s...")
            except Exception as e:
                self.logger.exception(f"An unexpected error occurred: {e}. Reconnecting in {reconnect_delay}s...")

            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

        self.logger.info("Order book WebSocket loop stopped.")

    async def run(self):
        """Starts the order book stream."""
        if self._running:
            self.logger.warning("Order book is already running.")
            return

        self.logger.info("Starting order book...")
        self._running = True
        self._ws_task = asyncio.create_task(self._run_ws_loop())
        try:
            await self._ws_task
        except asyncio.CancelledError:
            pass  # Task cancellation is expected on stop

    async def stop(self):
        """Stops the order book stream gracefully."""
        if not self._running or not self._ws_task:
            self.logger.warning("Order book is not running.")
            return

        self.logger.info("Stopping order book...")
        self._running = False
        self._ws_task.cancel()
        try:
            await self._ws_task
        except asyncio.CancelledError:
            pass  # Expected cancellation
        finally:
            self._ws_task = None
            self.logger.info("Order book stopped.")
