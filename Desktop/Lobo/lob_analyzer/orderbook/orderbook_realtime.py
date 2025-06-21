import asyncio
import csv
import os
import time
from loguru import logger
from sortedcontainers import SortedDict


class RealTimeOrderBook:
    """
    Manages a real-time order book for a given symbol from Binance Futures,
    calculates features like imbalance, and saves them asynchronously to a CSV file.
    """
    def __init__(self, symbol: str, depth_pct=0.0025):
        self.symbol = symbol.lower()
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth@100ms"
        self.bids = SortedDict(lambda x: -x)
        self.asks = SortedDict()
        self.callbacks = []
        self.depth_pct = depth_pct
        self.logger = logger.bind(symbol=self.symbol)

        self.last_logged_imbalance = None
        self.last_logged_features_ts = 0.0

        self.features_file = f"data/{self.symbol.upper()}_features.csv"
        self._setup_features_csv()

        self._features_queue = asyncio.Queue()
        self._writer_task = None

    def _setup_features_csv(self):
        """Creates the directory and the CSV header if the file doesn't exist."""
        os.makedirs(os.path.dirname(self.features_file), exist_ok=True)
        if not os.path.exists(self.features_file):
            with open(self.features_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "ts", "symbol", "mid_price", "imbalance", "best_bid",
                    "best_ask", "bid_volume", "ask_volume"
                ])

    def on_update(self, callback):
        """Registers a callback to be invoked on each order book update."""
        self.callbacks.append(callback)

    def get_best_bid(self) -> float | None:
        """Returns the best bid price, or None if not available."""
        try:
            return next(iter(self.bids.keys()))
        except StopIteration:
            return None

    def get_best_ask(self) -> float | None:
        """Returns the best ask price, or None if not available."""
        try:
            return next(iter(self.asks.keys()))
        except StopIteration:
            return None

    def get_mid_price(self) -> float | None:
        """Calculates the mid-price from the best bid and ask."""
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return None

    def get_imbalance(self, depth_pct=None) -> tuple[float, float, float]:
        """
        Calculates the order book imbalance within a certain percentage depth.
        Returns: (imbalance, total bid volume, total ask volume)
        """
        depth_pct = depth_pct or self.depth_pct
        mid = self.get_mid_price()
        if mid is None:
            return 0.0, 0.0, 0.0

        bid_limit = mid * (1 - depth_pct)
        ask_limit = mid * (1 + depth_pct)

        bid_vol = sum(v for p, v in self.bids.items() if p >= bid_limit)
        ask_vol = sum(v for p, v in self.asks.items() if p <= ask_limit)

        total = bid_vol + ask_vol
        if total == 0:
            return 0.0, bid_vol, ask_vol

        imbalance = (bid_vol - ask_vol) / total
        return imbalance, bid_vol, ask_vol

    def _write_batch(self, batch: list[dict]):
        """Writes a batch of features to the CSV file."""
        if not batch:
            return
        try:
            # File I/O is sync, but it's fast enough for this purpose.
            with open(self.features_file, 'a', newline='') as f:
                # Use the keys from the first item as fieldnames
                writer = csv.DictWriter(f, fieldnames=batch[0].keys())
                writer.writerows(batch)
            self.logger.debug(f"Wrote batch of {len(batch)} features.")
        except Exception as e:
            self.logger.error(f"Error writing features batch: {e}")

    async def _features_writer_loop(self):
        """
        A background task that collects features from a queue and writes them
        to a file in batches. It stops when a `None` sentinel is received.
        """
        self.logger.info("Features writer task started.")
        is_running = True
        while is_running:
            batch = []
            item = None
            try:
                # Wait for the first item, which might be the stop sentinel
                item = await asyncio.wait_for(self._features_queue.get(), timeout=1.0)
                if item is None:
                    is_running = False
                else:
                    batch.append(item)

                # Collect more items for the batch without waiting long
                while len(batch) < 50 and is_running:
                    try:
                        item = self._features_queue.get_nowait()
                        if item is None:
                            is_running = False
                        else:
                            batch.append(item)
                    except asyncio.QueueEmpty:
                        break # No more items, proceed to write
            except asyncio.TimeoutError:
                pass # Timed out waiting for the first item, batch will be empty

            self._write_batch(batch)

        # Drain the queue one last time after receiving the stop signal
        final_batch = []
        while not self._features_queue.empty():
            try:
                item = self._features_queue.get_nowait()
                if item: # ensure not to add None
                    final_batch.append(item)
            except asyncio.QueueEmpty:
                break
        self._write_batch(final_batch)
        self.logger.info("Features writer task stopped.")

    def _log_and_save_features(self):
        """
        Calculates, logs, and queues features for saving based on filtering logic
        to avoid excessive data points.
        """
        now = time.time()
        imbalance, bid_vol, ask_vol = self.get_imbalance()

        # Filtering logic to reduce noise
        if self.last_logged_imbalance is not None:
            imbalance_changed_sign = (imbalance > 0 and self.last_logged_imbalance < 0) or \
                                     (imbalance < 0 and self.last_logged_imbalance > 0)
            significant_change = abs(imbalance - self.last_logged_imbalance) > 0.05

            if not significant_change and not imbalance_changed_sign and (now - self.last_logged_features_ts) < 0.5:
                return

        mid_price = self.get_mid_price()
        if mid_price is None:
            return # Cannot proceed without a mid-price

        features = {
            "ts": now,
            "symbol": self.symbol.upper(),
            "mid_price": mid_price,
            "imbalance": imbalance,
            "best_bid": self.get_best_bid() or 0.0,
            "best_ask": self.get_best_ask() or 0.0,
            "bid_volume": bid_vol,
            "ask_volume": ask_vol,
        }

        log_msg = (
            f"FEATURES: ts={features['ts']:.3f}, mid_price={features['mid_price']:.2f}, "
            f"imbalance={features['imbalance']:.4f}, "
            f"best_bid={features['best_bid']:.2f}, best_ask={features['best_ask']:.2f}, "
            f"bid_vol={features['bid_volume']:.3f}, ask_vol={features['ask_volume']:.3f}"
        )
        self.logger.info(log_msg)

        # Add features to the async queue for writing
        self._features_queue.put_nowait(features)

        self.last_logged_imbalance = imbalance
        self.last_logged_features_ts = now

    def _update(self, data: dict):
        """Updates the order book with new data from the WebSocket stream."""
        for price, qty in data['b']:
            p, q = float(price), float(qty)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for price, qty in data['a']:
            p, q = float(price), float(qty)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self._log_and_save_features()

        for cb in self.callbacks:
            try:
                cb(self)
            except Exception as e:
                self.logger.error(f"Error in order book update callback: {e}")

    async def stop(self):
        """Gracefully stops the writer task."""
        self.logger.info("Stopping RealTimeOrderBook...")
        if self._writer_task:
            await self._features_queue.put(None) # Send stop signal
            await self._writer_task
            self._writer_task = None
        self.logger.info("RealTimeOrderBook stopped.")

    async def run(self):
        """
        Connects to the WebSocket and runs the main loop for receiving order book data.
        """
        import json
        import websockets

        if self._writer_task:
            self.logger.warning("Writer task is already running. Skipping creation.")
        else:
            self._writer_task = asyncio.create_task(self._features_writer_loop())

        try:
            async with websockets.connect(self.ws_url) as ws:
                self.logger.info(f"WebSocket connected for {self.symbol.upper()}")
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        if 'e' in data and data['e'] == 'depthUpdate':
                            self._update(data)
                    except json.JSONDecodeError:
                        self.logger.warning(f"Received non-JSON message: {msg}")
                    except Exception as e:
                        self.logger.exception(f"Error processing message: {e}")
        except websockets.exceptions.ConnectionClosed as e:
            self.logger.error(f"WebSocket connection closed for {self.symbol.upper()}: {e}")
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred in run loop for {self.symbol.upper()}: {e}")
        finally:
            await self.stop() 