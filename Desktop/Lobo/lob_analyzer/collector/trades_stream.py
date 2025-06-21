import asyncio
import json
import time
from collections import deque
from loguru import logger
import numpy as np


class TradeEvent:
    """Represents a single trade event from the stream."""
    def __init__(self, ts: float, price: float, qty: float, side: str, symbol: str):
        self.ts = ts
        self.price = price
        self.qty = qty
        self.side = side
        self.symbol = symbol

    def __repr__(self):
        return f"TradeEvent(ts={self.ts}, symbol='{self.symbol}', price={self.price}, qty={self.qty}, side='{self.side}')"


class TradesStream:
    """
    Connects to a Binance Futures trade stream, processes trades, and performs
    time-based aggregation to detect volume anomalies.
    """
    def __init__(self, symbol: str, agg_interval: float = 1.0, window_size: int = 10):
        self.symbol = symbol.lower()
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@aggTrade"
        self.agg_interval = agg_interval
        
        self.agg_callbacks = []
        self.raw_trade_callbacks = []
        self._trade_buffer = deque()
        self._buffer_lock = asyncio.Lock()
        
        self.agg_history = deque(maxlen=window_size)
        
        # State for speed calculation
        self.prev_buy_vol = 0.0
        self.prev_sell_vol = 0.0
        
        self._agg_task = None
        self._ws_task = None
        self._running = False
        self._stop_event = asyncio.Event()
        self.logger = logger.bind(symbol=self.symbol)

    def on_trade_aggregate(self, callback):
        """
        Registers a callback to be invoked after each aggregation window.
        The callback will receive a dictionary with the aggregated data.
        """
        self.agg_callbacks.append(callback)

    def on_raw_trade(self, callback):
        """Registers a callback to be invoked for each raw trade event."""
        self.raw_trade_callbacks.append(callback)

    async def _process_message(self, msg: str):
        """Parses a trade message and adds it to the buffer."""
        try:
            data = json.loads(msg)
            if data.get('e') != 'aggTrade':
                return

            event = TradeEvent(
                ts=data.get("T", time.time() * 1000) / 1000,
                price=float(data["p"]),
                qty=float(data["q"]),
                side="sell" if data["m"] else "buy",
                symbol=self.symbol.upper()
            )

            async with self._buffer_lock:
                self._trade_buffer.append(event)
                if len(self._trade_buffer) > 20_000:
                    self._trade_buffer.popleft()

            self.logger.debug(f"TRADE | {event}")
            
            # Invoke raw trade callbacks
            for cb in self.raw_trade_callbacks:
                try:
                    cb(event)
                except Exception as e:
                    self.logger.error(f"Error in raw trade callback: {e}")

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.warning(f"Failed to process trade message: {e} | Raw msg: {msg}")

    async def _aggregator_loop(self):
        self.logger.info("Aggregator loop started.")
        try:
            while not self._stop_event.is_set():
                await self._perform_aggregation()
                # Wait for the next aggregation interval
                await asyncio.sleep(self.agg_interval)
        except asyncio.CancelledError:
            self.logger.info("Aggregator loop cancelled.")
        finally:
            self.logger.info("Aggregator loop stopped.")

    async def _perform_aggregation(self):
        """
        Aggregates trades from the buffer over the last interval, calculates
        volume imbalance and detects anomalies based on a rolling window.
        """
        now = time.time()
        cutoff = now - self.agg_interval

        buy_volume = 0.0
        sell_volume = 0.0
        count = 0
        
        # This critical section should be as short as possible.
        async with self._buffer_lock:
            while self._trade_buffer and self._trade_buffer[0].ts < cutoff:
                self._trade_buffer.popleft()
            
            # Create a temporary list for safe iteration
            trades_to_process = list(self._trade_buffer)

        if not trades_to_process:
            return

        buy_volume = sum(t.qty for t in trades_to_process if t.side == "buy")
        sell_volume = sum(t.qty for t in trades_to_process if t.side == "sell")
        count = len(trades_to_process)

        total_volume = buy_volume + sell_volume
        if total_volume == 0:
            # Update previous volumes and return if no activity
            self.prev_buy_vol = 0.0
            self.prev_sell_vol = 0.0
            return
            
        # Calculate volume-weighted average price
        total_qty = sum(t.qty for t in trades_to_process)
        avg_price = sum(t.price * t.qty for t in trades_to_process) / total_qty if total_qty > 0 else 0

        # Calculate volume speed
        buy_vol_speed = buy_volume - self.prev_buy_vol
        sell_vol_speed = sell_volume - self.prev_sell_vol
        self.prev_buy_vol = buy_volume
        self.prev_sell_vol = sell_volume

        imbalance = (buy_volume - sell_volume) / total_volume
        self.agg_history.append({'buy_vol': buy_volume, 'sell_vol': sell_volume})
        
        buy_vols = np.array([item['buy_vol'] for item in self.agg_history])
        sell_vols = np.array([item['sell_vol'] for item in self.agg_history])
        
        buy_mean = buy_vols.mean()
        buy_std = buy_vols.std() if len(buy_vols) > 1 else 0.0
        sell_mean = sell_vols.mean()
        sell_std = sell_vols.std() if len(sell_vols) > 1 else 0.0

        agg_data = {
            'ts': now, 'symbol': self.symbol.upper(), 'buy_volume': buy_volume,
            'sell_volume': sell_volume, 'imbalance': imbalance, 'trade_count': count,
            'avg_price': avg_price, 'buy_vol_speed': buy_vol_speed, 'sell_vol_speed': sell_vol_speed,
            'buy_mean': buy_mean, 'buy_std': buy_std, 'sell_mean': sell_mean, 'sell_std': sell_std
        }
        
        log_msg = (
            f"AGG | buy_vol={buy_volume:.3f} | sell_vol={sell_volume:.3f} | "
            f"imb={imbalance:.3f} | cnt={count} | "
            f"buy_mean={buy_mean:.3f} (std:{buy_std:.3f}) | "
            f"sell_mean={sell_mean:.3f} (std:{sell_std:.3f})"
        )
        self.logger.info(log_msg)

        buy_anomaly = buy_std > 0 and buy_volume > buy_mean + 2 * buy_std
        sell_anomaly = sell_std > 0 and sell_volume > sell_mean + 2 * sell_std
        if buy_anomaly or sell_anomaly:
            anomaly_type = 'buy' if buy_anomaly else 'sell'
            self.logger.warning(
                f"ANOMALY | type={anomaly_type} | buy_vol={buy_volume:.3f} (mean:{buy_mean:.3f}) | "
                f"sell_vol={sell_volume:.3f} (mean:{sell_mean:.3f})"
            )
            agg_data['anomaly'] = anomaly_type
            
        for cb in self.agg_callbacks:
            try:
                cb(agg_data)
            except Exception as e:
                self.logger.error(f"Error in trade aggregate callback: {e}")

    async def _run_ws_loop(self):
        import websockets
        self.logger.info(f"Starting WebSocket loop for {self.ws_url}...")
        reconnect_delay = 1
        while self._running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.logger.info("WebSocket connected for trades stream.")
                    reconnect_delay = 1  # Reset delay on successful connection
                    async for msg in ws:
                        if not self._running:
                            break
                        await self._process_message(msg)
            except asyncio.CancelledError:
                self.logger.info("Trade stream task cancelled.")
                break  # Exit loop if cancelled
            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                self.logger.warning(f"Trade stream connection closed: {e}. Reconnecting in {reconnect_delay}s...")
            except Exception as e:
                self.logger.exception(f"Error in trade stream: {e}. Reconnecting in {reconnect_delay}s...")
            
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff up to 60s
        
        self.logger.info("Trade stream loop stopped.")

    async def run(self):
        """Starts the trade stream and the aggregator and runs until stopped."""
        if self._running:
            self.logger.warning("Trade stream is already running.")
            return

        self.logger.info("Starting trade stream...")
        self._running = True
        
        # These tasks will run until self._running is set to False by the stop() method
        self._agg_task = asyncio.create_task(self._aggregator_loop())
        self._ws_task = asyncio.create_task(self._run_ws_loop())
        
        # This will wait for both tasks to complete. They complete when stop() is called,
        # which cancels them and causes them to exit their loops.
        try:
            await asyncio.gather(self._ws_task, self._agg_task)
        except asyncio.CancelledError:
            self.logger.info("Run method cancelled.")
        finally:
            self.logger.info("Trade stream run method finished.")

    async def stop(self):
        """Stops the trade stream and the aggregator gracefully."""
        if self._stop_event.is_set():
            self.logger.warning("Trade stream is not running or already stopping.")
            return
            
        self.logger.info("Stopping trade stream...")
        self._stop_event.set()
        self._running = False # Also set running flag for ws loop
        
        tasks = []
        if self._ws_task and not self._ws_task.done():
             tasks.append(self._ws_task)
        if self._agg_task and not self._agg_task.done():
            tasks.append(self._agg_task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.logger.info("Trade stream stopped.")

__all__ = ["TradesStream", "TradeEvent"]
