import asyncio
import signal
import sys
import time
from functools import partial
from typing import Optional
from loguru import logger
from lob_analyzer.analyzer.trade_activity_analyzer import TradeActivityAnalyzer
from lob_analyzer.collector.orderbook_realtime import RealTimeOrderBook
from lob_analyzer.collector.trades_stream import TradesStream, TradeEvent
from lob_analyzer.data.stream_merger import StreamMerger
from lob_analyzer.handlers.signal_detector import SignalDetector
from lob_analyzer.ml.model_runner import ModelRunner


def setup_logging():
    """Configures the logger for the application."""
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    logger.add(
        "logs/app_run.log",
        rotation="100 MB",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )
    logger.info("Logging setup complete.")


class CombinedStreamRunner:
    """
    Manages the lifecycle of data collection, processing, and merging streams.
    """

    def __init__(self, symbol: str, model_path: Optional[str] = None):
        self.symbol = symbol
        self.orderbook = RealTimeOrderBook(symbol=symbol)
        self.trades = TradesStream(symbol=symbol)
        self.merger = StreamMerger(symbol=symbol)
        self.activity_analyzer = TradeActivityAnalyzer(enable_logging=False)
        self.signal_detector = SignalDetector()
        self.tasks: list[asyncio.Task] = []
        self._running = False
        self.model_runner = None
        
        self._last_activity_spike_ts: Optional[float] = None
        self._activity_spike_ttl = 1.0  # seconds
        self._latest_trade_agg: dict = {}

        if model_path:
            try:
                self.model_runner = ModelRunner(model_path)
                self.merger.model_runner = self.model_runner # Assign model to merger
            except Exception:
                logger.error("Failed to initialize ModelRunner. Continuing without inference.")

    def _setup_callbacks(self):
        """Connects the data streams to the merger and sets up inference."""
        self.orderbook.on_update(self._handle_orderbook_update)
        self.orderbook.on_update(self.signal_detector.process_orderbook)
        self.signal_detector.on_signal(self._handle_signal)

        # Raw trades go directly to the merger
        self.trades.on_raw_trade(self.merger.add_trade)
        
        # Aggregated trade data is now used for two purposes:
        # 1. Caching for feature enrichment
        # 2. Feeding the activity analyzer
        self.trades.on_trade_aggregate(self._handle_trade_aggregate)
        self.trades.on_trade_aggregate(self.activity_analyzer.process_event)
        
        # Detected activity spikes are now handled to set a flag
        self.activity_analyzer.on_activity(self._handle_activity_spike)

        logger.info("Stream callbacks configured to feed the merger and analyzers.")

        if self.model_runner:
            self.merger.on_merge(self._handle_inference)
            logger.info("Callbacks for inference are set up.")

    def _handle_trade_aggregate(self, agg_data: dict):
        """Caches the latest aggregated trade data to enrich orderbook events."""
        self._latest_trade_agg = agg_data

    def _handle_orderbook_update(self, ob: RealTimeOrderBook):
        """Callback to process order book updates and feed the merger."""
        mid_price = ob.get_mid_price()
        if not mid_price:
            return

        imbalance, bid_vol, ask_vol = ob.get_imbalance()
        near_price_volume = ob.get_near_price_volume()
        
        # Check if a recent activity spike occurred
        now = time.time()
        activity_spike_feature = 0
        if self._last_activity_spike_ts and (now - self._last_activity_spike_ts < self._activity_spike_ttl):
            activity_spike_feature = 1
        else:
            self._last_activity_spike_ts = None # Expire the flag

        event = {
            "ts": ob.last_update_ts,
            "mid_price": mid_price,
            "imbalance": imbalance,
            "bid_volume": bid_vol,
            "ask_volume": ask_vol,
            "activity_spike": activity_spike_feature,
            **near_price_volume,
            **self._latest_trade_agg,
        }
        self.merger.add_orderbook(event)

    def _handle_signal(self, event: dict):
        """Callback to handle signals from the SignalDetector."""
        logger.critical(f"SIGNAL DETECTED: {event}")

    def _handle_activity_spike(self, spike_data: dict):
        """Callback to process activity spikes and feed them to the merger."""
        logger.warning(f"ACTIVITY SPIKE DETECTED: {spike_data}")
        # Set a timestamp flag instead of feeding the event to the merger
        self._last_activity_spike_ts = spike_data.get("ts")

    def _handle_inference(self, merged_record: dict):
        """Callback to run model prediction on a merged data record."""
        if not self.model_runner:
            return

        prediction = self.model_runner.predict(merged_record)
        if "error" not in prediction:
            logger.info(
                f"PREDICTION | Direction: {prediction['direction']}, "
                f"Confidence: {prediction['confidence']:.3f} | "
                f"Input TS: {merged_record['ts']:.3f}"
            )
        else:
            logger.warning(f"PREDICTION FAILED | {prediction['error']}")

    async def run(self):
        """Starts all data stream components and manages their execution."""
        if self._running:
            logger.warning("Runner is already running.")
            return

        self._running = True
        self._setup_callbacks()

        self.tasks = [
            asyncio.create_task(self.orderbook.run()),
            asyncio.create_task(self.trades.run()),
            asyncio.create_task(self.merger.run()),
        ]

        logger.info(f"Runner started for symbol {self.symbol.upper()}. Press Ctrl+C to stop.")
        
        # Keep the main task alive until a stop is requested
        while self._running:
            await asyncio.sleep(1)

    async def stop(self):
        """Gracefully stops all running components and tasks."""
        if not self._running:
            return

        logger.info("Stopping runner...")
        self._running = False

        # Stop data sources first to prevent them from adding new data
        await asyncio.gather(
            self.orderbook.stop(),
            self.trades.stop(),
            return_exceptions=True
        )
        logger.info("Data sources stopped.")

        # Now stop the merger, allowing it to process any remaining buffered data
        await self.merger.stop()
        logger.info("Merger stopped.")

        # Cancel the main tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("All tasks have been cancelled. Runner stopped.")


def handle_stop_signal(runner: CombinedStreamRunner):
    """Signal handler to initiate a graceful shutdown."""
    logger.warning("Stop signal received. Initiating graceful shutdown...")
    # It's important to not await here, as this is a signal handler
    asyncio.create_task(runner.stop()) 