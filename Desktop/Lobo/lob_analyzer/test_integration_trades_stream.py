import asyncio
import sys
import os
from loguru import logger

# Add project root to path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from collector.trades_stream import TradesStream, TradeEvent

# --- Test Configuration ---
SYMBOL = "BTCUSDT"
TEST_DURATION_SECONDS = 10  # Run for 10 seconds
AGGREGATION_INTERVAL_SECONDS = 1.0
# -------------------------

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss.SSS} | {level: <8} | {message}")

def setup_callbacks(stream: TradesStream):
    """Attaches logging callbacks to the stream's events."""

    def on_raw_trade(event: TradeEvent):
        logger.info(f"RAW  | Side: {event.side.upper():<4} | Price: {event.price:<8.2f} | Qty: {event.qty}")

    def on_trade_aggregate(data: dict):
        anomaly_mark = "🔥" if 'anomaly' in data else " "
        logger.warning(
            f"AGG {anomaly_mark}| Buy Vol: {data['buy_volume']:.2f} | Sell Vol: {data['sell_volume']:.2f} | "
            f"Imbalance: {data['imbalance']:.2f} | Count: {data['trade_count']}"
        )

    stream.on_raw_trade(on_raw_trade)
    stream.on_trade_aggregate(on_trade_aggregate)
    logger.info("Callbacks for raw trades and aggregates are set up.")


async def main():
    """
    Runs an integration test for the TradesStream component.
    It connects to the real Binance WebSocket, runs for a specified duration,
    and then gracefully shuts down.
    """
    logger.info(f"Starting TradesStream integration test for {SYMBOL}...")
    logger.info(f"Test will run for {TEST_DURATION_SECONDS} seconds.")

    stream = TradesStream(
        symbol=SYMBOL,
        agg_interval=AGGREGATION_INTERVAL_SECONDS
    )
    setup_callbacks(stream)

    stream_task = asyncio.create_task(stream.run())

    # Wait for the test duration
    try:
        await asyncio.sleep(TEST_DURATION_SECONDS)
    except asyncio.CancelledError:
        logger.info("Test runner interrupted.")

    # Gracefully stop the stream
    logger.info("Test duration elapsed. Stopping stream...")
    await stream.stop()

    # Wait for the stream task to fully complete its shutdown
    await stream_task
    logger.info("TradesStream integration test finished.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nTest stopped by user (Ctrl+C).") 