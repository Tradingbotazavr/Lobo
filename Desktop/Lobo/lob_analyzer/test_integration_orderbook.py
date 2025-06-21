import asyncio
import sys
import os
import time
from loguru import logger

# Add project root to path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from collector.orderbook_realtime import RealTimeOrderBook

# --- Test Configuration ---
SYMBOL = "BTCUSDT"
TEST_DURATION_SECONDS = 15
# -------------------------

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss.SSS} | {level: <8} | {message}")

def setup_callback(stream: RealTimeOrderBook):
    """Attaches a logging callback to the order book's update event."""
    last_log_time = 0

    def on_update(ob: RealTimeOrderBook):
        nonlocal last_log_time
        now = time.time()
        # Throttle logging to once every 0.5 seconds to avoid flooding the console
        if now - last_log_time < 0.5:
            return

        mid_price = ob.get_mid_price()
        if not mid_price:
            return
            
        imbalance, bid_vol, ask_vol = ob.get_imbalance()
        logger.info(
            f"UPDATE | Mid Price: {mid_price:<8.2f} | Imbalance: {imbalance: .4f} | "
            f"Bid Vol: {bid_vol:<8.2f} | Ask Vol: {ask_vol:<8.2f}"
        )
        last_log_time = now

    stream.on_update(on_update)
    logger.info("Callback for order book updates is set up.")


async def main():
    """
    Runs an integration test for the RealTimeOrderBook component.
    """
    logger.info(f"Starting RealTimeOrderBook integration test for {SYMBOL}...")
    logger.info(f"Test will run for {TEST_DURATION_SECONDS} seconds.")

    stream = RealTimeOrderBook(symbol=SYMBOL)
    setup_callback(stream)

    stream_task = asyncio.create_task(stream.run())

    try:
        await asyncio.sleep(TEST_DURATION_SECONDS)
    except asyncio.CancelledError:
        logger.info("Test runner interrupted.")

    logger.info("Test duration elapsed. Stopping stream...")
    await stream.stop()
    await stream_task
    logger.info("RealTimeOrderBook integration test finished.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nTest stopped by user (Ctrl+C).") 