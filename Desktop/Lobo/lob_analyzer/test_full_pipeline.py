import asyncio
import sys
import os
from loguru import logger

# Add project root to path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lob_analyzer.runner import CombinedStreamRunner

# --- Test Configuration ---
SYMBOL = "BTCUSDT"
TEST_DURATION_SECONDS = 30
# -------------------------

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss.SSS} | {level: <8} | {message}")


async def main():
    """
    Runs a full end-to-end integration test of the system using CombinedStreamRunner.
    This test initializes all components, runs them for a specified duration,
    and ensures a graceful shutdown, resulting in a saved Parquet file.
    """
    logger.info("--- Starting Full Pipeline Integration Test ---")
    
    # CombinedStreamRunner handles the initialization and wiring of all components
    runner = CombinedStreamRunner(symbol=SYMBOL)

    runner_task = asyncio.create_task(runner.run())

    logger.info(f"Pipeline is running. Test will stop in {TEST_DURATION_SECONDS} seconds.")
    
    try:
        await asyncio.sleep(TEST_DURATION_SECONDS)
    except asyncio.CancelledError:
        logger.info("Test runner interrupted.")

    logger.info("Test duration elapsed. Initiating graceful shutdown...")
    await runner.stop()
    
    # Wait for the runner's main task to finish completely
    await runner_task
    
    logger.info("--- Full Pipeline Integration Test Finished ---")
    logger.info("Check the 'data/merged/' directory for the resulting Parquet file.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nTest stopped by user (Ctrl+C).") 