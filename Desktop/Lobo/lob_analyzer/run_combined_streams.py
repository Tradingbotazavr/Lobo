import asyncio
import signal
import sys
import os
from functools import partial
import argparse
from loguru import logger

# Adjust the path to include the root of the project
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lob_analyzer.runner import CombinedStreamRunner, setup_logging, handle_stop_signal


async def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="Run the Lob Analyzer data collection and inference streams.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-s", "--symbol",
        type=str,
        default="BTCUSDT",
        help="The trading symbol to monitor."
    )
    parser.add_argument(
        "-m", "--model-path",
        type=str,
        default=None,
        help="Optional path to a trained model file for live inference."
    )
    args = parser.parse_args()

    setup_logging()
    
    runner = CombinedStreamRunner(symbol=args.symbol, model_path=args.model_path)

    # Set up signal handlers for graceful shutdown on POSIX systems
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        stop_handler = partial(handle_stop_signal, runner=runner)
        loop.add_signal_handler(signal.SIGINT, stop_handler)
        loop.add_signal_handler(signal.SIGTERM, stop_handler)

    try:
        await runner.run()
    except asyncio.CancelledError:
        logger.info("Main task cancelled.")
    finally:
        if runner._running:
            await runner.stop()
        logger.info("Application has shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This is a fallback for Windows and other environments where
        # signal handlers might not be available or work as expected.
        # The runner's finally block will handle the graceful shutdown.
        logger.info("\nKeyboardInterrupt caught. Shutting down...") 