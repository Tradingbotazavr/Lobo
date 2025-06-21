import asyncio
import os
import sys

from loguru import logger

# Ensure the project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lob_analyzer.collector.trades_stream import TradesStream


async def test_trades_stream_integration():
    """
    An integration test for TradesStream. It connects to the live
    Binance WebSocket, runs for a short duration, and checks if trades
    are received and aggregated.
    """
    symbol = "BTCUSDT"
    run_duration = 10  # seconds
    
    logger.info(f"Starting integration test for TradesStream with symbol {symbol}...")

    # Arrange
    raw_trades_received = 0
    aggregations_received = 0

    def raw_trade_callback(event):
        nonlocal raw_trades_received
        raw_trades_received += 1
        if raw_trades_received % 20 == 0: # Log every 20th trade
            logger.debug(f"Raw trade #{raw_trades_received} received: {event}")

    def aggregate_callback(data):
        nonlocal aggregations_received
        aggregations_received += 1
        logger.info(f"Aggregation #{aggregations_received} received: {data}")

    stream = TradesStream(symbol=symbol, agg_interval=2.0)
    stream.on_raw_trade(raw_trade_callback)
    stream.on_trade_aggregate(aggregate_callback)

    # Act
    runner_task = asyncio.create_task(stream.run())

    logger.info(f"Running for {run_duration} seconds...")
    await asyncio.sleep(run_duration)

    logger.info("Stopping the trades stream...")
    await stream.stop()

    # Wait for the runner task to ensure it finishes cleanly
    try:
        await asyncio.wait_for(runner_task, timeout=5.0)
    except asyncio.TimeoutError:
        logger.error("Runner task did not complete after stop().")
        runner_task.cancel()

    # Assert
    logger.info("Verifying results...")
    assert raw_trades_received > 0, "Raw trade callback was never called."
    assert aggregations_received > 0, "Aggregate callback was never called."

    print(f"\n✅ TradesStream integration test passed!")
    print(f"  - Received {raw_trades_received} raw trades.")
    print(f"  - Received {aggregations_received} aggregations.")


if __name__ == "__main__":
    # Configure logger for standalone run
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    try:
        asyncio.run(test_trades_stream_integration())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user.") 