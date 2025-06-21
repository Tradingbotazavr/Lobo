import asyncio
import os
import sys
import time

from loguru import logger

# Ensure the project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lob_analyzer.collector.orderbook_realtime import RealTimeOrderBook


async def test_orderbook_integration():
    """
    An integration test for RealTimeOrderBook. It connects to the live
    Binance WebSocket, runs for a short duration, and checks if feature
    data is being written.
    """
    symbol = "BTCUSDT"
    run_duration = 10  # seconds
    
    logger.info(f"Starting integration test for RealTimeOrderBook with symbol {symbol}...")
    
    # Arrange
    updates_received = 0
    def simple_callback(orderbook: RealTimeOrderBook):
        nonlocal updates_received
        updates_received += 1
        imbalance, _, _ = orderbook.get_imbalance()
        mid_price = orderbook.get_mid_price()
        if updates_received % 10 == 0: # Log every 10th update
             logger.debug(f"Update {updates_received}: Imbalance={imbalance:.4f}, MidPrice={mid_price}")

    ob = RealTimeOrderBook(symbol=symbol)
    ob.on_update(simple_callback)
    
    output_file = ob.features_file
    if os.path.exists(output_file):
        os.remove(output_file)

    # Act
    runner_task = asyncio.create_task(ob.run())

    logger.info(f"Running for {run_duration} seconds...")
    await asyncio.sleep(run_duration)

    logger.info("Stopping the order book stream...")
    await ob.stop()

    # Wait for the runner task to ensure it finishes cleanly
    try:
        await asyncio.wait_for(runner_task, timeout=5.0)
    except asyncio.TimeoutError:
        logger.error("Runner task did not complete after stop().")
        runner_task.cancel()

    # Assert
    logger.info("Verifying results...")
    assert updates_received > 0, "Callback was never called; no updates were processed."
    assert os.path.exists(output_file), f"Output file '{output_file}' was not created."
    
    file_size = os.path.getsize(output_file)
    assert file_size > 0, f"Output file '{output_file}' is empty."

    print(f"\n✅ RealTimeOrderBook integration test passed!")
    print(f"  - Received {updates_received} updates.")
    print(f"  - Output file '{output_file}' created with size {file_size} bytes.")

    # Cleanup
    if os.path.exists(output_file):
        os.remove(output_file)
        logger.info(f"Cleaned up test file: {output_file}")


if __name__ == "__main__":
    # Configure logger for standalone run
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    try:
        asyncio.run(test_orderbook_integration())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user.") 