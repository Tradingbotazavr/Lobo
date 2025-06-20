import asyncio
import json
import websockets
import time
from loguru import logger
from collector.orderbook_realtime import RealTimeOrderBook
from handlers.signal_detector import SignalDetector
from data.stream_merger import StreamMerger


def setup_logging():
    logger.add("logs/test_run.log", 
               rotation="100 MB", 
               format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}")


def signal_callback(event):
    """Callback function for handling signal events"""
    logger.info(f"Signal detected: {event}")
    print(f"\nSignal: {event}")


def on_orderbook_update(orderbook):
    # Извлекаем фичи для merger
    now = time.time()
    imbalance, bid_vol, ask_vol = orderbook.get_imbalance()
    mid_price = orderbook.get_mid_price()
    event = {
        "timestamp": now,
        "price": mid_price,
        "volume": bid_vol + ask_vol,
        "side": "buy" if imbalance > 0 else "sell",
        "bid_ask_ratio": (bid_vol / ask_vol) if ask_vol > 0 else 0,
        "limit_disbalance": imbalance
    }
    if hasattr(on_orderbook_update, "merger"):
        on_orderbook_update.merger.add_orderbook(event)


async def main():
    # Setup logging
    setup_logging()
    
    # Initialize components
    orderbook = RealTimeOrderBook("btcusdt")
    detector = SignalDetector(imbalance_threshold=0.7)
    # Инициализация merger
    merger_path = f"data/merged/realdata_{int(time.time())}.parquet"
    merger = StreamMerger(save_path=merger_path, future_offset=5, threshold_pct=0.001)
    on_orderbook_update.merger = merger
    
    # Setup signal handling
    detector.on_signal(signal_callback)
    orderbook.on_update(detector.process_orderbook)
    orderbook.on_update(on_orderbook_update)
    
    # Запуск merger
    asyncio.create_task(merger.run())
    
    logger.info("Starting orderbook monitoring...")
    print("Monitoring orderbook (Press Ctrl+C to stop)...")
    
    try:
        # Start WebSocket connection
        async with websockets.connect(orderbook.ws_url) as ws:
            logger.info("WebSocket connected")
            
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(msg)
                    orderbook._update(data)
                    detector.process_orderbook(orderbook)
                    on_orderbook_update(orderbook)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.exception(f"Error processing message: {e}")
                    
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        logger.info("Application stopped")


if __name__ == "__main__":
    try:
        # Use default event loop policy without signal handlers
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
        logger.info("Application stopped by user") 