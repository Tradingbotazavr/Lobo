import asyncio
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collector.trades_stream import TradesStream
from trade_activity_analyzer import TradeActivityAnalyzer
from data.stream_merger import StreamMerger

# Коллбек для вывода всплесков активности
def print_activity(data):
    print("⚠️ Activity Spike:", data)
    # Добавляем трейд в merger
    trade_event = {
        "timestamp": data["ts"],
        "price": data["avg_price"],
        "volume": data["buy_volume"] + data["sell_volume"],
        "side": data["direction"],
        "bid_ask_ratio": data["buy_sell_ratio"],
        "limit_disbalance": 0.0  # Можно доработать, если есть
    }
    if hasattr(print_activity, "merger"):
        print_activity.merger.add_trade(trade_event)

async def main():
    stream = TradesStream()
    analyzer = TradeActivityAnalyzer(window_sec=1.0, volume_threshold=10, imbalance_ratio_high=2.0, imbalance_ratio_low=0.5, spike_multiplier=1.2)
    # Инициализация merger
    merger_path = f"data/merged/realdata_{int(time.time())}.parquet"
    merger = StreamMerger(save_path=merger_path, future_offset=5, threshold_pct=0.001)
    print_activity.merger = merger
    analyzer.on_activity(print_activity)
    stream.on_trade(analyzer.process_trade)
    # Запуск merger
    asyncio.create_task(merger.run())
    try:
        await stream.stream()
    except KeyboardInterrupt:
        print("Остановлено пользователем")
        stream.stop()

if __name__ == "__main__":
    asyncio.run(main()) 