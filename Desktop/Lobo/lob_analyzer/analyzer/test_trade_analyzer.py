import time

from trade_activity_analyzer import TradeActivityAnalyzer

# Локальный TradeEvent для теста
class TradeEvent:
    def __init__(self, ts, price, qty, side):
        self.ts = ts
        self.price = price
        self.qty = qty
        self.side = side

analyzer = TradeActivityAnalyzer(window_sec=1.0)

def callback(data):
    print("Callback triggered:", data)

analyzer.on_activity(callback)

now = time.time()

# Набросаем 10 сделок в 1 секунду
events = [
    TradeEvent(ts=now + i * 0.1, price=10000 + i, qty=10, side="buy" if i % 2 == 0 else "sell")
    for i in range(10)
]

for event in events:
    analyzer.process_trade(event)
    time.sleep(0.05) 