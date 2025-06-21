import time
import os
import json
from collections import deque
from typing import Callable, Deque, Dict, List
from loguru import logger
import numpy as np

from lob_analyzer.collector.trades_stream import TradeEvent


class TradeActivityAnalyzer:
    def __init__(
        self,
        volume_threshold: float = 10.0,
        imbalance_ratio_high: float = 2.0,
        imbalance_ratio_low: float = 0.5,
        spike_multiplier: float = 1.2,
        enable_logging: bool = True,
        save_path: str = "activity_spikes.jsonl",
    ):
        self.volume_threshold = volume_threshold
        self.imbalance_ratio_high = imbalance_ratio_high
        self.imbalance_ratio_low = imbalance_ratio_low
        self.spike_multiplier = spike_multiplier
        self.enable_logging = enable_logging
        self.save_path = save_path

        self.callbacks: List[Callable[[Dict], None]] = []

        if save_path and not os.path.exists(self.save_path):
            with open(self.save_path, "w") as f:
                pass

    def on_activity(self, callback: Callable[[dict], None]):
        self.callbacks.append(callback)

    def process_event(self, event: Dict):
        """Processes an aggregated trade event from TradesStream."""
        now = event.get('ts', time.time())
        buy_volume = event.get('buy_volume', 0)
        sell_volume = event.get('sell_volume', 0)
        total_volume = buy_volume + sell_volume
        
        if total_volume == 0:
            return

        # MA of volume is now calculated inside TradesStream and passed in the event
        buy_mean = event.get('buy_mean', 0)
        sell_mean = event.get('sell_mean', 0)
        ma_volume = buy_mean + sell_mean
        
        spike = (ma_volume > 0 and total_volume / ma_volume > self.spike_multiplier)
        
        buy_sell_ratio = buy_volume / sell_volume if sell_volume > 0 else 1_000_000.0 if buy_volume > 0 else 1.0
        imbalance = (
            buy_sell_ratio > self.imbalance_ratio_high or
            buy_sell_ratio < self.imbalance_ratio_low
        )
        volume_exceeded = total_volume > self.volume_threshold

        if spike or imbalance or volume_exceeded:
            direction = "buy" if buy_sell_ratio > 1 else "sell"
            
            spike_data = {
                "ts": now,
                "type": "trade_activity_spike",
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "buy_sell_ratio": buy_sell_ratio,
                "avg_price": event.get('avg_price', 0), 
                "trade_count": event.get('trade_count', 0),
                "ma_volume": ma_volume,
                "direction": direction,
                "reason": "spike" if spike else ("imbalance" if imbalance else "volume_exceeded")
            }
            if self.save_path:
                with open(self.save_path, "a") as f:
                    f.write(json.dumps(spike_data, ensure_ascii=False) + "\n")
            
            for cb in self.callbacks:
                cb(spike_data)

            if self.enable_logging:
                logger.warning(f"ACTIVITY SPIKE: {spike_data['reason']} | Direction: {direction} | Total Vol: {total_volume:.2f}") 