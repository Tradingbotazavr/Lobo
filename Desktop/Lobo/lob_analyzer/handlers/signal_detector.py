import time
from dataclasses import dataclass, field
from typing import Callable, List

from loguru import logger

# Assuming RealTimeOrderBook is in a discoverable path, e.g., via sys.path modification
# or by running from the project root.
from lob_analyzer.collector.orderbook_realtime import RealTimeOrderBook


@dataclass
class SignalEvent:
    """
    Represents a specific market signal detected by the system.
    """
    event_type: str
    direction: str  # 'buy' or 'sell'
    imbalance: float
    mid_price: float
    details: dict = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def __repr__(self):
        return (
            f"SignalEvent(type={self.event_type}, direction='{self.direction}', "
            f"imbalance={self.imbalance:.4f}, mid_price={self.mid_price:.2f}, ts={self.ts:.3f})"
        )


class SignalDetector:
    """
    Analyzes order book data to detect significant imbalance signals, with
    mechanisms to prevent signal flooding.
    """

    def __init__(
        self,
        imbalance_threshold: float = 0.6,
        reset_threshold: float = 0.3,
        cooldown_seconds: int = 10,
    ):
        """
        Initializes the detector with thresholds and state.

        :param imbalance_threshold: The minimum absolute imbalance to trigger a signal.
        :param reset_threshold: The absolute imbalance below which the detector re-arms.
        :param cooldown_seconds: Minimum time between consecutive signals.
        """
        if reset_threshold >= imbalance_threshold:
            raise ValueError("reset_threshold must be less than imbalance_threshold")

        self.imbalance_threshold = imbalance_threshold
        self.reset_threshold = reset_threshold
        self.cooldown_seconds = cooldown_seconds
        
        self._callbacks: List[Callable[[SignalEvent], None]] = []
        self._is_armed = True
        self._last_signal_ts = 0
        
        self.logger = logger.bind(component="SignalDetector")
        self.logger.info(
            f"Initialized with threshold={imbalance_threshold}, "
            f"reset_threshold={reset_threshold}, cooldown={cooldown_seconds}s"
        )

    def on_signal(self, callback: Callable[[SignalEvent], None]):
        """Registers a callback function to be invoked when a signal is detected."""
        self._callbacks.append(callback)

    def process_orderbook(self, orderbook: RealTimeOrderBook):
        """
        Processes an order book snapshot to check for signals.
        This method is designed to be called on every order book update.
        """
        now = time.time()
        imbalance, _, _ = orderbook.get_imbalance()
        mid_price = orderbook.get_mid_price()

        if mid_price is None:
            return

        # 1. Re-arm the detector if imbalance has returned to a neutral state
        if not self._is_armed and abs(imbalance) < self.reset_threshold:
            self._is_armed = True
            self.logger.debug(f"Detector re-armed. Imbalance {imbalance:.4f} is below reset threshold {self.reset_threshold}")

        # 2. Check for a new signal only if the detector is armed and not in cooldown
        if self._is_armed and (now - self._last_signal_ts) > self.cooldown_seconds:
            signal_direction = None
            if imbalance >= self.imbalance_threshold:
                signal_direction = "buy"
            elif imbalance <= -self.imbalance_threshold:
                signal_direction = "sell"

            if signal_direction:
                self._is_armed = False
                self._last_signal_ts = now
                
                event = SignalEvent(
                    event_type="imbalance_spike",
                    direction=signal_direction,
                    imbalance=imbalance,
                    mid_price=mid_price,
                )
                self.logger.warning(f"SIGNAL DETECTED: {event}")
                self._fire_callbacks(event)

    def _fire_callbacks(self, event: SignalEvent):
        """Invokes all registered callbacks with the signal event."""
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception as e:
                self.logger.exception(f"Error in signal callback: {e}") 