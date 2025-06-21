import sys
import os
import pytest
from loguru import logger

# Add project root to path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lob_analyzer.analyzer.trade_activity_analyzer import TradeActivityAnalyzer

# Configure Loguru to output to console for tests, but disable for the fixture
logger.remove()
logger.add(sys.stderr, level="INFO")

@pytest.fixture
def analyzer():
    """Provides a default TradeActivityAnalyzer instance for tests."""
    # Disable logging within the analyzer itself to avoid cluttering test output,
    # as we are testing callback functionality directly.
    return TradeActivityAnalyzer(
        volume_threshold=50.0,
        imbalance_ratio_high=3.0,  # 3:1
        imbalance_ratio_low=1/3,   # 1:3
        spike_multiplier=2.0,      # Spike if current volume is 2x the moving average
        enable_logging=False  # Disabled for cleaner tests
    )

def test_no_spike_on_normal_activity(analyzer):
    """
    Tests that no spike is detected for normal, balanced market activity.
    """
    detected_reason = None
    def callback(data):
        nonlocal detected_reason
        detected_reason = data.get('reason')

    analyzer.on_activity(callback)

    # Normal event: balanced volume, below threshold, no historical data yet
    normal_event = {
        'ts': 1234567890,
        'buy_volume': 20.0,
        'sell_volume': 20.0,
        'buy_mean': 15.0, # from TradesStream
        'sell_mean': 15.0, # from TradesStream
        'trade_count': 10
    }
    
    analyzer.process_event(normal_event)

    assert detected_reason is None, "Spike should not be detected for normal activity"

def test_spike_on_high_volume(analyzer):
    """
    Tests that a spike is detected when total volume exceeds the absolute threshold.
    """
    detected_reason = None
    def callback(data):
        nonlocal detected_reason
        detected_reason = data.get('reason')

    analyzer.on_activity(callback)
    
    high_volume_event = {
        'ts': 1234567890,
        'buy_volume': 30.0,
        'sell_volume': 25.0, # Total volume = 55, threshold = 50
        'buy_mean': 15.0,
        'sell_mean': 15.0,
        'trade_count': 20
    }
    
    analyzer.process_event(high_volume_event)

    assert detected_reason == 'volume_exceeded', "Spike reason should be 'volume_exceeded'"

def test_spike_on_imbalance(analyzer):
    """
    Tests that a spike is detected due to a high buy/sell imbalance ratio.
    """
    detected_reason = None
    def callback(data):
        nonlocal detected_reason
        detected_reason = data.get('reason')

    analyzer.on_activity(callback)
    
    imbalance_event = {
        'ts': 1234567890,
        'buy_volume': 35.0,
        'sell_volume': 10.0, # Ratio = 3.5, threshold = 3.0
        'buy_mean': 15.0,
        'sell_mean': 15.0,
        'trade_count': 15
    }
    
    analyzer.process_event(imbalance_event)

    assert detected_reason == 'imbalance', "Spike reason should be 'imbalance'"

def test_spike_on_volume_spike_vs_ma(analyzer):
    """
    Tests that a spike is detected when current volume is significantly
    higher than the moving average.
    """
    detected_reason = None
    def callback(data):
        nonlocal detected_reason
        detected_reason = data.get('reason')

    analyzer.on_activity(callback)
    
    # Total volume is 40, MA volume is 15. 40 / 15 > 2.0
    spike_event = {
        'ts': 1234567890,
        'buy_volume': 20.0,
        'sell_volume': 20.0,
        'buy_mean': 10.0,
        'sell_mean': 5.0,
        'trade_count': 25
    }
    
    analyzer.process_event(spike_event)

    assert detected_reason == 'spike', "Spike reason should be 'spike'"

if __name__ == "__main__":
    pytest.main() 