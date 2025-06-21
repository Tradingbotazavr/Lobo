import sys
import os
import pytest

# Add project root to path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lob_analyzer.collector.orderbook_realtime import RealTimeOrderBook

@pytest.fixture
def orderbook():
    """Provides a default RealTimeOrderBook instance for tests."""
    return RealTimeOrderBook(symbol="TESTUSDT", depth_pct=0.1) # Use 10% depth for easy testing

def test_initial_state(orderbook: RealTimeOrderBook):
    """Tests that the order book is empty upon initialization."""
    assert not orderbook.bids
    assert not orderbook.asks
    assert orderbook.get_mid_price() is None
    assert orderbook.get_imbalance() == (0.0, 0.0, 0.0)

def test_update_from_message(orderbook: RealTimeOrderBook):
    """Tests that the order book state is correctly updated from a message."""
    callback_called = False
    def on_update_callback(ob_instance):
        nonlocal callback_called
        assert isinstance(ob_instance, RealTimeOrderBook)
        callback_called = True
    
    orderbook.on_update(on_update_callback)

    sample_data = {
        "b": [["100.0", "10.0"], ["99.0", "5.0"]],  # Bids
        "a": [["101.0", "12.0"], ["102.0", "8.0"]], # Asks
    }
    
    orderbook._update_from_message(sample_data)

    assert callback_called, "The on_update callback was not called"
    
    # Check bids (SortedDict sorts descending by price)
    assert list(orderbook.bids.keys()) == [100.0, 99.0]
    assert orderbook.bids[100.0] == 10.0
    
    # Check asks (SortedDict sorts ascending by price)
    assert list(orderbook.asks.keys()) == [101.0, 102.0]
    assert orderbook.asks[101.0] == 12.0

    # Test removal of a level by setting quantity to 0
    sample_data_update = {
        "b": [["99.0", "0"]],
        "a": [],
    }
    orderbook._update_from_message(sample_data_update)
    assert list(orderbook.bids.keys()) == [100.0]

def test_calculations(orderbook: RealTimeOrderBook):
    """Tests the correctness of mid-price and imbalance calculations."""
    sample_data = {
        "b": [["100.0", "10.0"], ["98.0", "20.0"]], # Total bid vol = 30
        "a": [["102.0", "15.0"], ["104.0", "5.0"]], # Total ask vol = 20
    }
    orderbook._update_from_message(sample_data)

    # Mid price
    assert orderbook.get_best_bid() == 100.0
    assert orderbook.get_best_ask() == 102.0
    assert orderbook.get_mid_price() == 101.0

    # Imbalance (mid is 101, depth_pct is 0.1, so range is 90.9 to 111.1)
    # All bids and asks are within this range
    imbalance, bid_vol, ask_vol = orderbook.get_imbalance()
    
    assert bid_vol == 30.0
    assert ask_vol == 20.0
    # Imbalance = (30 - 20) / (30 + 20) = 10 / 50 = 0.2
    assert pytest.approx(imbalance) == 0.2

def test_imbalance_with_depth_limit(orderbook: RealTimeOrderBook):
    """Tests that imbalance calculation correctly respects the depth_pct limit."""
    sample_data = {
        "b": [["100.0", "10.0"], ["80.0", "50.0"]], # Bid at 80 is outside depth
        "a": [["102.0", "15.0"], ["120.0", "40.0"]], # Ask at 120 is outside depth
    }
    orderbook._update_from_message(sample_data)

    # mid_price is (100+102)/2 = 101
    # With depth_pct=0.1, range is [90.9, 111.1]
    
    imbalance, bid_vol, ask_vol = orderbook.get_imbalance()

    # Only bid at 100.0 (vol 10) is inside
    assert bid_vol == 10.0
    # Only ask at 102.0 (vol 15) is inside
    assert ask_vol == 15.0
    # Imbalance = (10 - 15) / (10 + 15) = -5 / 25 = -0.2
    assert pytest.approx(imbalance) == -0.2

if __name__ == "__main__":
    pytest.main() 