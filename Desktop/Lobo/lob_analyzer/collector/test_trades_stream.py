import asyncio
import json
import sys
import os
import time
import pytest

# Add project root to path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lob_analyzer.collector.trades_stream import TradesStream, TradeEvent

@pytest.fixture
def stream():
    """Provides a default TradesStream instance for tests."""
    # Use a short aggregation interval for faster testing
    return TradesStream(symbol="TESTUSDT", agg_interval=0.1)

@pytest.fixture
def monkeypatch_time(monkeypatch):
    """Fixture to mock and control time.time()."""
    current_time = [time.time()]
    def mock_time():
        return current_time[0]
    
    def advance_time(seconds: float):
        current_time[0] += seconds

    monkeypatch.setattr(time, 'time', mock_time)
    # Return the function to advance time so tests can use it
    return advance_time

@pytest.mark.anyio
async def test_process_valid_message(stream: TradesStream):
    """Tests processing of a valid aggTrade message and raw trade callback."""
    raw_trade_received = None
    def on_raw_trade_callback(event: TradeEvent):
        nonlocal raw_trade_received
        raw_trade_received = event

    stream.on_raw_trade(on_raw_trade_callback)

    ts = int(time.time() * 1000)
    msg = json.dumps({
        "e": "aggTrade", "p": "100.50", "q": "10.0", "m": True, "T": ts,
    })

    await stream._process_message(msg)

    assert raw_trade_received is not None, "Raw trade callback was not called"
    assert raw_trade_received.price == 100.50
    assert raw_trade_received.qty == 10.0
    assert raw_trade_received.side == "sell"
    assert len(stream._trade_buffer) == 1

@pytest.mark.anyio
async def test_aggregation_logic(stream: TradesStream, monkeypatch_time):
    """Tests the aggregation logic for correctness."""
    agg_data_received = None
    def on_agg_callback(data: dict):
        nonlocal agg_data_received
        agg_data_received = data

    stream.on_trade_aggregate(on_agg_callback)
    current_time = time.time()

    # Add some trades to the buffer
    async with stream._buffer_lock:
        stream._trade_buffer.append(TradeEvent(ts=current_time - 0.05, price=100, qty=10, side="buy", symbol="TESTUSDT"))
        stream._trade_buffer.append(TradeEvent(ts=current_time - 0.03, price=101, qty=5, side="sell", symbol="TESTUSDT"))
        stream._trade_buffer.append(TradeEvent(ts=current_time - 0.01, price=102, qty=20, side="buy", symbol="TESTUSDT"))

    await stream._perform_aggregation()
    
    assert agg_data_received is not None, "Aggregate callback was not called"
    assert agg_data_received['buy_volume'] == 30.0
    assert agg_data_received['sell_volume'] == 5.0
    assert agg_data_received['trade_count'] == 3
    # imbalance = (30 - 5) / (30 + 5) = 25 / 35
    assert pytest.approx(agg_data_received['imbalance']) == 25 / 35

@pytest.mark.anyio
async def test_aggregation_clears_old_trades(stream: TradesStream, monkeypatch_time):
    """Tests that the aggregation correctly clears old trades from the buffer."""
    current_time = time.time()
    
    async with stream._buffer_lock:
        # This trade is old and should be removed during aggregation
        stream._trade_buffer.append(TradeEvent(ts=current_time - stream.agg_interval * 2, price=98, qty=50, side="buy", symbol="TESTUSDT"))
        # This trade is recent and should be kept
        stream._trade_buffer.append(TradeEvent(ts=current_time - stream.agg_interval * 0.5, price=100, qty=10, side="buy", symbol="TESTUSDT"))

    # The aggregation cutoff is `now - agg_interval`
    await stream._perform_aggregation()

    async with stream._buffer_lock:
        assert len(stream._trade_buffer) == 1, "Old trades were not cleared"
        assert stream._trade_buffer[0].price == 100

@pytest.mark.anyio
async def test_anomaly_detection(stream: TradesStream, monkeypatch_time):
    """Tests that the aggregation logic correctly identifies a volume anomaly."""
    agg_data_received = None
    def on_agg_callback(data: dict):
        nonlocal agg_data_received
        agg_data_received = data

    stream.on_trade_aggregate(on_agg_callback)
    
    # Prime history with normal volume, so we have a baseline mean/std
    stream.agg_history.append({'buy_vol': 10, 'sell_vol': 10})
    stream.agg_history.append({'buy_vol': 12, 'sell_vol': 8})
    # Mean buy_vol ~ 11, std ~ 1
    
    # Add a new trade that constitutes a significant spike
    current_time = time.time()
    async with stream._buffer_lock:
        stream._trade_buffer.append(TradeEvent(ts=current_time - 0.01, price=100, qty=100, side="buy", symbol="TESTUSDT"))

    await stream._perform_aggregation()

    assert agg_data_received is not None, "Aggregate callback was not called"
    assert 'anomaly' in agg_data_received, "Anomaly was not detected"
    assert agg_data_received['anomaly'] == 'buy' 