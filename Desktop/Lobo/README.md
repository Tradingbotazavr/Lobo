# Lobo Trading Analyzer

This project is a real-time cryptocurrency market data collector and analyzer. It is designed to connect to Binance, capture order book snapshots and trade data, calculate various market features, and detect significant activity spikes that could indicate trading opportunities.

## Core Features
- **Real-time Data Collection**: Streams aggregated trades and order book depth from Binance.
- **Feature Engineering**: Calculates metrics like order book imbalance, near-book volume, volume-weighted average price (VWAP), and trade volume velocity.
- **Spike Detection**: Identifies unusual trading activity based on volume and imbalance.
- **Data Storage**: Saves the collected and processed data into Parquet files for efficient offline analysis and model training.
- **Asynchronous Architecture**: Built with `asyncio` for high performance and concurrency. 