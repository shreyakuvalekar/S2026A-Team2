import pandas as pd

raw_data = [
  {
    "date": "2024-01-01",
    "symbol": "AAPL",
    "open": 149.0,
    "close": 150.5,
    "volume": 1200000
  },
  {
    "date": "2024-01-02",
    "symbol": "AAPL",
    "open": 150.5,
    "close": 152.0,
    "volume": 980000
  },
  {
    "date": "2024-01-03",
    "symbol": "GOOGL",
    "open": 152.0,
    "close": 151.0,
    "volume": 870000
  }
]

df = pd.DataFrame(raw_data)
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
df['open'] = df['open'].astype(float)
df['close'] = df['close'].astype(float)
df['volume'] = df['volume'].astype(int)

# Filter rows where symbol equals "AAPL"
df = df[df['symbol'] == 'AAPL']

# Rename the date column to trade_date
df.rename(columns={'date': 'trade_date'}, inplace=True)

# Add a new column price_change which is calculated as close - open
df['price_change'] = df['close'] - df['open']

# Sort records by trade_date in ascending order
df.sort_values(by='trade_date', inplace=True)

result = df.to_dict(orient='records')