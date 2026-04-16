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
    "symbol": "AAPL",
    "open": 152.0,
    "close": 151.0,
    "volume": 870000
  }
]

df = pd.DataFrame(raw_data)
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
df.rename(columns={'symbol': 'ticker'}, inplace=True)
df[['open', 'close']] = df[['open', 'close']].astype(float)
df['volume'] = df['volume'].astype(int)
df = df[df['ticker'] == "AAPL"]
df['price_change'] = df['close'] - df['open']
df.sort_values(by='date', inplace=True)

result = df.to_dict(orient='records')