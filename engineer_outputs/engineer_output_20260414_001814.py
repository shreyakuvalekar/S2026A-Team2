import pandas as pd

# Transform raw_data according to the plan
df = pd.DataFrame(raw_data)
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
df['open'] = df['open'].astype(float)
df['close'] = df['close'].astype(float)
df['volume'] = df['volume'].astype(int)

# Filter rows where symbol equals "AAPL"
df = df[df['symbol'] == 'AAPL']

# Rename the date column to trade_date
df.rename(columns={'date': 'trade_date'}, inplace=True)

# Add a new column price_change
df['price_change'] = df['close'] - df['open']

# Sort records by trade_date in ascending order
df.sort_values(by='trade_date', inplace=True)

# Save the transformed data to /tmp/demo_etl_output.csv
df.to_csv('/tmp/demo_etl_output.csv', index=False)

# Assign final result as a list of dicts
result = df.to_dict(orient='records')