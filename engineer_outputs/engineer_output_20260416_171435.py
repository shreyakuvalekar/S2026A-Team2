import pandas as pd

# Convert list of dictionaries into a DataFrame
df = pd.DataFrame(data)

# Drop rows where 'no' is not NaN
result = df.dropna(subset=['no'])

# Reset index after dropping rows
result.reset_index(drop=True, inplace=True)