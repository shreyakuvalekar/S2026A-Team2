import json
import pandas as pd

# Assuming raw_data is defined and available here.
df = pd.DataFrame(raw_data)

# Step 1: Filter rows based on ASCII characters in course_title
def contains_only_ascii(s):
    return all(ord(c) < 128 for c in s)

df['is_ascii'] = df['course_title'].apply(contains_only_ascii)
df = df[df['is_ascii']]

# Verify that the remaining data does not contain non-ASCII characters.
assert all(df['is_ascii'])

result = df.to_dict(orient='records')