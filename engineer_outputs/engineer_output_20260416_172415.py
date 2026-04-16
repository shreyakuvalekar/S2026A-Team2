import pandas as pd

df = pd.DataFrame(raw_data)
df = df[~df['course_title'].str.contains('[^\x00-\x7F]+')]
df = df.groupby(df['course_title'].str[0]).head(20).reset_index(drop=True)
result = df.to_dict(orient='records')