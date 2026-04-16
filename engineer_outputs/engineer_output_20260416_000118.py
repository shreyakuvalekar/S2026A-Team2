import pandas as pd

# Convert list of dictionaries into a DataFrame
df = pd.DataFrame(courses)

# Drop rows where 'course_title' column contains '\u041c\u0430\u0448'
filtered_df = df[~df['course_title'].str.contains('\u041c\u0430\u0448')]

result = filtered_df.to_dict(orient='records')