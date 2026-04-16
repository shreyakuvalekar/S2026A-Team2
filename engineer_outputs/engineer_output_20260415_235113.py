import json
import pandas as pd

raw_data = [
  {"Unnamed: 0": 134, "course_title": "(ISC)\\xb2 Systems Security Certified Practitioner (SSCP)", "course_organization": "(ISC)\\xb2", "course_Certificate_type": "SPECIALIZATION", "course_rating": 4.7, "course_difficulty": "Beginner", "course_students_enrolled": "5.3k"},
  {"Unnamed: 0": 743, "course_title": "A Crash Course in Causality:  Inferring Causal Effects from Observational Data", "course_organization": "University of Pennsylvania", "course_Certificate_type": "COURSE", "course_rating": 4.7, "course_difficulty": "Intermediate", "course_students_enrolled": "17k"},
  {"Unnamed: 0": 874, "course_title": "A Crash Course in Data Science", "course_organization": "Johns Hopkins University", "course_Certificate_type": "COURSE", "course_rating": 4.5, "course_difficulty": "Mixed", "course_students_enrolled": "130k"}
]

df = pd.DataFrame(raw_data)
df = df[~df['course_title'].apply(lambda x: len(x.encode('ascii', 'ignore')) != len(x))]
result = df.to_dict(orient='records')