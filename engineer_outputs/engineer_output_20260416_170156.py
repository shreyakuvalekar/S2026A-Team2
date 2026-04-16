import json

# Parse the input JSON string
data = json.loads(input_json)

# Transform the data
result = []
for item in data:
    # Create a new dictionary with the required fields
    transformed_item = {
        "course_title": item["course_title"],
        "course_organization": item["course_organization"],
        "course_rating": item["course_rating"],
        "course_difficulty": item["course_difficulty"],
        "course_students_enrolled": item["course_students_enrolled"]
    }
    result.append(transformed_item)

# Convert the result back to JSON string
result = json.dumps(result, indent=2)