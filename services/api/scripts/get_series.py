import json

# Path to your downloaded JSON file
FILE_PATH = "/Users/julianasogwa/Downloads/api-response.json"

with open(FILE_PATH, "r") as f:
    data = json.load(f)

categories = set()

for series in data['series']:
    category = series.get("category")
    if category:
        categories.add(category.strip())

# Print sorted, unique categories
for c in sorted(categories):
    print(c)

print(f"\nTotal unique categories: {len(categories)}")
