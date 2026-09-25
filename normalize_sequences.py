import json
import os

INPUT_PATH = "data/raw/actual_sequences.json"
OUTPUT_PATH = "data/bronze/actual_sequences.jsonl"

os.makedirs("data/bronze", exist_ok=True)

with open(INPUT_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

count = 0

with open(OUTPUT_PATH, "w", encoding="utf-8") as out:
    for route_id, route_data in data.items():

        # الملف عادة يحتوي actual داخل كل route
        actual = route_data.get("actual", {})

        for stop_id, sequence_no in actual.items():
            record = {
                "route_id": route_id,
                "stop_id": stop_id,
                "actual_sequence": sequence_no
            }

            out.write(json.dumps(record) + "\n")
            count += 1

print("Sequences normalized successfully.")
print("Rows:", count)
print("Output:", OUTPUT_PATH)