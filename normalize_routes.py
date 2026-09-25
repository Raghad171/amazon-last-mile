import json
import math
import os

INPUT_PATH = "data/raw/route_data.json"

ROUTES_OUTPUT = "data/bronze/routes.jsonl"
STOPS_OUTPUT = "data/bronze/stops.jsonl"

os.makedirs("data/bronze", exist_ok=True)


def clean_value(value):
    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, dict):
        return {k: clean_value(v) for k, v in value.items()}

    if isinstance(value, list):
        return [clean_value(v) for v in value]

    return value


with open(INPUT_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)


route_count = 0
stop_count = 0


with open(ROUTES_OUTPUT, "w", encoding="utf-8") as routes_file, \
     open(STOPS_OUTPUT, "w", encoding="utf-8") as stops_file:

    for route_id, route_data in data.items():

        route_record = {
            "route_id": route_id,
            "station_code": route_data.get("station_code"),
            "date_YYYY_MM_DD": route_data.get("date_YYYY_MM_DD"),
            "departure_time_utc": route_data.get("departure_time_utc"),
            "executor_capacity_cm3": route_data.get("executor_capacity_cm3"),
            "route_score": route_data.get("route_score")
        }

        routes_file.write(
            json.dumps(clean_value(route_record)) + "\n"
        )

        route_count += 1


        stops = route_data.get("stops", {})

        for stop_id, stop_data in stops.items():

            stop_record = {
                "route_id": route_id,
                "stop_id": stop_id,
                "lat": stop_data.get("lat"),
                "lng": stop_data.get("lng"),
                "stop_type": stop_data.get("type"),
                "zone_id": stop_data.get("zone_id")
            }

            stops_file.write(
                json.dumps(clean_value(stop_record)) + "\n"
            )

            stop_count += 1


print("Normalization completed successfully.")
print("Routes:", route_count)
print("Stops:", stop_count)
print("Routes file:", ROUTES_OUTPUT)
print("Stops file:", STOPS_OUTPUT) 