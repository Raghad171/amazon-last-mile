import json
import math
import os

INPUT_PATH = "data/raw/package_data.json"
OUTPUT_PATH = "data/bronze/packages.jsonl"

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


package_count = 0


with open(OUTPUT_PATH, "w", encoding="utf-8") as out:

    for route_id, route_data in data.items():

        for stop_id, packages in route_data.items():

            for package_id, package_data in packages.items():

                dimensions = package_data.get("dimensions", {}) or {}
                time_window = package_data.get("time_window", {}) or {}

                depth = dimensions.get("depth_cm")
                height = dimensions.get("height_cm")
                width = dimensions.get("width_cm")

                package_volume_cm3 = None

                if (
                    depth is not None
                    and height is not None
                    and width is not None
                ):
                    package_volume_cm3 = depth * height * width

                record = {
                    "route_id": route_id,
                    "stop_id": stop_id,
                    "package_id": package_id,
                    "scan_status": package_data.get("scan_status"),
                    "time_window_start_utc": time_window.get("start_time_utc"),
                    "time_window_end_utc": time_window.get("end_time_utc"),
                    "planned_service_time_seconds":
                        package_data.get("planned_service_time_seconds"),
                    "depth_cm": depth,
                    "height_cm": height,
                    "width_cm": width,
                    "package_volume_cm3": package_volume_cm3
                }

                out.write(
                    json.dumps(clean_value(record)) + "\n"
                )

                package_count += 1


print("Packages normalized successfully.")
print("Packages:", package_count)
print("Output:", OUTPUT_PATH)