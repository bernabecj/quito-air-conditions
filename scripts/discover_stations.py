"""
List the OpenAQ stations around Quito and the sensors each one carries.

Phase 1, step 1: confirm which Red Metropolitana stations OpenAQ actually
exposes, which parameters they report and when each last reported, before the
producer is written against them.

    python -m scripts.discover_stations
    python -m scripts.discover_stations --json data/local/quito_sensors.json
"""

import argparse
import json
from pathlib import Path

from producers.openaq import fetch_quito_locations, flatten_sensors, get_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--json", type=Path, help="also write the sensor rows to this file")
    args = parser.parse_args()

    locations = fetch_quito_locations(get_api_key())
    rows = flatten_sensors(locations)

    print(f"{len(locations)} locations, {len(rows)} sensors\n")
    print(f"{'location':<10}{'name':<32}{'sensor':<12}{'parameter':<14}{'last seen (UTC)'}")
    for row in sorted(rows, key=lambda r: (r["location_name"], r["parameter"])):
        print(
            f"{row['location_id']:<10}{row['location_name'][:31]:<32}"
            f"{row['sensor_id']:<12}{row['parameter']:<14}{row['last_seen_utc'] or '-'}"
        )

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nWrote {args.json}")


if __name__ == "__main__":
    main()
