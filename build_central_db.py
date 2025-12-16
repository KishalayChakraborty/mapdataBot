import os
import glob
import json
import re
from typing import List, Dict, Any

import pandas as pd
import numpy as np


WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_COMBINED_CSV = os.path.join(WORKSPACE_DIR, "combined_places.csv")
OUTPUT_COUNTS_CSV = os.path.join(WORKSPACE_DIR, "counts_by_city_and_type.csv")
OUTPUT_COUNTS_TXT = os.path.join(WORKSPACE_DIR, "counts_summary.txt")


def find_result_files(root: str) -> List[str]:
    pattern = os.path.join(root, "results_*.json")
    return sorted(glob.glob(pattern))


def parse_filename_info(filepath: str) -> Dict[str, str]:
    """Parse `results_{locationtype}_{area}_{city}.json` into parts.

    - location_type: first token after `results_`
    - city: last token before `.json`
    - area: all middle tokens joined with `_`
    """
    name = os.path.basename(filepath)
    if not name.startswith("results_") or not name.endswith(".json"):
        return {"location_type": "unknown", "area": "unknown", "city": "unknown"}

    core = name[len("results_"):-len(".json")]  # strip prefix and suffix
    parts = core.split("_")
    if len(parts) < 2:
        return {"location_type": "unknown", "area": "unknown", "city": parts[-1] if parts else "unknown"}

    location_type = parts[0]
    city = parts[-1]
    area = "_".join(parts[1:-1]) if len(parts) > 2 else ""
    return {"location_type": location_type, "area": area, "city": city}


def load_entries(filepath: str) -> List[Dict[str, Any]]:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("results", "data", "items", "places"):
            val = data.get(key)
            if isinstance(val, list):
                return val
        # If none of the typical keys are lists, treat the dict as a single record
        return [data]
    # Unsupported structure -> return empty
    return []


LAT_CANDIDATES = [
    "lat",
    "geometry.location.lat",
    "geometry.lat",
    "location.lat",
    "coordinates.lat",
    "latitude",
]
LON_CANDIDATES = [
    "lon",
    "lng",
    "geometry.location.lng",
    "geometry.lng",
    "location.lng",
    "coordinates.lng",
    "longitude",
]


def extract_lat_lon_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create canonical `lat` and `lon` columns from any present candidate columns."""
    def to_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce")

    lat_sources = [c for c in LAT_CANDIDATES if c in df.columns]
    lon_sources = [c for c in LON_CANDIDATES if c in df.columns]

    if lat_sources:
        lat_vals = df[lat_sources].apply(to_numeric)
        lat = lat_vals.bfill(axis=1).iloc[:, 0]
    else:
        lat = pd.Series(np.nan, index=df.index)

    if lon_sources:
        lon_vals = df[lon_sources].apply(to_numeric)
        lon = lon_vals.bfill(axis=1).iloc[:, 0]
    else:
        lon = pd.Series(np.nan, index=df.index)

    df = df.copy()
    df["lat"] = lat
    df["lon"] = lon
    return df


def filter_valid_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rows where lat/lon are finite and within valid ranges."""
    valid_lat = df["lat"].between(-90, 90, inclusive="both")
    valid_lon = df["lon"].between(-180, 180, inclusive="both")
    finite = np.isfinite(df["lat"]) & np.isfinite(df["lon"])  # excludes NaN and inf
    return df[valid_lat & valid_lon & finite]


def normalize_entries(entries: List[Dict[str, Any]]) -> pd.DataFrame:
    # Flatten nested records while keeping dotted paths
    df = pd.json_normalize(entries, sep=".")
    df = extract_lat_lon_columns(df)
    return df


def main() -> None:
    files = find_result_files(WORKSPACE_DIR)
    if not files:
        print("No results_*.json files found.")
        return

    frames: List[pd.DataFrame] = []

    for fp in files:
        meta = parse_filename_info(fp)
        entries = load_entries(fp)
        if not entries:
            continue

        df = normalize_entries(entries)
        df = filter_valid_lat_lon(df)

        # Add file-derived metadata
        df["location_type"] = meta["location_type"].lower()
        df["area"] = meta["area"].replace("_", " ") if meta["area"] else ""
        df["city"] = meta["city"].lower()
        df["source_file"] = os.path.basename(fp)

        # Prefer common display fields if present
        # Ensure presence of helpful columns
        for col in [
            "name",
            "formatted_address",
            "vicinity",
            "place_id",
            "rating",
            "user_ratings_total",
            "types",
        ]:
            if col not in df.columns:
                df[col] = pd.NA

        frames.append(df)

    if not frames:
        print("No valid entries with lat/lon found across files.")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Save combined DB
    combined.to_csv(OUTPUT_COMBINED_CSV, index=False)

    # Aggregate counts by city and location type
    counts = (
        combined.groupby(["city", "location_type"])  # type: ignore[arg-type]
        .size()
        .reset_index(name="count")
        .sort_values(["city", "location_type"])
    )

    # Save counts
    counts.to_csv(OUTPUT_COUNTS_CSV, index=False)

    # Human-readable summary
    lines = [
        f"{row.city} {row.location_type} = {int(row['count'])}"
        for _, row in counts.iterrows()
    ]
    with open(OUTPUT_COUNTS_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    # Console output
    print(f"Combined rows (valid lat/lon): {len(combined)}")
    print("Counts by city and location_type:")
    for line in lines:
        print(" -", line)
    print("\nOutputs:")
    print(f" - {OUTPUT_COMBINED_CSV}")
    print(f" - {OUTPUT_COUNTS_CSV}")
    print(f" - {OUTPUT_COUNTS_TXT}")


if __name__ == "__main__":
    main()
