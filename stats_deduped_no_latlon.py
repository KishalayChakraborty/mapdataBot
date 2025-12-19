import os
import glob
import json
from typing import List, Dict, Any

import pandas as pd

WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DEDUPED_CSV = os.path.join(WORKSPACE_DIR, "combined_deduped_no_latlon.csv")
OUTPUT_COUNTS_CSV = os.path.join(WORKSPACE_DIR, "counts_deduped_no_latlon_by_city.csv")
OUTPUT_SUMMARY_TXT = os.path.join(WORKSPACE_DIR, "counts_deduped_no_latlon_summary.txt")


def find_result_files(root: str) -> List[str]:
    return sorted(glob.glob(os.path.join(root, "results_*.json")))


def parse_filename_info(filepath: str) -> Dict[str, str]:
    name = os.path.basename(filepath)
    if not name.startswith("results_") or not name.endswith(".json"):
        return {"location_type": "unknown", "area": "", "city": "unknown"}
    core = name[len("results_"):-len(".json")]
    parts = core.split("_")
    if len(parts) < 2:
        return {"location_type": "unknown", "area": "", "city": parts[-1] if parts else "unknown"}
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
        return [data]
    return []


def build_address_column(df: pd.DataFrame) -> pd.Series:
    formatted = df["formatted_address"] if "formatted_address" in df.columns else None
    vicinity = df["vicinity"] if "vicinity" in df.columns else None

    if formatted is not None:
        address = formatted.copy()
        if vicinity is not None:
            address = address.where(address.notna(), vicinity)
        address = address.fillna("")
    elif vicinity is not None:
        address = vicinity.fillna("")
    else:
        address = pd.Series(["" for _ in range(len(df))], index=df.index)
    return address


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
        df = pd.json_normalize(entries, sep=".")

        # Keep only fields relevant to dedup + useful context
        for col in [
            "name",
            "formatted_address",
            "vicinity",
            "place_id",
            "types",
            "rating",
            "user_ratings_total",
        ]:
            if col not in df.columns:
                df[col] = pd.NA

        df["city"] = meta["city"].lower()
        df["location_type"] = meta["location_type"].lower()
        df["source_file"] = os.path.basename(fp)

        frames.append(df)

    if not frames:
        print("No entries found in any JSON files.")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Build exact address string and dedupe globally on name+address
    combined["address"] = build_address_column(combined)

    # Cast name/address to string for exact matching, preserving exact text
    combined["name"] = combined["name"].astype(str)
    combined["address"] = combined["address"].astype(str)

    before = len(combined)
    deduped = combined.drop_duplicates(subset=["name", "address"], keep="first")
    after = len(deduped)
    removed = before - after

    # Save deduped combined rows (no lat/lon validation)
    deduped.to_csv(OUTPUT_DEDUPED_CSV, index=False)

    # City-wise counts on the deduped set
    counts = (
        deduped.groupby(["city"])  # type: ignore[arg-type]
        .size()
        .reset_index(name="count")
        .sort_values(["city"]) 
    )
    counts.to_csv(OUTPUT_COUNTS_CSV, index=False)

    total = int(after)

    # Human-readable summary
    lines = [f"{row.city} = {int(row['count'])}" for _, row in counts.iterrows()]
    with open(OUTPUT_SUMMARY_TXT, "w", encoding="utf-8") as f:
        f.write(f"TOTAL (deduped, no lat/lon validation) = {total}\n")
        f.write("\n".join(lines) + "\n")

    print(f"Input rows (all JSON): {before}")
    print(f"Deduped rows (global name+address): {after}")
    print(f"Duplicates removed: {removed}")
    print("City-wise counts (deduped, no lat/lon validation):")
    for line in lines:
        print(" -", line)
    print("\nOutputs:")
    print(f" - {OUTPUT_DEDUPED_CSV}")
    print(f" - {OUTPUT_COUNTS_CSV}")
    print(f" - {OUTPUT_SUMMARY_TXT}")


if __name__ == "__main__":
    main()
