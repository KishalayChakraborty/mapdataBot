import os
import pandas as pd

WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(WORKSPACE_DIR, "combined_places.csv")
OUTPUT_CSV = os.path.join(WORKSPACE_DIR, "combined_places_deduped.csv")


def main() -> None:
    if not os.path.exists(INPUT_CSV):
        print(f"Input not found: {INPUT_CSV}")
        return

    df = pd.read_csv(INPUT_CSV, dtype=str)  # read all as strings to preserve exactness

    # Ensure expected columns exist
    if "name" not in df.columns:
        print("Column 'name' missing in input; cannot dedupe by name.")
        return
    # Build a single 'address' field using exact strings
    # Prefer formatted_address; fall back to vicinity; else empty string
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
        # No address fields present; dedupe only by name
        address = pd.Series(["" for _ in range(len(df))], index=df.index)

    df["address"] = address

    before = len(df)
    deduped = df.drop_duplicates(subset=["name", "address"], keep="first")
    after = len(deduped)
    removed = before - after

    deduped.to_csv(OUTPUT_CSV, index=False)

    print(f"Input rows: {before}")
    print(f"Deduped rows: {after}")
    print(f"Duplicates removed: {removed}")
    print(f"Output written: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
