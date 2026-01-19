import os
import csv
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

import psycopg2
from pprint import pprint
from psycopg2.extras import execute_values

# Helper: get env with optional default

def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(name)
    return val if val is not None else default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload POIs from CSV into PostgreSQL skytron_api_pointofinterests")
    parser.add_argument("--csv", dest="csv_path", default="combined_places_dedupedold.csv", help="Path to input CSV file")
    parser.add_argument("--batch-size", type=int, default=1000, help="Insert batch size")
    parser.add_argument("--dry-run", action="store_true", help="Preview mapping without inserting")
    parser.add_argument("--on-conflict", choices=["ignore", "update"], default="ignore", help="Conflict handling on unique name")
    parser.add_argument("--created-by-id", type=int, default=int(getenv("DEFAULT_CREATED_BY_ID", "1")), help="Default created_by_id if not provided")
    parser.add_argument("--updated-by-id", type=int, default=int(getenv("DEFAULT_UPDATED_BY_ID", "1")), help="Default updated_by_id if not provided")
    parser.add_argument("--status", default=getenv("DEFAULT_STATUS", "active"), help="Default status value")
    parser.add_argument("--status2", default=getenv("DEFAULT_STATUS2", "active"), help="Default status2 value")
    parser.add_argument("--mark-type", default=getenv("DEFAULT_MARK_TYPE", "poi"), help="Default mark_type when not derivable")
    parser.add_argument("--use-type", default=getenv("DEFAULT_USE_TYPE", "public"), help="Default use_type")
    parser.add_argument("--show-mapping", action="store_true", help="Show CSV→DB mapping spec and exit")
    # Mapping customization
    parser.add_argument(
        "--desc-source",
        default=getenv("DESC_SOURCE", "types,vicinity,formatted_address,address,name"),
        help="Comma-separated priority for description derivation"
    )
    parser.add_argument(
        "--location-source",
        default=getenv("LOCATION_SOURCE", "formatted_address,address,latlon,city,area,name"),
        help="Comma-separated priority for location text derivation"
    )
    parser.add_argument("--radius-default", type=float, default=None, help="Default radius to set for all rows")
    parser.add_argument("--speed-limit", type=int, default=None, help="Override speed_limit (else DB default applies)")
    parser.add_argument("--alert-type", default=None, help="Override alert_type (else DB default applies)")
    # Safety/targeting
    parser.add_argument("--rollback", action="store_true", help="Execute inserts but ROLLBACK instead of COMMIT (no writes)")
    parser.add_argument("--table", default="public.skytron_api_pointofinterests", help="Target table (e.g., public.skytron_api_pointofinterests_staging)")
    return parser.parse_args()


# Explicit CSV → DB mapping specification for clarity
# Keys are DB columns; values describe CSV sources and defaults
MAPPING_SPEC: Dict[str, Dict[str, Any]] = {
    "name": {"from": ["name"], "default": None},
    "description": {"from": ["address", "plus_code", "area", "city", "formatted_address"], "default": "name", "join": " + "},
    # 'location' is a composite derived from CSV columns 'lat' and 'lon'
    "location": {"from": ["lat", "lon"], "default": "name", "format": "[[{lat},{lon}]]"},
    "mark_type": {"from": [], "default": "Point"},
    "use_type": {"from": ["location_type"], "default": None},
    "status": {"from": [], "default": "Active"},
    "status2": {"from": [], "default": "Active"},
    "radius": {"from": [], "default": "--radius-default (None → leave NULL)"},
    "created": {"from": [], "default": "UTC now"},
    "updated": {"from": [], "default": "UTC now"},
    "created_by_id": {"from": [], "default": 1},
    "updated_by_id": {"from": [], "default": "--updated-by-id"},
    "speed_limit": {"from": [], "default": "--speed-limit or DB default (100)"},
    "alert_type": {"from": [], "default": "--alert-type or DB default ('none')"},
    "lat": {"from": ["lat"], "default": None},
    "lon": {"from": ["lon"], "default": None},
    "address": {"from": ["address", "plus_code", "area", "city", "formatted_address"], "default": None, "join": " + "},
    "pluscode": {"from": ["plus_code", "pluscode"], "default": None},
    "area": {"from": ["area"], "default": None},
    "city": {"from": ["city"], "default": None},
    "state": {"from": ["state"], "default": None},  # not present in CSV → stays NULL
    "pincode": {"from": ["pincode"], "default": None},  # not present in CSV → stays NULL
    "phone": {"from": ["phone"], "default": None},
    "website": {"from": ["website"], "default": None},
}


def show_mapping(headers: List[str]) -> None:
    print("CSV headers detected:")
    print(headers)
    print("\nDB mapping specification (DB column → {from, default}):")
    pprint(MAPPING_SPEC)


def connect_db() -> psycopg2.extensions.connection:
    host = getenv("DB_HOST")
    dbname = getenv("DB_NAME")
    user = getenv("DB_USER")
    password = getenv("DB_PASSWORD")
    port = getenv("DB_PORT", "5432")

    missing = [k for k, v in [("DB_HOST", host), ("DB_NAME", dbname), ("DB_USER", user), ("DB_PASSWORD", password)] if not v]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    conn.autocommit = False
    return conn


# Map CSV row to DB columns with defaults

def _parse_priority_list(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def map_row(row: Dict[str, str], defaults: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name = (row.get("name") or "").strip()
    if not name:
        return None
    name = name[:50]  # enforce varchar(50)

    # Collect raw fields for composite strings
    raw_address = (row.get("address") or "").strip()
    formatted_address = (row.get("formatted_address") or "").strip()
    plus_code = (row.get("plus_code") or row.get("pluscode") or "").strip()
    area = (row.get("area") or "").strip()
    city = (row.get("city") or "").strip()

    # Description: address + plus_code + area + city + formatted_address
    desc_parts = [raw_address, plus_code, area, city, formatted_address]
    description = " + ".join([p for p in desc_parts if p]) or name

    # lat/lon
    lat = None
    lon = None
    try:
        lat_str = row.get("lat")
        lon_str = row.get("lon")
        lat = float(lat_str) if lat_str not in (None, "") else None
        lon = float(lon_str) if lon_str not in (None, "") else None
    except ValueError:
        lat = None
        lon = None

    # location must be NOT NULL and in format [[lat,lon]] when available
    location = None
    if lat is not None and lon is not None:
        location = f"[[{lat},{lon}]]"
    else:
        # Fallback: composite of address parts; if still empty, use name
        fallback_loc = " + ".join([p for p in [raw_address, plus_code, area, city, formatted_address] if p])
        location = fallback_loc or name

    # mark_type static "Point"; use_type from location_type
    mark_type = "Point"
    use_type_src = (row.get("location_type") or defaults.get("use_type") or "")
    use_type = str(use_type_src)[:20] if use_type_src else None

    # status fields (static)
    status = "Active"
    status2 = "Active"

    # Timestamp now UTC
    now = datetime.now(timezone.utc)

    # Optional fields
    pluscode = plus_code or None
    area = area or None
    city = city or None
    state = (row.get("state") or "").strip() or None
    pincode = (row.get("pincode") or "").strip() or None
    phone = (row.get("phone") or "").strip() or None
    website = (row.get("website") or "").strip() or None

    # Build composite address as requested
    address_composite = " + ".join([p for p in [raw_address, plus_code, area, city, formatted_address] if p]) or None

    return {
        "status2": status2,
        "status": status,
        "mark_type": mark_type,
        "use_type": use_type,
        "location": location,
        "radius": defaults.get("radius_default"),
        "name": name,
        "description": description,
        "created": now,
        "updated": now,
        "created_by_id": 1,
        "updated_by_id": int(defaults.get("updated_by_id")),
        "speed_limit": defaults.get("speed_limit_override"),  # If None, inserts NULL
        "alert_type": (defaults.get("alert_type_override") or "none"),  # Ensure NOT NULL
        "lat": lat,
        "lon": lon,
        "address": address_composite,
        "pluscode": pluscode,
        "area": area,
        "city": city,
        "state": state,
        "pincode": pincode,
        "phone": phone,
        "website": website,
    }


def read_csv_rows(csv_path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = [r for r in reader]
    return headers, rows


def _validate_table_identifier(table: str) -> str:
    # Allow only letters, digits, underscore, and dot for schema.table
    import re
    if not re.match(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$", table):
        raise ValueError(f"Invalid table identifier: {table}")
    return table


def build_insert_sql(on_conflict: str, table: str) -> str:
    cols = [
        "status2", "status", "mark_type", "use_type", "location", "radius", "name", "description",
        "created", "updated", "created_by_id", "updated_by_id", "speed_limit", "alert_type",
        "lat", "lon", "address", "pluscode", "area", "city", "state", "pincode", "phone", "website"
    ]
    table = _validate_table_identifier(table)
    base = (
        f"INSERT INTO {table} (" + ", ".join(cols) + ") VALUES %s"
    )
    if on_conflict == "ignore":
        return base + " ON CONFLICT (name) DO NOTHING"
    else:
        # Conservative update: refresh commonly changing fields; keep existing when new is NULL
        return base + (
            " ON CONFLICT (name) DO UPDATE SET "
            "updated = EXCLUDED.updated, "
            "updated_by_id = EXCLUDED.updated_by_id, "
            "mark_type = COALESCE(EXCLUDED.mark_type, skytron_api_pointofinterests.mark_type), "
            "use_type = COALESCE(EXCLUDED.use_type, skytron_api_pointofinterests.use_type), "
            "location = COALESCE(EXCLUDED.location, skytron_api_pointofinterests.location), "
            "lat = COALESCE(EXCLUDED.lat, skytron_api_pointofinterests.lat), "
            "lon = COALESCE(EXCLUDED.lon, skytron_api_pointofinterests.lon), "
            "address = COALESCE(EXCLUDED.address, skytron_api_pointofinterests.address), "
            "pluscode = COALESCE(EXCLUDED.pluscode, skytron_api_pointofinterests.pluscode), "
            "area = COALESCE(EXCLUDED.area, skytron_api_pointofinterests.area), "
            "city = COALESCE(EXCLUDED.city, skytron_api_pointofinterests.city), "
            "state = COALESCE(EXCLUDED.state, skytron_api_pointofinterests.state), "
            "pincode = COALESCE(EXCLUDED.pincode, skytron_api_pointofinterests.pincode), "
            "phone = COALESCE(EXCLUDED.phone, skytron_api_pointofinterests.phone), "
            "website = COALESCE(EXCLUDED.website, skytron_api_pointofinterests.website)"
        )


def rows_to_values(mapped_rows: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    values: List[Tuple[Any, ...]] = []
    for r in mapped_rows:
        values.append((
            r["status2"], r["status"], r["mark_type"], r["use_type"], r["location"], r["radius"], r["name"], r["description"],
            r["created"], r["updated"], r["created_by_id"], r["updated_by_id"], r["speed_limit"], r["alert_type"],
            r["lat"], r["lon"], r["address"], r["pluscode"], r["area"], r["city"], r["state"], r["pincode"], r["phone"], r["website"]
        ))
    return values


def main():
    args = parse_args()

    defaults = {
        "created_by_id": args.created_by_id,
        "updated_by_id": args.updated_by_id,
        "status": args.status,
        "status2": args.status2,
        "mark_type": args.mark_type,
        "use_type": args.use_type,
        "desc_sources": _parse_priority_list(args.desc_source),
        "location_sources": _parse_priority_list(args.location_source),
        "radius_default": args.radius_default,
        "speed_limit_override": args.speed_limit,
        "alert_type_override": args.alert_type,
    }

    headers, csv_rows = read_csv_rows(args.csv_path)
    if args.show_mapping:
        show_mapping(headers)
        return
    print(f"Loaded {len(csv_rows)} rows from {args.csv_path}. Headers: {headers}")

    mapped: List[Dict[str, Any]] = []
    skipped = 0
    for row in csv_rows:
        m = map_row(row, defaults)
        if m is None:
            skipped += 1
            continue
        mapped.append(m)

    print(f"Mapped {len(mapped)} rows. Skipped {skipped} rows due to missing name.")

    if args.dry_run:
        preview = mapped[:5]
        for i, r in enumerate(preview, start=1):
            print(f"Preview row {i}: {r}")
        print("Dry-run mode: no database changes were made.")
        return

    conn = connect_db()
    try:
        with conn.cursor() as cur:
            sql = build_insert_sql(args.on_conflict, args.table)
            batch_size = max(1, args.batch_size)
            total = 0
            for i in range(0, len(mapped), batch_size):
                batch = mapped[i:i+batch_size]
                values = rows_to_values(batch)
                execute_values(cur, sql, values, page_size=batch_size)
                total += len(batch)
                print(f"Inserted/upserted {total} rows...")
        if args.rollback:
            conn.rollback()
            print(f"Transaction rolled back. 0 rows committed (simulated {len(mapped)} inserts).")
        else:
            conn.commit()
            print(f"Done. Inserted/upserted {len(mapped)} rows in total.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
