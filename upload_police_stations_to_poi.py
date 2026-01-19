import argparse
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(name)
    return val if val is not None else default


def _try_load_dotenv() -> None:
    # Optional: if python-dotenv is available, load .env automatically.
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        return


def _canonical_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _get(row: Dict[str, str], *candidate_headers: str) -> str:
    # Candidate headers are matched case-insensitively with whitespace normalization.
    if not row:
        return ""

    canonical_map = {_canonical_header(k): k for k in row.keys()}
    for cand in candidate_headers:
        key = canonical_map.get(_canonical_header(cand))
        if key is not None:
            return (row.get(key) or "").strip()
    return ""


def _parse_float(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _validate_table_identifier(table: str) -> str:
    # Allow only letters, digits, underscore, and dot for schema.table
    if not re.match(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$", table or ""):
        raise ValueError(f"Invalid table identifier: {table}")
    return table


def _build_location(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    # Keep same format as existing POI importer: "[[lat,lon]]"
    if lat is None or lon is None:
        return None
    return f"[[{lat},{lon}]]"


def connect_db() -> psycopg2.extensions.connection:
    # Prefer DATABASE_URL if present.
    db_url = getenv("DATABASE_URL") or getenv("DB_URL")
    if db_url:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn

    host = getenv("DB_HOST")
    dbname = getenv("DB_NAME")
    user = getenv("DB_USER")
    password = getenv("DB_PASSWORD")
    port = getenv("DB_PORT", "5432")

    missing = [
        k
        for k, v in [
            ("DB_HOST", host),
            ("DB_NAME", dbname),
            ("DB_USER", user),
            ("DB_PASSWORD", password),
        ]
        if not v
    ]
    if missing:
        raise RuntimeError(
            "Missing required environment variables. Set DATABASE_URL (recommended) "
            f"or set: {', '.join(missing)}"
        )

    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    conn.autocommit = False
    return conn


@dataclass
class Defaults:
    created_by_id: int = 1
    updated_by_id: int = 1
    radius: int = 100
    status: str = "Active"
    status2: str = "Active"
    mark_type: str = "Point"
    use_type: str = "police"
    alert_type: str = "none"
    speed_limit: int = 100


def map_row(row: Dict[str, str], defaults: Defaults) -> Optional[Dict[str, Any]]:
    name = _get(row, "Name")
    if not name:
        return None
    name = name[:50]  # varchar(50)

    district = _get(row, "District")
    state = _get(row, "State")
    address = _get(row, "Address")
    phone = _get(row, "Contact Number")

    lat = _parse_float(_get(row, "Location:latitude", "Location:latitude ", "Location:latitude "))
    lon = _parse_float(_get(row, "Location: longitude", "Location: longitude ", "Location: longitude"))

    location = _build_location(lat, lon) or (address or name)

    now = datetime.now(timezone.utc)

    return {
        "status2": defaults.status2,
        "status": defaults.status,
        "mark_type": defaults.mark_type,
        "use_type": defaults.use_type[:20] if defaults.use_type else None,
        "location": location,
        "radius": defaults.radius,
        "name": name,
        "description": address or name,
        "created": now,
        "updated": now,
        "created_by_id": defaults.created_by_id,
        "updated_by_id": defaults.updated_by_id,
        "speed_limit": defaults.speed_limit,
        "alert_type": defaults.alert_type,
        "lat": lat,
        "lon": lon,
        "address": address or None,
        "pluscode": "",
        "area": district or None,
        "city": district or None,
        "state": state or None,
        "pincode": "",
        "phone": phone or None,
        "website": "",
    }


def read_csv_rows(csv_path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    return read_csv_rows_with_encoding(csv_path, encoding="utf-8-sig")


def read_csv_rows_with_encoding(csv_path: str, encoding: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """Read CSV rows with a requested encoding and common fallbacks.

    Many CSVs saved from Excel on Windows are not UTF-8; they are often cp1252.
    """

    def _read(enc: str) -> Tuple[List[str], List[Dict[str, str]]]:
        with open(csv_path, "r", encoding=enc, newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            rows = [r for r in reader]
        return headers, rows

    # Try the user-provided encoding first, then common fallbacks.
    tried: List[str] = []
    for enc in [encoding, "utf-8-sig", "cp1252", "latin-1", "utf-16"]:
        if enc in tried:
            continue
        tried.append(enc)
        try:
            return _read(enc)
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        f"Failed to decode CSV. Tried encodings: {', '.join(tried)}. "
        "Re-run with --encoding cp1252 (common for Excel on Windows) or save the CSV as UTF-8.",
    )


def build_insert_sql(on_conflict: str, table: str) -> Tuple[str, List[str]]:
    cols = [
        "status2",
        "status",
        "mark_type",
        "use_type",
        "location",
        "radius",
        "name",
        "description",
        "created",
        "updated",
        "created_by_id",
        "updated_by_id",
        "speed_limit",
        "alert_type",
        "lat",
        "lon",
        "address",
        "pluscode",
        "area",
        "city",
        "state",
        "pincode",
        "phone",
        "website",
    ]

    table = _validate_table_identifier(table)
    base = f"INSERT INTO {table} (" + ", ".join(cols) + ") VALUES %s"

    if on_conflict == "ignore":
        return base + " ON CONFLICT (name) DO NOTHING", cols

    # Update common fields.
    return (
        base
        + (
            " ON CONFLICT (name) DO UPDATE SET "
            "updated = EXCLUDED.updated, "
            "updated_by_id = EXCLUDED.updated_by_id, "
            "status2 = EXCLUDED.status2, "
            "status = EXCLUDED.status, "
            "mark_type = EXCLUDED.mark_type, "
            "use_type = EXCLUDED.use_type, "
            "location = EXCLUDED.location, "
            "radius = EXCLUDED.radius, "
            "description = EXCLUDED.description, "
            "alert_type = EXCLUDED.alert_type, "
            "speed_limit = EXCLUDED.speed_limit, "
            "lat = EXCLUDED.lat, "
            "lon = EXCLUDED.lon, "
            "address = EXCLUDED.address, "
            "pluscode = EXCLUDED.pluscode, "
            "area = EXCLUDED.area, "
            "city = EXCLUDED.city, "
            "state = EXCLUDED.state, "
            "pincode = EXCLUDED.pincode, "
            "phone = EXCLUDED.phone, "
            "website = EXCLUDED.website"
        )
    ), cols


def rows_to_values(mapped_rows: List[Dict[str, Any]], cols: List[str]) -> List[Tuple[Any, ...]]:
    values: List[Tuple[Any, ...]] = []
    for r in mapped_rows:
        values.append(tuple(r.get(c) for c in cols))
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload Police Station POIs from CSV into PostgreSQL table public.skytron_api_pointofinterests. "
            "Standalone script; does not touch Django code."
        )
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default="Copy of Assam_Police_Stations_Template.csv",
        help="Path to police stations CSV",
    )
    parser.add_argument(
        "--encoding",
        default=getenv("CSV_ENCODING", "utf-8-sig"),
        help=(
            "CSV text encoding. If you see UnicodeDecodeError, try cp1252 (Excel on Windows) "
            "or utf-16. You can also set CSV_ENCODING env var."
        ),
    )
    parser.add_argument("--table", default="public.skytron_api_pointofinterests", help="Target table")
    parser.add_argument("--batch-size", type=int, default=1000, help="Insert batch size")
    parser.add_argument("--dry-run", action="store_true", help="Preview first rows without inserting")
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Execute inserts but ROLLBACK at end (no writes committed)",
    )
    parser.add_argument(
        "--on-conflict",
        choices=["ignore", "update"],
        default="ignore",
        help="How to handle duplicate name (unique constraint on name)",
    )

    # Defaults / constants (you asked to force these values)
    parser.add_argument("--created-by-id", type=int, default=int(getenv("DEFAULT_CREATED_BY_ID", "1")))
    parser.add_argument("--updated-by-id", type=int, default=int(getenv("DEFAULT_UPDATED_BY_ID", "1")))
    parser.add_argument("--radius", type=int, default=int(getenv("DEFAULT_RADIUS", "100")))
    parser.add_argument("--use-type", default=getenv("DEFAULT_USE_TYPE", "police"))
    return parser.parse_args()


def main() -> None:
    _try_load_dotenv()
    args = parse_args()

    defaults = Defaults(
        created_by_id=args.created_by_id,
        updated_by_id=args.updated_by_id,
        radius=args.radius,
        use_type=args.use_type,
    )

    headers, csv_rows = read_csv_rows_with_encoding(args.csv_path, args.encoding)
    print(f"Loaded {len(csv_rows)} rows from {args.csv_path}.")
    print(f"CSV headers: {headers}")

    mapped: List[Dict[str, Any]] = []
    skipped = 0
    for row in csv_rows:
        m = map_row(row, defaults)
        if m is None:
            skipped += 1
            continue
        mapped.append(m)

    print(f"Mapped {len(mapped)} rows. Skipped {skipped} rows (missing Name).")

    if args.dry_run:
        for i, r in enumerate(mapped[:5], start=1):
            print(f"Preview row {i}: {r}")
        print("Dry-run mode: no database changes were made.")
        return

    conn = connect_db()
    try:
        sql, cols = build_insert_sql(args.on_conflict, args.table)
        batch_size = max(1, args.batch_size)

        with conn.cursor() as cur:
            total = 0
            for i in range(0, len(mapped), batch_size):
                batch = mapped[i : i + batch_size]
                values = rows_to_values(batch, cols)
                execute_values(cur, sql, values, page_size=batch_size)
                total += len(batch)
                print(f"Inserted/upserted {total} rows...")

        if args.rollback:
            conn.rollback()
            print(f"Transaction rolled back. 0 rows committed (simulated {len(mapped)} inserts).")
        else:
            conn.commit()
            print(f"Done. Inserted/upserted {len(mapped)} rows in total.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
