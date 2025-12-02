import os
import csv
import psycopg
from dotenv import load_dotenv

# Load DATABASE_URL from .env
load_dotenv()
conn_string = os.getenv("DATABASE_URL")
if not conn_string:
    raise RuntimeError("Please set DATABASE_URL in .env")

GTFS_FOLDER = "/Users/iqrakhan/Documents/Product Studio/product-studio/gtfs_subway"

GTFS_FILES = [
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
    "shapes.txt",
    "stop_times.txt",
    "stops.txt",
    "transfers.txt",
    "trips.txt",
]


def get_header_columns(file_path):
    """Read the first row to get column names."""
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header


def load_gtfs_file_fast(filename, conn, truncate=True):
    table_name = filename.replace(".txt", "")
    file_path = os.path.join(GTFS_FOLDER, filename)

    print(f"\n=== Loading {filename} into table '{table_name}' ===")

    # Get column names from header
    columns = get_header_columns(file_path)
    columns_sql = ", ".join(columns)

    copy_sql = (
        f"COPY {table_name} ({columns_sql}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    with conn.cursor() as cur:
        # Optional: clear table first so you don't get duplicates
        if truncate:
            cur.execute(f"TRUNCATE TABLE {table_name};")

        # Stream the file into COPY in chunks
        with open(file_path, "rb") as f:
            with cur.copy(copy_sql) as copy:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    if not chunk:
                        break
                    copy.write(chunk)

    print(f"✓ Finished loading {filename}")


def main():
    with psycopg.connect(conn_string) as conn:
        for filename in GTFS_FILES:
            load_gtfs_file_fast(filename, conn, truncate=True)

        conn.commit()
        print("\n✅ ALL GTFS FILES LOADED SUCCESSFULLY")


if __name__ == "__main__":
    main()
