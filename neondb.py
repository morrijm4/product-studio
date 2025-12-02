import os
import csv
import psycopg
from dotenv import load_dotenv

# Load DATABASE_URL from .env
load_dotenv()
conn_string = os.getenv("NEON_DATABASE_URL")
if not conn_string:
    raise RuntimeError("Please set DATABASE_URL in .env")

GTFS_FOLDER = "./gtfs_subway"


def load_gtfs_file(filename, conn):
    table_name = filename.replace(".txt", "")  # e.g. agency.txt → agency
    file_path = os.path.join(GTFS_FOLDER, filename)

    print(f"\nLoading {filename} into table '{table_name}' ...")

    # Read CSV
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        if not columns:
            print(f"Skipping {filename}, no columns found.")
            return

        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

        with conn.cursor() as cur:
            for row in reader:
                values = [row.get(col) for col in columns]
                try:
                    cur.execute(insert_sql, values)
                except Exception as e:
                    print(f"Error inserting row {row}: {e}")

        print(f"✓ Finished loading {filename}")


def main():
    # List of GTFS files to load
    gtfs_files = [
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

    with psycopg.connect(conn_string) as conn:
        for filename in gtfs_files:
            load_gtfs_file(filename, conn)

        conn.commit()
        print("\nALL GTFS FILES LOADED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
