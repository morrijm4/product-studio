#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv(".env")
import os
import psycopg2
import requests 
import hashlib


DATABASE_URL = os.environ.get("NEON_DATABASE_URL")
ROUTE_GROUPS = {
    "ace": ["A", "C", "E"],
    "bdfm": ["B", "D", "F", "M"],
    "g": ["G"],
    "jz": ["J", "Z"],
    "nqrw": ["N", "Q", "R", "W"],
    "l": ["L"],
    "number": ["1", "2", "3", "4", "5", "6", "7", "S"],
}
BASE_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError(
            "NEON_DATABASE_URL env var is not set. "
            "Example: postgresql://user:password@host/dbname?sslmode=require"
        )
    return psycopg2.connect(DATABASE_URL)


def fetch_feed(group_key: str) -> str:
    if group_key == "number":
        url = BASE_URL              # no suffix for 1/2/3/4/5/6/7/S
    else:
        url = f"{BASE_URL}-{group_key}"

    resp = requests.get(url, timeout=10)
    print(f"[{url}] requested")
    resp.raise_for_status()
    return resp.content


def insert_raw_blob(conn, text: str, route_group: str):
    data_hash = hashlib.md5(text).hexdigest()

    sql = """
        INSERT INTO raw (data, data_hash, route_group)
        VALUES (%s, %s, %s)
        ON CONFLICT (data_hash) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (text, data_hash, route_group))
    conn.commit()
    print(f"[{route_group}] inserted (or skipped duplicate)")


def main():
    conn = get_connection()

    try:
        for group_key in ROUTE_GROUPS.keys():
            print(f"Fetching group: {group_key}")
            raw_text = fetch_feed(group_key)
            insert_raw_blob(conn, raw_text, group_key)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
