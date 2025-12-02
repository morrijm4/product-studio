#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv(".env")

import os
import psycopg2
import requests
import hashlib
import gzip

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


def fetch_feed(group_key: str) -> bytes:
    """Fetch raw GTFS-realtime protobuf bytes for the given route group."""
    if group_key == "number":
        url = BASE_URL  # no suffix for 1/2/3/4/5/6/7/S
    else:
        url = f"{BASE_URL}-{group_key}"

    print(f"[{url}] requested")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.content  # raw protobuf bytes


def insert_raw_blob(conn, blob: bytes, route_group: str):
    """
    Compress the protobuf bytes, hash the compressed data,
    and store the compressed blob in the 'raw' table.
    """
    # 1) Compress (gzip)
    compressed = gzip.compress(blob)

    # 2) Hash the compressed data
    data_hash = hashlib.md5(compressed).hexdigest()

    # 3) Insert compressed blob into BYTEA column `data`
    sql = """
        INSERT INTO raw (data, data_hash, route_group)
        VALUES (%s, %s, %s)
        ON CONFLICT (data_hash) DO NOTHING;
    """

    with conn.cursor() as cur:
        # psycopg2.Binary tells psycopg2 this is binary (BYTEA) data
        cur.execute(sql, (psycopg2.Binary(compressed), data_hash, route_group))
    conn.commit()

    print(
        f"[{route_group}] compressed={len(compressed)} bytes, "
        f"hash={data_hash}, inserted (or skipped duplicate)"
    )


def main():
    conn = get_connection()
    try:
        for group_key in ROUTE_GROUPS.keys():
            print(f"Fetching group: {group_key}")
            raw_bytes = fetch_feed(group_key)
            insert_raw_blob(conn, raw_bytes, group_key)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
