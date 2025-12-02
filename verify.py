#!/usr/bin/env python3
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv
load_dotenv(".env")

import os
import psycopg2

DATABASE_URL = os.environ["NEON_DATABASE_URL"]


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def fetch_one():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, route_group, created_at, data
                FROM raw
                ORDER BY created_at DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

            if not row:
                print("No rows found.")
                return

            id_, route_group, created_at, blob_mv = row

            # Convert memoryview -> bytes
            blob = bytes(blob_mv)
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(blob)

            print(f"id: {id_}")
            print(f"route_group: {route_group}")
            print(f"created_at: {created_at}")
            print(f"blob type: {type(blob)}")
            print(f"blob length: {len(blob)} bytes")
            print(f"first 50 bytes: {blob[:50]}")
            print(f"data: {feed}")
    finally:
        conn.close()


if __name__ == "__main__":
    fetch_one()
