#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv(".env")

import os
import time
import datetime
import threading
import psycopg2
import psycopg2.extras
import requests
import gzip
import hashlib

from flask import Flask, request, abort, jsonify, Response
from google.transit import gtfs_realtime_pb2 as gtfs

# ----------------------------
#  Flask App
# ----------------------------
app = Flask(__name__)

# ----------------------------
#  Database
# ----------------------------
DATABASE_URL = os.environ.get("NEON_DATABASE_URL")

def get_db():
    return psycopg2.connect(DATABASE_URL)

# ----------------------------
#  In-memory GTFS Cache
# ----------------------------
_GTFS_CACHE = {}
_GTFS_CACHE_LOCK = threading.Lock()
MIN_REFRESH_INTERVAL = 10  # seconds

# ----------------------------
#  Feed URL Builder
# ----------------------------
BASE_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

def build_feed_url(route_id):
    match route_id:
        case "A" | "C" | "E":
            return BASE_URL + "-ace"
        case "B" | "D" | "F" | "M":
            return BASE_URL + "-bdfm"
        case "G":
            return BASE_URL + "-g"
        case "J" | "Z":
            return BASE_URL + "-jz"
        case "N" | "Q" | "R" | "W":
            return BASE_URL + "-nqrw"
        case "L":
            return BASE_URL + "-l"
        case "1" | "2" | "3" | "4" | "5" | "6" | "7" | "S":
            return BASE_URL
        case _:
            return None

# ----------------------------
#  Fetch + Cache Live Feed
# ----------------------------
def get_live_feed(url):
    now = time.time()

    with _GTFS_CACHE_LOCK:
        if url in _GTFS_CACHE:
            last_ts = _GTFS_CACHE[url]["ts"]
            if now - last_ts < MIN_REFRESH_INTERVAL:
                return _GTFS_CACHE[url]["feed"]

    resp = requests.get(url)
    resp.raise_for_status()

    blob = resp.content
    feed = gtfs.FeedMessage()
    feed.ParseFromString(blob)

    with _GTFS_CACHE_LOCK:
        _GTFS_CACHE[url] = {"ts": now, "feed": feed}

    return feed

# ----------------------------
#  Helpers
# ----------------------------
def epoch_to_time(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")

# ----------------------------
#  EXISTING ENDPOINTS
# ----------------------------

@app.route("/route/<route_id>/feed")
def route_feed(route_id):
    url = build_feed_url(route_id)
    if not url:
        abort(400, "Invalid route_id")
    feed = get_live_feed(url)
    return str(feed)


@app.route("/route/<route_id>/arrivals")
def route_arrivals(route_id):
    url = build_feed_url(route_id)
    if not url:
        abort(400, "Invalid route_id")

    feed = get_live_feed(url)
    stop_filter = request.args.getlist("stop_id")

    arrivals = []
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue

        trip = ent.trip_update.trip
        if trip.route_id != route_id:
            continue

        for stu in ent.trip_update.stop_time_update:
            if not stu.HasField("stop_id"):
                continue
            if stop_filter and stu.stop_id not in stop_filter:
                continue
            if stu.HasField("arrival"):
                arrivals.append(
                    {
                        "trip_id": trip.trip_id,
                        "stop_id": stu.stop_id,
                        "arrival_epoch": stu.arrival.time,
                        "arrival_time": epoch_to_time(stu.arrival.time),
                    }
                )

    arrivals.sort(key=lambda x: (x["stop_id"], x["arrival_epoch"]))
    return jsonify(arrivals)

# ----------------------------
#  NEW ENDPOINTS: DATABASE ACCESS
# ----------------------------

@app.route("/db/raw", methods=["GET"])
def db_list_raw():
    """
    List all entries in the raw table (meta only).
    Returns: id, route_group, created_at, data_hash, size_bytes
    """
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                id,
                route_group,
                created_at,
                data_hash,
                octet_length(data) AS size_bytes
            FROM raw
            ORDER BY created_at DESC;
        """)
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/db/raw/<int:row_id>", methods=["GET"])
def db_get_raw(row_id):
    """
    Get a single row's metadata.
    Optional: ?include_data=true returns base64 of compressed data as `data_b64`.
    """
    include_data = request.args.get("include_data", "false").lower() == "true"

    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                id,
                route_group,
                created_at,
                data_hash,
                octet_length(data) AS size_bytes,
                data
            FROM raw
            WHERE id = %s;
        """, (row_id,))
        row = cur.fetchone()
        if not row:
            abort(404, description="Row not found")

        # Convert memoryview -> bytes
        blob = bytes(row["data"])
        del row["data"]

        if include_data:
            import base64
            row["data_b64"] = base64.b64encode(blob).decode("ascii")

        return jsonify(row)
    finally:
        conn.close()


@app.route("/db/raw/<int:row_id>/compressed", methods=["GET"])
def db_get_raw_compressed(row_id):
    """
    Return the compressed binary blob exactly as stored in BYTEA.
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT data FROM raw WHERE id = %s;", (row_id,))
        row = cur.fetchone()
        if not row:
            abort(404, description="Row not found")

        blob = bytes(row[0])  # memoryview -> bytes
        return Response(blob, mimetype="application/octet-stream")
    finally:
        conn.close()


@app.route("/db/raw/<int:row_id>/protobuf", methods=["GET"])
def db_get_raw_protobuf(row_id):
    """
    Return the decompressed protobuf bytes from a stored compressed blob.
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT data FROM raw WHERE id = %s;", (row_id,))
        row = cur.fetchone()
        if not row:
            abort(404, description="Row not found")

        compressed = bytes(row[0])
        decompressed = gzip.decompress(compressed)
        return Response(decompressed, mimetype="application/octet-stream")
    finally:
        conn.close()


@app.route("/db/raw/<int:row_id>/arrivals", methods=["GET"])
def db_get_raw_arrivals(row_id):
    """
    Parse a stored snapshot, return arrivals from that feed.
    Optional query params:
      - route_id=<route>
      - stop_id=<id> (can appear multiple times)
    """
    route_filter = request.args.get("route_id")
    stop_ids = request.args.getlist("stop_id")

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT data FROM raw WHERE id = %s;", (row_id,))
        row = cur.fetchone()
        if not row:
            abort(404, description="Row not found")

        compressed = bytes(row[0])
        decompressed = gzip.decompress(compressed)

        feed = gtfs.FeedMessage()
        feed.ParseFromString(decompressed)

        results = []

        for ent in feed.entity:
            if not ent.HasField("trip_update"):
                continue
            trip = ent.trip_update.trip

            if route_filter and trip.route_id != route_filter:
                continue

            for stu in ent.trip_update.stop_time_update:
                if not stu.HasField("stop_id"):
                    continue
                if stop_ids and stu.stop_id not in stop_ids:
                    continue
                if stu.HasField("arrival"):
                    results.append({
                        "trip_id": trip.trip_id,
                        "route_id": trip.route_id,
                        "stop_id": stu.stop_id,
                        "arrival_epoch": stu.arrival.time,
                        "arrival_time": epoch_to_time(stu.arrival.time),
                    })

        results.sort(key=lambda x: (x["stop_id"], x["arrival_epoch"]))
        return jsonify(results)
    finally:
        conn.close()

# ----------------------------
#  Run server
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
