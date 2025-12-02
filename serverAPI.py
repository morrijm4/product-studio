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
import base64

from flask import Flask, request, abort, jsonify, Response
from google.transit import gtfs_realtime_pb2 as gtfs

# ----------------------------
#  Flask App
# ----------------------------
app = Flask(__name__)

# ----------------------------
#  Database
# ----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")

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
    Query params:
      - route_group=<str> (can appear multiple times)
      - start_date=YYYY-MM-DD
      - start_time=HH:MM:SS
      - end_date=YYYY-MM-DD
      - end_time=HH:MM:SS
      - limit=<int> (default 50 if no filters; else 100)
      - offset=<int>

    If no params → return latest 50 rows.
    """

    # ----------- Read query parameters -----------
    route_groups = request.args.getlist("route_group")

    start_date = request.args.get("start_date")  # "2025-12-02"
    start_time = request.args.get("start_time")  # "04:49:16"

    end_date = request.args.get("end_date")
    end_time = request.args.get("end_time")

    limit_param = request.args.get("limit")
    offset_param = request.args.get("offset")

    # ----------- Detect no filters -----------
    no_filters = (
        not route_groups
        and not start_date and not start_time
        and not end_date and not end_time
        and not limit_param and not offset_param
    )

    if no_filters:
        limit = 50
        offset = 0
    else:
        limit = int(limit_param) if limit_param else 100
        offset = int(offset_param) if offset_param else 0

    # ----------- Build WHERE clause -----------
    where_clauses = []
    params: list = []

    # route_group filter
    if route_groups:
        where_clauses.append("route_group = ANY(%s)")
        params.append(route_groups)

    # ----------- Build timestamp filters -----------
    start_ts = None
    end_ts = None

    # today's date fallback if user only gives time
    today = datetime.datetime.utcnow().date()

    if start_date or start_time:
        sd = start_date if start_date else today.isoformat()
        st = start_time if start_time else "00:00:00"
        start_ts = f"{sd} {st}"

    if end_date or end_time:
        ed = end_date if end_date else today.isoformat()
        et = end_time if end_time else "23:59:59"
        end_ts = f"{ed} {et}"

    if start_ts and end_ts:
        where_clauses.append("created_at BETWEEN %s AND %s")
        params.extend([start_ts, end_ts])
    elif start_ts:
        where_clauses.append("created_at >= %s")
        params.append(start_ts)
    elif end_ts:
        where_clauses.append("created_at <= %s")
        params.append(end_ts)

    # ----------- SQL assembly -----------
    sql = """
        SELECT
            id,
            route_group,
            created_at,
            octet_length(data) AS size_bytes
        FROM raw
    """

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    sql += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    # ----------- Execute query -----------
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        # Reformat created_at to match Neon UI
        cleaned = []
        for r in rows:
            dt = r["created_at"]
            cleaned.append({
                "id": r["id"],
                "route_group": r["route_group"],
                "created_at": dt.strftime("%a, %d %b %Y %H:%M:%S GMT"),
                "size_bytes": r["size_bytes"],
            })

        return jsonify(cleaned)

    finally:
        conn.close()




@app.route("/db/raw/<int:row_id>/protobuf_raw", methods=["GET"])
def db_get_raw_protobuf(row_id):
    """
    Return the **decompressed** protobuf bytes from a stored compressed blob.

    Response:
      - Content-Type: application/octet-stream
      - Body: raw protobuf (FeedMessage serialized bytes)
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT data FROM raw WHERE id = %s;", (row_id,))
        row = cur.fetchone()
        if not row:
            abort(404, description="Row not found")

        compressed = bytes(row[0])        # BYTEA -> bytes
        decompressed = gzip.decompress(compressed)

        return Response(
            decompressed,
            mimetype="application/octet-stream"
        )
    finally:
        conn.close()


# ----------------------------
#  Run server
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
