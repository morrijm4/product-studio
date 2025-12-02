from dotenv import load_dotenv
from flask import Flask, request, abort, jsonify
import time
import requests
import os
import psycopg2
import psycopg2.extras
import hashlib
import gzip

load_dotenv(".env")

DATABASE_URL = os.environ.get("NEON_DATABASE_URL")

app = Flask(__name__)


def get_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "NEON_DATABASE_URL env var is not set. "
            "Example: postgresql://user:password@host/dbname?sslmode=require"
        )
    return psycopg2.connect(DATABASE_URL)

@app.route("/db/agency/<agency_id>")
def agency(agency_id):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                agency_id,
                agency_name,
                agency_url,
                agency_timezone,
                agency_lang,
                agency_phone
            FROM agency
            WHERE agency_id = %s
            """,
            (agency_id,),
        )
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/db/calendar_dates/<service_id>")
def calendar_dates(service_id):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                service_id,
                date,
                exception_type
            FROM calendar_dates
            WHERE service_id = %s
            """,
            (service_id,),
        )
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route("/db/calendar/<service_id>")
def calendar(service_id):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                service_id,
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday,
                start_date,
                end_date
            FROM calendar
            WHERE service_id = %s
            """,
            (service_id,),
        )
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/db/stops/<stop_id>")
def stops(stop_id):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                location_type,
                parent_station
            FROM stops
            WHERE stop_id = %s
            """,
            (stop_id,),
        )
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route("/db/routes/<route_id>")
def routes(route_id):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                route_id,
                agency_id,
                route_short_name,
                route_long_name,
                route_desc,
                route_type,
                route_url,
                route_color,
                route_text_color,
                route_sort_order
            FROM routes
            WHERE route_id = %s
            """,
            (route_id,),
        )
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route("/db/shapes/<shape_id>")
def shapes(shape_id):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                shape_id,
                shape_pt_sequence,
                shape_pt_lat,
                shape_pt_lon
            FROM shapes
            WHERE shape_id = %s
            """,
            (shape_id,),
        )
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()


@app.route("/db/stop_times/<trip_id>")
def stop_times(trip_id):
    # Use these as the time window bounds
    start_time = request.args.get("arrival_time")
    end_time = request.args.get("departure_time")

    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Base query: all stops for this trip
        query = """
            SELECT
                trip_id,
                stop_id,
                arrival_time,
                departure_time,
                stop_sequence
            FROM stop_times
            WHERE trip_id = %s
        """
        params = [trip_id]

        # If both bounds are provided, filter by arrival_time window
        if start_time and end_time:
            query += """
                AND arrival_time >= %s
                AND arrival_time <= %s
            """
            params.extend([start_time, end_time])

        cur.execute(query, params)
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route("/db/transfers")
def transfers():
    from_stop_id = request.args.get("from_stop_id")
    to_stop_id = request.args.get("to_stop_id")

    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Base query
        query = """
            SELECT
                from_stop_id,
                to_stop_id,
                transfer_type,
                min_transfer_time
            FROM transfers
            WHERE 1 = 1
        """
        params = []

        # Optional filters
        if from_stop_id:
            query += " AND from_stop_id = %s"
            params.append(from_stop_id)

        if to_stop_id:
            query += " AND to_stop_id = %s"
            params.append(to_stop_id)

        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

######################
# ISSUES with this part
#######################
@app.route("/db/trips/<trip_id>")
def trip_detail(trip_id):
    """
    GET /db/trips/<trip_id>
    Optional: ?include=route,calendar,calendar_dates,stop_times,shapes
    """
    include_param = request.args.get("include", "")
    include = {x.strip() for x in include_param.split(",") if x.strip()}

    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # 1) Get the trip itself
        cur.execute(
            """
            SELECT
                route_id,
                trip_id,
                service_id,
                trip_headsign,
                direction_id,
                shape_id
            FROM trips
            WHERE trip_id = %s
            """,
            (trip_id,),
        )
        trip = cur.fetchone()
        if not trip:
            return jsonify({"error": "trip not found"}), 404

        result = {"trip": trip}

        route_id = trip["route_id"]
        service_id = trip["service_id"]
        shape_id = trip["shape_id"]

        # 2) Route info (routes table) – uses route_id
        if not include or "route" in include:
            cur.execute(
                """
                SELECT *
                FROM routes
                WHERE route_id = %s
                """,
                (route_id,),
            )
            result["route"] = cur.fetchone()

        # 3) Service calendar (calendar table) – uses service_id
        if not include or "calendar" in include:
            cur.execute(
                """
                SELECT *
                FROM calendar
                WHERE service_id = %s
                """,
                (service_id,),
            )
            result["calendar"] = cur.fetchone()

        # 4) Service exceptions (calendar_dates table) – uses service_id
        if not include or "calendar_dates" in include:
            cur.execute(
                """
                SELECT *
                FROM calendar_dates
                WHERE service_id = %s
                ORDER BY date
                """,
                (service_id,),
            )
            result["calendar_dates"] = cur.fetchall()

        # 5) Stop times (stop_times table) – uses trip_id
        if not include or "stop_times" in include:
            cur.execute(
                """
                SELECT
                    trip_id,
                    stop_id,
                    arrival_time,
                    departure_time,
                    stop_sequence
                FROM stop_times
                WHERE trip_id = %s
                ORDER BY stop_sequence
                """,
                (trip_id,),
            )
            result["stop_times"] = cur.fetchall()

        # 6) Shape points (shapes table) – uses shape_id
        if not include or "shapes" in include:
            cur.execute(
                """
                SELECT *
                FROM shapes
                WHERE shape_id = %s
                ORDER BY shape_pt_sequence
                """,
                (shape_id,),
            )
            result["shapes"] = cur.fetchall()

        return jsonify(result)

    finally:
        conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
