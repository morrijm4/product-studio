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



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
