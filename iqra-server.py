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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
