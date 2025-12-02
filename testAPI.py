#!/usr/bin/env python3

import requests
import json

BASE_URL = "http://localhost:8080"  # change if you used a different port


def pretty(obj):
    print(json.dumps(obj, indent=2, default=str))


def test_route_feed(route_id="F"):
    print(f"\n=== TEST: /route/{route_id}/feed ===")
    url = f"{BASE_URL}/route/{route_id}/feed"
    r = requests.get(url)
    print("Status:", r.status_code)
    if r.ok:
        print("Body (first 500 chars):")
        print(r.text[:500] + ("..." if len(r.text) > 500 else ""))
    else:
        print("Error body:", r.text)


def test_route_arrivals(route_id="F", stop_ids=None):
    print(f"\n=== TEST: /route/{route_id}/arrivals ===")
    params = []
    if stop_ids:
        for sid in stop_ids:
            params.append(("stop_id", sid))

    url = f"{BASE_URL}/route/{route_id}/arrivals"
    r = requests.get(url, params=params)
    print("Status:", r.status_code)
    if r.ok:
        data = r.json()
        print(f"Got {len(data)} arrivals (showing up to 5):")
        pretty(data[:5])
    else:
        print("Error body:", r.text)


def test_db_list_raw():
    print("\n=== TEST: GET /db/raw ===")
    url = f"{BASE_URL}/db/raw"
    r = requests.get(url)
    print("Status:", r.status_code)
    if not r.ok:
        print("Error body:", r.text)
        return []

    rows = r.json()
    print(f"Got {len(rows)} rows (showing up to 5):")
    pretty(rows[:5])
    return rows


def test_db_get_raw(row_id, include_data=False):
    print(f"\n=== TEST: GET /db/raw/{row_id}?include_data={include_data} ===")
    params = {"include_data": str(include_data).lower()}
    url = f"{BASE_URL}/db/raw/{row_id}"
    r = requests.get(url, params=params)
    print("Status:", r.status_code)
    if r.ok:
        data = r.json()
        # Hide the giant b64 string if present
        if "data_b64" in data:
            data_copy = dict(data)
            data_copy["data_b64"] = f"<base64 string length={len(data['data_b64'])}>"
            pretty(data_copy)
        else:
            pretty(data)
    else:
        print("Error body:", r.text)


def test_db_blob(row_id):
    print(f"\n=== TEST: GET /db/raw/{row_id}/compressed ===")
    url = f"{BASE_URL}/db/raw/{row_id}/compressed"
    r = requests.get(url)
    print("Status:", r.status_code)
    if r.ok:
        print("Compressed blob length:", len(r.content), "bytes")
        print("Content-Type:", r.headers.get("Content-Type"))
    else:
        print("Error body:", r.text)


def test_db_protobuf(row_id):
    print(f"\n=== TEST: GET /db/raw/{row_id}/protobuf ===")
    url = f"{BASE_URL}/db/raw/{row_id}/protobuf"
    r = requests.get(url)
    print("Status:", r.status_code)
    if r.ok:
        print("Decompressed protobuf length:", len(r.content), "bytes")
        print("Content-Type:", r.headers.get("Content-Type"))
    else:
        print("Error body:", r.text)


def test_db_arrivals(row_id, route_id=None, stop_ids=None):
    print(f"\n=== TEST: GET /db/raw/{row_id}/arrivals ===")
    params = {}
    if route_id:
        params["route_id"] = route_id
    if stop_ids:
        for sid in stop_ids:
            # allow multiple stop_id params
            params.setdefault("stop_id", [])
            params["stop_id"].append(sid)

    url = f"{BASE_URL}/db/raw/{row_id}/arrivals"
    r = requests.get(url, params=params)
    print("Status:", r.status_code)
    if r.ok:
        data = r.json()
        print(f"Got {len(data)} arrivals (showing up to 5):")
        pretty(data[:5])
    else:
        print("Error body:", r.text)


def main():
    # 1. Test live GTFS endpoints
    test_route_feed("F")
    test_route_arrivals("F")  # optionally: stop_ids=["D40N"]

    # 2. Test database-backed endpoints
    rows = test_db_list_raw()
    if not rows:
        print("\nNo rows in /db/raw – run your ingest script first.")
        return

    # Use the most recent row
    row_id = rows[0]["id"]
    print(f"\nUsing row_id={row_id} for detailed tests")

    test_db_get_raw(row_id, include_data=False)
    test_db_get_raw(row_id, include_data=True)
    test_db_blob(row_id)
    test_db_protobuf(row_id)
    test_db_arrivals(row_id)                # all arrivals
    test_db_arrivals(row_id, route_id="F")  # filtered by route


if __name__ == "__main__":
    main()
