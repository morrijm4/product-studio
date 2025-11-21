from google.transit import gtfs_realtime_pb2
from flask import Flask, request, abort
import requests
import datetime


app = Flask(__name__)


def epoch_to_time(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def build_feed_url(route_id):
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

    match route_id:
        case "A" | "C" | "E":
            return url + "-ace"
        case "B" | "D" | "F" | "M":
            return url + "-bdfm"
        case "G":
            return url + "-g"
        case "J" | "Z":
            return url + "-jz"
        case "N" | "Q" | "R" | "W":
            return url + "-nqrw"
        case "L":
            return url + "-l"
        case "1" | "2" | "3" | "4" | "5" | "6" | "7" | "S":
            return url
        case _:
            return None


def get_feed(url):
    feed = gtfs_realtime_pb2.FeedMessage()

    response = requests.get(url)
    response.raise_for_status()

    feed.ParseFromString(response.content)
    return feed


@app.route("/route/<route_id>/feed")
def feed(route_id):
    url = build_feed_url(route_id)

    if url is None:
        abort(400, description="Unsupported route_id")

    return str(get_feed(url))


@app.route("/route/<route_id>/arrivals")
def arrivals(route_id):
    url = build_feed_url(route_id)

    if url is None:
        abort(400, description="Unsupported route_id")

    feed = get_feed(url)
    stop_ids = request.args.getlist("stop_id")

    arrivals = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip = entity.trip_update.trip

        # Filter out route ID
        if trip.route_id != route_id:
            continue

        for stu in entity.trip_update.stop_time_update:
            if not stu.HasField("stop_id"):
                continue

            # Filter our stop IDs
            if len(stop_ids) != 0 and stu.stop_id not in stop_ids:
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

    return arrivals


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
