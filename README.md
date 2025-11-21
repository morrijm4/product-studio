# Product Studio Project

This repository is to experiment with requesting MTA real-time subway data
through the GTFS real-time protocol and serving it through a HTTP server.

## Installation

```sh
pip install flask requests gtfs-realtime-bindings
```

## Run the server

```sh
python3 server.py
```

## Make a request

To get the arrival times for a given route, you can make a request to
`/route/<route_id>/arrivals` and can filter out stops with the `?stop_id=` query
parameter. You can find stop\_ids in the
[gtfs_subway/stops.txt](./gtfs_subway/stops.txt) file.

For example, if you want to find the arrival times for Roosevelt Island use the
following URL. You can put the URL in your browser to make a request

```url
http://localhost:8080/route/F/arrivals?stop_id=B06S&stop_id=B06N
```

Stop ID B06N and B06S are the IDs for Roosevelt Island Queens bound and
Manhattan bound respectively.

## What is the `gtfs_subway/` folder?

This is static information about the NYC subway system that can be joined with
data from the real-time feed to extract meaningful information. This data was
download from here: https://www.mta.info/developers.

