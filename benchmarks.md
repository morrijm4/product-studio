# Benchmarks

Without caching MTA real-time request

```sh
$ wrk -t4 -c50 -d30s http://localhost:8080/route/F/arrivals\?stop_id\=B06S\&stop_id\=B06N
Running 30s test @ http://localhost:8080/route/F/arrivals?stop_id=B06S&stop_id=B06N
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.51s   484.68ms   2.00s    80.39%
    Req/Sec    17.87     17.88    80.00     82.40%
  896 requests in 30.08s, 4.06MB read
  Socket errors: connect 0, read 0, write 0, timeout 70
Requests/sec:     29.79
Transfer/sec:    138.34KB
```

With caching MTA real-time request

```sh
$ wrk -t4 -c50 -d30s http://localhost:8080/route/F/arrivals\?stop_id\=B06S\&stop_id\=B06N
Running 30s test @ http://localhost:8080/route/F/arrivals?stop_id=B06S&stop_id=B06N
  4 threads and 50 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   211.14ms  326.88ms   1.99s    91.45%
    Req/Sec   103.67     22.38   191.00     89.70%
  10515 requests in 30.10s, 45.69MB read
  Socket errors: connect 0, read 0, write 0, timeout 38
Requests/sec:    349.30
Transfer/sec:      1.52MB
```
