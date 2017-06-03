# statsq
StatsD library with the posibility of using dimensions (key/value tags)

Inspired by statsdaemon, this library allows `statsd` metrics to add dimensions.

## Motivation

When using dynamic containerized environments (like in Docker Services), the use of StatsD is similar to the limitations graphite brought to the table.

Only one (bucket-)name is possible, ignoring what modern metrics format offering by using tags. 

OpenTSDB (and others) allow the following.

```
put request 11 <timestamp> service=http1,host=web1
put request 12 <timestamp> service=http1,host=web2
put request 13 <timestamp> service=http1,host=web3
put request 11 <timestamp> service=http2,host=web1
```
The frontend can graph the `request` metric over all services and host, or grouped by either of them.

In Graphite this the dimensions have to be included into the metric-name. [Dieter Plaetinck]() proposed a graphite metric name like this for the above metrics:

```
service=http1.host=web1.requests 11 <timestamp>
service=http1.host=web2.requests 12 <timestamp>
service=http1.host=web3.requests 13 <timestamp>
service=http2.host=web1.requests 21 <timestamp>
```

The same problem comes with StatsD, the format for the metrics are along this lines.

```
service=http1.host=web1.requests:+11|c
service=http1.host=web2.requests:+12|c
...
```
Problem here is, that StatsD does not care about dimensions and will use distinct buckets.

## Proposal

StatsQ is going to introduce dimensions as well, so that the following...
```
requests:+11|c service=http1,host=web1
requests:+12|c service=http1,host=web2
```
...will result in a set of permutations to be send.

```
put requests 11 <timestamp> service=http1,host=web1
put requests 12 <timestamp> service=http1,host=web2
```
An additional increase in requests.

```
requests:+1|c service=http1,host=web1
requests:+2|c service=http1,host=web2
```
will result in the following metrics:

```
put requests 12 <timestamp> service=http1,host=web1
put requests 14 <timestamp> service=http1,host=web2
```
Thus, the metrics can be grouped.

