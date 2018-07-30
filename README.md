# Clickhouse Exporter for Prometheus

[![Build Status](https://travis-ci.org/nineinchnick/clickhouse_exporter.svg?branch=master)](https://travis-ci.org/nineinchnick/clickhouse_exporter)
[![Go Report Card](https://goreportcard.com/badge/github.com/nineinchnick/clickhouse_exporter)](https://goreportcard.com/report/github.com/nineinchnick/clickhouse_exporter)

This is a simple server that periodically scrapes ClickHouse(https://clickhouse.yandex/) stats and exports them via HTTP for Prometheus(https://prometheus.io/)
consumption.

To run it:

```bash
./clickhouse_exporter [flags]
```

Help on flags:
```bash
./clickhouse_exporter --help
```

Credentials(if not default):

via environment variables
```
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
```

## Using Docker

```
docker run -d -p 9116:9116 nineinchnick/clickhouse-exporter -scrape_uri=tcp://clickhouse.service.consul:9000/
```
## Sample dashboard
Grafana dashboard could be a start for inspiration https://grafana.net/dashboards/882
