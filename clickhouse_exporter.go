package main

import (
	"database/sql"
	"flag"
	"net/http"

	"github.com/nineinchnick/clickhouse_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/log"
)

var (
	listeningAddress    = flag.String("telemetry.address", ":9116", "Address on which to expose metrics.")
	metricsEndpoint     = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
	clickhouseScrapeURI = flag.String("scrape_uri", "tcp://localhost:9000/", "URI to clickhouse tcp endpoint")
)

func main() {
	flag.Parse()

	db, err := sql.Open("clickhouse", *clickhouseScrapeURI)
	if err != nil {
		log.Fatal(err)
	}
	e, err := exporter.NewExporter(db)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(e)

	log.Printf("Starting Server: %s", *listeningAddress)
	http.Handle(*metricsEndpoint, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Clickhouse Exporter</title></head>
			<body>
			<h1>Clickhouse Exporter</h1>
			<p><a href="` + *metricsEndpoint + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}
