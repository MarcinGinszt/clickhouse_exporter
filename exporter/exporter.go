package exporter

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const (
	namespace = "clickhouse" // For Prometheus metrics.

	metricsQuery      = "select metric,value from system.metrics"
	asyncMetricsQuery = "select metric,value from system.asynchronous_metrics"
	eventsQuery       = "select event,value from system.events"
	partsQuery        = "select database, table, sum(bytes) as bytes, count() as parts, sum(rows) as rows from system.parts where active = 1 group by database, table"

	// Timeout is default for all database operations
	Timeout = 1 * time.Second
)

type sqldb interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

// Exporter collects clickhouse stats and exports them using
// the prometheus metrics package.
type Exporter struct {
	db    sqldb
	mutex sync.RWMutex

	scrapeFailures prometheus.Counter

	gauges   []*prometheus.GaugeVec
	counters []*prometheus.CounterVec

	user     string
	password string
}

// NewExporter returns an initialized Exporter.
func NewExporter(db sqldb) (*Exporter, error) {
	return &Exporter{
		db: db,
		scrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrape_failures_total",
			Help:      "Number of errors while scraping clickhouse.",
		}),
		gauges:   make([]*prometheus.GaugeVec, 0, 20),
		counters: make([]*prometheus.CounterVec, 0, 20),
	}, nil
}

// Describe describes all the metrics ever exported by the clickhouse exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from clickhouse. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	metrics, err := e.queryMetrics()
	if err != nil {
		return err
	}

	for _, m := range metrics {
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricName(m.key),
			Help:      "Number of " + m.key + " currently processed",
		}, []string{}).WithLabelValues()
		newMetric.Set(m.value)
		newMetric.Collect(ch)
	}

	asyncMetrics, err := e.queryAsyncMetrics()
	if err != nil {
		return err
	}

	for _, am := range asyncMetrics {
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricName(am.key),
			Help:      "Number of " + am.key + " async processed",
		}, []string{}).WithLabelValues()
		newMetric.Set(am.value)
		newMetric.Collect(ch)
	}

	events, err := e.queryEvents()
	if err != nil {
		return err
	}

	for _, ev := range events {
		newMetric, _ := prometheus.NewConstMetric(
			prometheus.NewDesc(
				namespace+"_"+metricName(ev.key)+"_total",
				"Number of "+ev.key+" total processed", []string{}, nil),
			prometheus.CounterValue, ev.value)
		ch <- newMetric
	}

	parts, err := e.queryParts(partsQuery)
	if err != nil {
		return err
	}

	for _, part := range parts {
		newBytesMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "table_parts_bytes",
			Help:      "Table size in bytes",
		}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
		newBytesMetric.Set(float64(part.bytes))
		newBytesMetric.Collect(ch)

		newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "table_parts_count",
			Help:      "Number of parts of the table",
		}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
		newCountMetric.Set(float64(part.parts))
		newCountMetric.Collect(ch)

		newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "table_parts_rows",
			Help:      "Number of rows in the table",
		}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
		newRowsMetric.Set(float64(part.rows))
		newRowsMetric.Collect(ch)
	}

	return nil
}

type queryResult struct {
	key   string
	value float64
}

func query(db sqldb, query string, scanner func(rows *sql.Rows) (queryResult, error)) ([]queryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []queryResult

	for rows.Next() {
		row, err := scanner(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, row)

	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func asyncMetricsScanner(rows *sql.Rows) (queryResult, error) {
	row := queryResult{}
	err := rows.Scan(&row.key, &row.value)
	return row, err
}

func (e *Exporter) queryAsyncMetrics() ([]queryResult, error) {
	return query(e.db, asyncMetricsQuery, asyncMetricsScanner)
}

func eventsScanner(rows *sql.Rows) (queryResult, error) {
	row := queryResult{}
	var value uint64
	err := rows.Scan(&row.key, &value)
	row.value = float64(value)
	return row, err
}

func (e *Exporter) queryEvents() ([]queryResult, error) {
	return query(e.db, eventsQuery, eventsScanner)
}

func metricsScanner(rows *sql.Rows) (queryResult, error) {
	row := queryResult{}
	var value int64
	err := rows.Scan(&row.key, &value)
	row.value = float64(value)
	return row, err
}

func (e *Exporter) queryMetrics() ([]queryResult, error) {
	return query(e.db, metricsQuery, metricsScanner)
}

type partsResult struct {
	database string
	table    string
	bytes    int
	parts    int
	rows     int
}

func (e *Exporter) queryParts(query string) ([]partsResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []partsResult

	for rows.Next() {
		row := partsResult{}
		if err := rows.Scan(&row.database, &row.table, &row.bytes, &row.parts, &row.rows); err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// Collect fetches the stats from configured clickhouse location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock() // To protect metrics from concurrent collects.
	defer e.mutex.Unlock()
	if err := e.collect(ch); err != nil {
		log.Printf("Error scraping clickhouse: %s", err)
		e.scrapeFailures.Inc()
		e.scrapeFailures.Collect(ch)
	}
	// Reset metrics.
	for _, vec := range e.gauges {
		vec.Reset()
	}

	for _, vec := range e.counters {
		vec.Reset()
	}

	for _, vec := range e.gauges {
		vec.Collect(ch)
	}

	for _, vec := range e.counters {
		vec.Collect(ch)
	}

	return
}

func metricName(in string) string {
	out := toSnake(in)
	return strings.Replace(out, ".", "_", -1)
}

// toSnake convert the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func toSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}

// check interface
var _ prometheus.Collector = (*Exporter)(nil)
