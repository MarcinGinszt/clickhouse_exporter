package exporter

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestScrape(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exporter, err := NewExporter(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Describe", func(t *testing.T) {
		ch := make(chan *prometheus.Desc)
		go func() {
			exporter.Describe(ch)
			close(ch)
		}()

		for range ch {
		}
	})

	t.Run("Collect", func(t *testing.T) {
		ch := make(chan prometheus.Metric)
		var err error
		go func() {
			rows := sqlmock.NewRows([]string{"name", "value"}).
				AddRow("one", 1).
				AddRow("two", 2)
			partsRows := sqlmock.NewRows([]string{"database", "table", "bytes", "parts", "rows"}).
				AddRow("db", "one", 1, 2, 3).
				AddRow("db", "two", 2, 3, 4)
			mock.ExpectQuery("^select metric,value").WillReturnRows(rows)
			mock.ExpectQuery("^select metric,value").WillReturnRows(rows)
			mock.ExpectQuery("^select event,value").WillReturnRows(rows)
			mock.ExpectQuery("^select database").WillReturnRows(partsRows)
			err = exporter.collect(nil, ch)
			if err != nil {
				panic(err)
			}
			close(ch)
		}()

		for range ch {
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}
