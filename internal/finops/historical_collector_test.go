package finops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHistoricalCollectorQueryRange_EncodesPromQL(t *testing.T) {
	expectedQuery := `sum(rate(container_cpu_usage_seconds_total{namespace="prod"}[5m])) + on() vector(1)`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != expectedQuery {
			t.Fatalf("expected query %q, got %q", expectedQuery, got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"values":[[1700000000,"0.50"],[1700000300,"0.75"]]}]}}`))
	}))
	defer server.Close()

	collector := NewHistoricalCollector(server.URL)
	values, err := collector.queryRange(context.Background(), expectedQuery, 10)
	if err != nil {
		t.Fatalf("queryRange returned error: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if values[0] != 0.50 || values[1] != 0.75 {
		t.Fatalf("unexpected parsed values: %#v", values)
	}
}
