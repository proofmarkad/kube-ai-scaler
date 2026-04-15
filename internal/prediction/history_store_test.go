package prediction

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHistoryStoreQueryHourlyAverages_EncodesPromQL(t *testing.T) {
	expectedQuery := `avg(rate(http_requests_total{namespace="prod"}[5m])) + on() vector(1)`
	ts1 := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC).Unix()
	ts2 := time.Date(2025, 1, 13, 10, 0, 0, 0, time.UTC).Unix()
	expectedSlot := int(time.Unix(ts1, 0).Weekday())*24 + time.Unix(ts1, 0).Hour()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != expectedQuery {
			t.Fatalf("expected query %q, got %q", expectedQuery, got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(
			`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[%d,"3"],[%d,"5"]]}]}}`,
			ts1,
			ts2,
		)))
	}))
	defer server.Close()

	store := NewHistoryStore(server.URL)
	averages, err := store.QueryHourlyAverages(context.Background(), expectedQuery, 14)
	if err != nil {
		t.Fatalf("QueryHourlyAverages returned error: %v", err)
	}

	avg, ok := averages[expectedSlot]
	if !ok {
		t.Fatalf("expected slot %d in averages map", expectedSlot)
	}
	if avg != 4 {
		t.Fatalf("expected average 4, got %v", avg)
	}
	if ts2 <= ts1 {
		t.Fatalf("test timestamps must be ordered")
	}
}
