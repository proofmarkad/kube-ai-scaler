package external

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const gcpMonitoringReadScope = "https://www.googleapis.com/auth/monitoring.read"

func newGCPMonitoringClient(ctx context.Context, timeout time.Duration) (*http.Client, error) {
	tokenSource, err := google.DefaultTokenSource(ctx, gcpMonitoringReadScope)
	if err != nil {
		return nil, fmt.Errorf("load GCP application default credentials: %w", err)
	}

	client := oauth2.NewClient(ctx, tokenSource)
	client.Timeout = timeout
	return client, nil
}

func newGCPTimeSeriesRequest(
	ctx context.Context,
	projectID string,
	filter string,
	start time.Time,
	end time.Time,
) (*http.Request, error) {
	endpoint := fmt.Sprintf(
		"https://monitoring.googleapis.com/v3/projects/%s/timeSeries",
		url.PathEscape(projectID),
	)
	params := url.Values{
		"filter":             {filter},
		"interval.startTime": {start.UTC().Format(time.RFC3339)},
		"interval.endTime":   {end.UTC().Format(time.RFC3339)},
		"view":               {"FULL"},
	}

	return http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
}
