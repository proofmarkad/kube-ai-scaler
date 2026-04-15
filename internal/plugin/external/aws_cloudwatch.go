package external

import (
	"context"
	"fmt"
	"strconv"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
	plugin.Register("cloudwatch", func() plugin.SignalPlugin {
		return &cloudWatchPlugin{}
	})
}

type cloudWatchPlugin struct {
	region         string
	namespace      string
	metricName     string
	dimensionName  string
	dimensionValue string
	statistic      cwtypes.Statistic
	period         int32
	client         *cloudwatch.Client
	healthy        bool
}

func (p *cloudWatchPlugin) Name() string   { return "cloudwatch" }
func (p *cloudWatchPlugin) Required() bool { return false }
func (p *cloudWatchPlugin) Healthy() bool  { return p.healthy }

func (p *cloudWatchPlugin) Init(cfg map[string]string) error {
	p.region = cfg["region"]
	if p.region == "" {
		return fmt.Errorf("cloudwatch plugin requires 'region' config")
	}
	p.namespace = cfg["namespace"]
	if p.namespace == "" {
		return fmt.Errorf("cloudwatch plugin requires 'namespace' config")
	}
	p.metricName = cfg["metricName"]
	if p.metricName == "" {
		return fmt.Errorf("cloudwatch plugin requires 'metricName' config")
	}
	p.dimensionName = cfg["dimensionName"]
	p.dimensionValue = cfg["dimensionValue"]

	switch cfg["statistic"] {
	case "Sum":
		p.statistic = cwtypes.StatisticSum
	case "Maximum":
		p.statistic = cwtypes.StatisticMaximum
	case "Minimum":
		p.statistic = cwtypes.StatisticMinimum
	case "SampleCount":
		p.statistic = cwtypes.StatisticSampleCount
	default:
		p.statistic = cwtypes.StatisticAverage
	}

	p.period = 300
	if v, ok := cfg["period"]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			p.period = int32(i)
		}
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(p.region),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	p.client = cloudwatch.NewFromConfig(awsCfg)
	p.healthy = true
	return nil
}

func (p *cloudWatchPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	now := time.Now()
	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  &p.namespace,
		MetricName: &p.metricName,
		StartTime:  timePtr(now.Add(-time.Duration(p.period) * time.Second)),
		EndTime:    timePtr(now),
		Period:     &p.period,
		Statistics: []cwtypes.Statistic{p.statistic},
	}

	if p.dimensionName != "" && p.dimensionValue != "" {
		input.Dimensions = []cwtypes.Dimension{
			{Name: &p.dimensionName, Value: &p.dimensionValue},
		}
	}

	out, err := p.client.GetMetricStatistics(ctx, input)
	if err != nil {
		p.healthy = false
		return fmt.Errorf("cloudwatch GetMetricStatistics failed: %w", err)
	}

	if len(out.Datapoints) == 0 {
		p.healthy = true
		return nil
	}

	// Use the most recent datapoint
	latest := out.Datapoints[0]
	for i := range out.Datapoints {
		if out.Datapoints[i].Timestamp != nil && latest.Timestamp != nil {
			if out.Datapoints[i].Timestamp.After(*latest.Timestamp) {
				latest = out.Datapoints[i]
			}
		}
	}

	var val float64
	switch p.statistic {
	case cwtypes.StatisticAverage:
		if latest.Average != nil {
			val = *latest.Average
		}
	case cwtypes.StatisticSum:
		if latest.Sum != nil {
			val = *latest.Sum
		}
	case cwtypes.StatisticMaximum:
		if latest.Maximum != nil {
			val = *latest.Maximum
		}
	case cwtypes.StatisticMinimum:
		if latest.Minimum != nil {
			val = *latest.Minimum
		}
	case cwtypes.StatisticSampleCount:
		if latest.SampleCount != nil {
			val = *latest.SampleCount
		}
	}

	if bundle.CustomSignals == nil {
		bundle.CustomSignals = make(map[string]float64)
	}
	bundle.CustomSignals[fmt.Sprintf("cloudwatch_%s_%s", p.namespace, p.metricName)] = val

	p.healthy = true
	return nil
}

func timePtr(t time.Time) *time.Time { return &t }
