package external

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
	plugin.Register("aws-sqs", func() plugin.SignalPlugin {
		return &sqsPlugin{}
	})
}

type sqsPlugin struct {
	queueURL          string
	region            string
	targetQueueLength float64
	client            *sqs.Client
	healthy           bool
}

func (p *sqsPlugin) Name() string   { return "aws-sqs" }
func (p *sqsPlugin) Required() bool { return false }
func (p *sqsPlugin) Healthy() bool  { return p.healthy }

func (p *sqsPlugin) Init(cfg map[string]string) error {
	p.queueURL = cfg["queueURL"]
	if p.queueURL == "" {
		return fmt.Errorf("aws-sqs plugin requires 'queueURL' config")
	}
	p.region = cfg["region"]
	if p.region == "" {
		p.region = "us-east-1"
	}
	p.targetQueueLength = 5
	if v, ok := cfg["targetQueueLength"]; ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			p.targetQueueLength = f
		}
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(p.region),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	p.client = sqs.NewFromConfig(awsCfg)
	p.healthy = true
	return nil
}

func (p *sqsPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	out, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(p.queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		p.healthy = false
		return fmt.Errorf("failed to get SQS attributes: %w", err)
	}

	visible, _ := strconv.ParseFloat(out.Attributes["ApproximateNumberOfMessages"], 64)
	inflight, _ := strconv.ParseFloat(out.Attributes["ApproximateNumberOfMessagesNotVisible"], 64)

	bundle.QueueDepth = visible + inflight
	if bundle.CustomSignals == nil {
		bundle.CustomSignals = make(map[string]float64)
	}
	bundle.CustomSignals["sqs_visible_messages"] = visible
	bundle.CustomSignals["sqs_inflight_messages"] = inflight
	if p.targetQueueLength > 0 {
		bundle.CustomSignals["sqs_target_ratio"] = (visible + inflight) / p.targetQueueLength
	}

	p.healthy = true
	return nil
}
