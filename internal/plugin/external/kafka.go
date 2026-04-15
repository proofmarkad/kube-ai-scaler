package external

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"

	"github.com/IBM/sarama"
)

func init() {
	plugin.Register("kafka", func() plugin.SignalPlugin {
		return &kafkaPlugin{}
	})
}

// kafkaPlugin reads consumer group lag from Kafka via the admin client.
type kafkaPlugin struct {
	brokers       []string
	consumerGroup string
	topic         string
	timeout       time.Duration
	healthy       bool
}

func (k *kafkaPlugin) Name() string   { return "kafka" }
func (k *kafkaPlugin) Required() bool { return false }
func (k *kafkaPlugin) Healthy() bool  { return k.healthy }

func (k *kafkaPlugin) Init(cfg map[string]string) error {
	brokersRaw := cfg["brokers"]
	if brokersRaw == "" {
		return fmt.Errorf("kafka plugin requires 'brokers' config (comma-separated)")
	}
	k.brokers = strings.Split(brokersRaw, ",")

	k.consumerGroup = cfg["consumerGroup"]
	if k.consumerGroup == "" {
		return fmt.Errorf("kafka plugin requires 'consumerGroup' config")
	}
	k.topic = cfg["topic"]
	if k.topic == "" {
		return fmt.Errorf("kafka plugin requires 'topic' config")
	}
	k.timeout = 10 * time.Second
	if v, ok := cfg["timeoutSeconds"]; ok {
		if secs, err := strconv.Atoi(v); err == nil {
			k.timeout = time.Duration(secs) * time.Second
		}
	}
	k.healthy = true
	return nil
}

func (k *kafkaPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	config := sarama.NewConfig()
	config.Net.DialTimeout = k.timeout
	config.Net.ReadTimeout = k.timeout

	admin, err := sarama.NewClusterAdmin(k.brokers, config)
	if err != nil {
		k.healthy = false
		return fmt.Errorf("failed to create kafka admin: %w", err)
	}
	defer admin.Close()

	// Get topic partition offsets (latest)
	client, err := sarama.NewClient(k.brokers, config)
	if err != nil {
		k.healthy = false
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(k.topic)
	if err != nil {
		k.healthy = false
		return fmt.Errorf("failed to get partitions for topic %q: %w", k.topic, err)
	}

	// Get consumer group offsets
	offsets, err := admin.ListConsumerGroupOffsets(k.consumerGroup, map[string][]int32{k.topic: partitions})
	if err != nil {
		k.healthy = false
		return fmt.Errorf("failed to get consumer group offsets: %w", err)
	}

	var totalLag int64
	for _, partition := range partitions {
		latestOffset, err := client.GetOffset(k.topic, partition, sarama.OffsetNewest)
		if err != nil {
			continue
		}
		block := offsets.GetBlock(k.topic, partition)
		if block != nil && block.Offset >= 0 {
			lag := latestOffset - block.Offset
			if lag > 0 {
				totalLag += lag
			}
		}
	}

	bundle.QueueDepth = float64(totalLag)
	if bundle.CustomSignals == nil {
		bundle.CustomSignals = make(map[string]float64)
	}
	bundle.CustomSignals["kafka_consumer_lag"] = float64(totalLag)

	k.healthy = true
	return nil
}
