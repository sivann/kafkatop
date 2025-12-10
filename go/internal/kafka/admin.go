package kafka

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sivann/kafkatop/internal/types"
)

// AdminClient wraps Kafka admin operations
type AdminClient struct {
	broker string
	conn   *kafka.Conn
}

// NewAdminClient creates a new Kafka admin client
func NewAdminClient(broker string) (*AdminClient, error) {
	// Add default port if not specified
	if matched, _ := regexp.MatchString(`:\d+$`, broker); !matched {
		broker = broker + ":9092"
	}

	// Test connection
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %w", broker, err)
	}

	return &AdminClient{
		broker: broker,
		conn:   conn,
	}, nil
}

// Close closes the admin client connection
func (a *AdminClient) Close() error {
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

// ListConsumerGroups lists all consumer groups
func (a *AdminClient) ListConsumerGroups(ctx context.Context, params *types.Params) (map[string]*types.ConsumerGroup, error) {
	conn, err := kafka.Dial("tcp", a.broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	// Get coordinator
	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller: %w", err)
	}

	coordConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer coordConn.Close()

	groups := make(map[string]*types.ConsumerGroup)

	// Note: kafka-go library doesn't provide a direct consumer group listing API
	// In a production implementation, you would need to:
	// 1. Use the Kafka wire protocol directly
	// 2. Or use a different library like sarama
	// 3. Or maintain a known list of consumer groups

	// Compile filter patterns for future use
	var filterPattern, excludePattern *regexp.Regexp
	if params.KafkaGroupFilterPattern != "" {
		filterPattern = regexp.MustCompile(params.KafkaGroupFilterPattern)
		_ = filterPattern // TODO: implement filtering when group listing is available
	}
	if params.KafkaGroupExcludePattern != "" {
		excludePattern = regexp.MustCompile(params.KafkaGroupExcludePattern)
		_ = excludePattern // TODO: implement filtering when group listing is available
	}

	// For now, we return empty groups which will be caught by the caller
	// The Python version uses confluent-kafka which wraps librdkafka
	// which provides this functionality

	return groups, nil
}

// DescribeConsumerGroups describes consumer groups and returns their topic assignments
func (a *AdminClient) DescribeConsumerGroups(ctx context.Context, groupIDs []string) (map[string]*types.ConsumerGroup, error) {
	groups := make(map[string]*types.ConsumerGroup)

	for _, groupID := range groupIDs {
		cg := &types.ConsumerGroup{
			GroupID: groupID,
			State:   types.StateUnknown,
			Topics:  make(map[string][]int32),
		}

		groups[groupID] = cg
	}

	return groups, nil
}

// ListConsumerGroupOffsets lists offsets for a consumer group
func (a *AdminClient) ListConsumerGroupOffsets(ctx context.Context, groupID string) (*types.ConsumerGroupOffset, error) {
	conn, err := kafka.Dial("tcp", a.broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller: %w", err)
	}

	coordConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %w", err)
	}
	defer coordConn.Close()

	offsets := &types.ConsumerGroupOffset{
		GroupID:      groupID,
		TopicOffsets: make(map[string]map[int32]int64),
		Timestamp:    time.Now(),
	}

	// Note: kafka-go doesn't provide easy consumer group offset fetching
	// This would need to be implemented using the wire protocol directly
	// For now, return empty offsets

	return offsets, nil
}

// ListTopicOffsets lists latest offsets for topic partitions
func (a *AdminClient) ListTopicOffsets(ctx context.Context, topic string, partitions []int32) (*types.TopicOffset, error) {
	offsets := &types.TopicOffset{
		Topic:            topic,
		PartitionOffsets: make(map[int32]int64),
		Timestamp:        time.Now(),
	}

	for _, partition := range partitions {
		conn, err := kafka.DialLeader(ctx, "tcp", a.broker, topic, int(partition))
		if err != nil {
			// Partition may not exist or have no leader
			continue
		}

		offset, err := conn.ReadLastOffset()
		conn.Close()

		if err != nil {
			continue
		}

		offsets.PartitionOffsets[partition] = offset
	}

	return offsets, nil
}

// ListTopics lists all topics in the cluster
func (a *AdminClient) ListTopics(ctx context.Context) (map[string]*types.TopicInfo, error) {
	conn, err := kafka.Dial("tcp", a.broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	topics := make(map[string]*types.TopicInfo)

	for _, partition := range partitions {
		topic := partition.Topic
		if _, exists := topics[topic]; !exists {
			topics[topic] = &types.TopicInfo{
				Name:       topic,
				Partitions: 0,
				PartInfo:   []types.PartitionInfo{},
			}
		}
		topics[topic].Partitions++

		topics[topic].PartInfo = append(topics[topic].PartInfo, types.PartitionInfo{
			ID:       int32(partition.ID),
			Leader:   int32(partition.Leader.ID),
			Replicas: convertToInt32Slice(partition.Replicas),
			ISRs:     len(partition.Isr),
		})
	}

	return topics, nil
}

// GetClusterInfo retrieves cluster metadata
func (a *AdminClient) GetClusterInfo(ctx context.Context) (*types.ClusterInfo, error) {
	conn, err := kafka.Dial("tcp", a.broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	brokers, err := conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("failed to get brokers: %w", err)
	}

	controller, err := conn.Controller()
	if err != nil {
		// Controller info is optional
		controller = kafka.Broker{}
	}

	info := &types.ClusterInfo{
		Topics:     make(map[string]*types.TopicInfo),
		Brokers:    make(map[int32]*types.BrokerInfo),
		BrokerName: a.broker,
		ClusterID:  fmt.Sprintf("%d", controller.ID),
	}

	for _, broker := range brokers {
		info.Brokers[int32(broker.ID)] = &types.BrokerInfo{
			ID:   int32(broker.ID),
			Host: broker.Host,
			Port: int32(broker.Port),
		}
	}

	topics, err := a.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	info.Topics = topics

	return info, nil
}

// Helper functions

func mapGroupState(state string) types.ConsumerGroupState {
	switch state {
	case "Stable":
		return types.StateStable
	case "Empty":
		return types.StateEmpty
	case "Dead":
		return types.StateDead
	case "PreparingRebalance":
		return types.StatePreparingRebalance
	case "CompletingRebalance":
		return types.StateCompletingRebalance
	default:
		return types.StateUnknown
	}
}

func convertToInt32Slice(brokers []kafka.Broker) []int32 {
	result := make([]int32, len(brokers))
	for i, b := range brokers {
		result[i] = int32(b.ID)
	}
	return result
}
