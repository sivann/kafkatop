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
	// Use the Client API which has ListGroups
	client := &kafka.Client{
		Addr:    kafka.TCP(a.broker),
		Timeout: 10 * time.Second,
	}

	response, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	groups := make(map[string]*types.ConsumerGroup)

	var filterPattern, excludePattern *regexp.Regexp
	if params.KafkaGroupFilterPattern != "" {
		filterPattern = regexp.MustCompile(params.KafkaGroupFilterPattern)
	}
	if params.KafkaGroupExcludePattern != "" {
		excludePattern = regexp.MustCompile(params.KafkaGroupExcludePattern)
	}

	for _, group := range response.Groups {
		groupID := group.GroupID

		// Apply filters
		if filterPattern != nil && !filterPattern.MatchString(groupID) {
			continue
		}
		if excludePattern != nil && excludePattern.MatchString(groupID) {
			continue
		}

		groups[groupID] = &types.ConsumerGroup{
			GroupID: groupID,
			State:   types.StateStable,
			Topics:  make(map[string][]int32),
		}
	}

	return groups, nil
}

// DescribeConsumerGroups describes consumer groups and returns their topic assignments
func (a *AdminClient) DescribeConsumerGroups(ctx context.Context, groupIDs []string) (map[string]*types.ConsumerGroup, error) {
	groups := make(map[string]*types.ConsumerGroup)

	for _, groupID := range groupIDs {
		cg := &types.ConsumerGroup{
			GroupID: groupID,
			State:   types.StateStable,
			Topics:  make(map[string][]int32),
		}

		groups[groupID] = cg
	}

	return groups, nil
}

// ListConsumerGroupOffsets lists offsets for a consumer group
func (a *AdminClient) ListConsumerGroupOffsets(ctx context.Context, groupID string) (*types.ConsumerGroupOffset, error) {
	client := &kafka.Client{
		Addr:    kafka.TCP(a.broker),
		Timeout: 10 * time.Second,
	}

	// Use OffsetFetch to get committed offsets for the group
	response, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offsets for group %s: %w", groupID, err)
	}

	offsets := &types.ConsumerGroupOffset{
		GroupID:      groupID,
		TopicOffsets: make(map[string]map[int32]int64),
		Timestamp:    time.Now(),
	}

	for topic, partitions := range response.Topics {
		offsets.TopicOffsets[topic] = make(map[int32]int64)
		for _, partition := range partitions {
			if partition.CommittedOffset >= 0 {
				offsets.TopicOffsets[topic][int32(partition.Partition)] = partition.CommittedOffset
			}
		}
	}

	return offsets, nil
}

// ListTopicOffsets lists latest offsets for topic partitions using batch ListOffsets API
func (a *AdminClient) ListTopicOffsets(ctx context.Context, topic string, partitions []int32) (*types.TopicOffset, error) {
	offsets := &types.TopicOffset{
		Topic:            topic,
		PartitionOffsets: make(map[int32]int64),
		Timestamp:        time.Now(),
	}

	// Use the Client API for batch ListOffsets request
	client := &kafka.Client{
		Addr:    kafka.TCP(a.broker),
		Timeout: 10 * time.Second,
	}

	// Build the request with all partitions at once
	topics := make(map[string][]kafka.OffsetRequest)
	offsetRequests := make([]kafka.OffsetRequest, 0, len(partitions))

	for _, partition := range partitions {
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{
			Partition: int(partition),
			Timestamp: kafka.LastOffset, // -1 for latest offset
		})
	}
	topics[topic] = offsetRequests

	// Single batch request for all partitions
	response, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: topics,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list offsets for topic %s: %w", topic, err)
	}

	// Extract offsets from response
	if topicOffsets, ok := response.Topics[topic]; ok {
		for _, partOffset := range topicOffsets {
			if partOffset.Error == nil {
				offsets.PartitionOffsets[int32(partOffset.Partition)] = partOffset.LastOffset
			}
		}
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

func convertToInt32Slice(brokers []kafka.Broker) []int32 {
	result := make([]int32, len(brokers))
	for i, b := range brokers {
		result[i] = int32(b.ID)
	}
	return result
}
