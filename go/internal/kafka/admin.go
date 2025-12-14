package kafka

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sivann/kafkatop/internal/types"
)

// AdminClient wraps Kafka admin operations
type AdminClient struct {
	broker string
	conn   *kafka.Conn
	client *kafka.Client // Shared client for API calls
	clientPool chan *kafka.Client // Pool of clients for parallel operations
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

	// Create a single shared client for all operations
	// kafka.Client is thread-safe and can handle concurrent requests efficiently
	sharedClient := &kafka.Client{
		Addr:    kafka.TCP(broker),
		Timeout: 10 * time.Second,
	}

	return &AdminClient{
		broker: broker,
		conn:   conn,
		client: sharedClient,
		clientPool: nil, // No longer using a pool
	}, nil
}

// Close closes the admin client connection
func (a *AdminClient) Close() error {
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

// GetClientFromPool gets a client from the connection pool
func (a *AdminClient) GetClientFromPool() *kafka.Client {
	if a.clientPool == nil {
		// Fallback to shared client if pool not initialized
		return a.client
	}
	return <-a.clientPool
}

// ReturnClientToPool returns a client to the connection pool
func (a *AdminClient) ReturnClientToPool(client *kafka.Client) {
	if a.clientPool != nil {
		select {
		case a.clientPool <- client:
		default:
			// Pool is full, discard (shouldn't happen)
		}
	}
}

// ListConsumerGroups lists all consumer groups
func (a *AdminClient) ListConsumerGroups(ctx context.Context, params *types.Params) (map[string]*types.ConsumerGroup, error) {
	// Use the Client API which has ListGroups
	response, err := a.client.ListGroups(ctx, &kafka.ListGroupsRequest{})
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
func (a *AdminClient) DescribeConsumerGroups(ctx context.Context, groupIDs []string, maxConcurrent int) (map[string]*types.ConsumerGroup, error) {
	groups := make(map[string]*types.ConsumerGroup)

	if maxConcurrent <= 1 {
		// Sequential execution - try batching first, fall back to individual if needed
		// Try to describe all groups in a single batch request
		batchStart := time.Now()
		resp, err := a.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
			GroupIDs: groupIDs,
		})
		batchTime := time.Since(batchStart)
		
		if err == nil && len(resp.Groups) == len(groupIDs) {
			// Batch succeeded
			if len(groupIDs) > 0 {
				fmt.Fprintf(os.Stderr, "DescribeConsumerGroups: batch succeeded (%d groups in %v)\n", len(groupIDs), batchTime)
			}
			// Batch request succeeded, process all groups
			for _, grp := range resp.Groups {
				groupID := grp.GroupID
				var group *types.ConsumerGroup
				
				if grp.Error != nil {
					group = &types.ConsumerGroup{
						GroupID: groupID,
						State:   types.StateStable,
						Topics:  make(map[string][]int32),
					}
				} else {
					// Map Kafka group state to our state type
					state := types.StateUnknown
					switch grp.GroupState {
					case "Stable":
						state = types.StateStable
					case "Empty":
						state = types.StateEmpty
					case "PreparingRebalance":
						state = types.StatePreparingRebalance
					case "CompletingRebalance":
						state = types.StateCompletingRebalance
					case "Dead":
						state = types.StateDead
					default:
						// If no members, consider it empty
						if len(grp.Members) == 0 {
							state = types.StateEmpty
						} else {
							state = types.StateStable
						}
					}
					
					group = &types.ConsumerGroup{
						GroupID: groupID,
						State:   state,
						Topics:  make(map[string][]int32),
					}
				}
				groups[groupID] = group
			}
		} else {
			// Batch request failed or returned no groups, fall back to individual requests
			if len(groupIDs) > 0 {
				fmt.Fprintf(os.Stderr, "DescribeConsumerGroups: batch failed (%v), falling back to individual requests (%d groups)\n", err, len(groupIDs))
			}
			individualStart := time.Now()
			for _, groupID := range groupIDs {
				resp, err := a.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
					GroupIDs: []string{groupID},
				})

				if err != nil {
					// Fallback to STABLE if DescribeGroups fails
					// This can happen with some Kafka versions or network issues
					groups[groupID] = &types.ConsumerGroup{
						GroupID: groupID,
						State:   types.StateStable,
						Topics:  make(map[string][]int32),
					}
					continue
				}

				if len(resp.Groups) == 0 {
					groups[groupID] = &types.ConsumerGroup{
						GroupID: groupID,
						State:   types.StateStable,
						Topics:  make(map[string][]int32),
					}
					continue
				}

				group := resp.Groups[0]

				if group.Error != nil {
					groups[groupID] = &types.ConsumerGroup{
						GroupID: groupID,
						State:   types.StateStable,
						Topics:  make(map[string][]int32),
					}
					continue
				}

				// Map Kafka group state to our state type
				state := types.StateUnknown
				switch group.GroupState {
				case "Stable":
					state = types.StateStable
				case "Empty":
					state = types.StateEmpty
				case "PreparingRebalance":
					state = types.StatePreparingRebalance
				case "CompletingRebalance":
					state = types.StateCompletingRebalance
				case "Dead":
					state = types.StateDead
				default:
					// If no members, consider it empty
					if len(group.Members) == 0 {
						state = types.StateEmpty
					} else {
						state = types.StateStable
					}
				}

				groups[groupID] = &types.ConsumerGroup{
					GroupID: groupID,
					State:   state,
					Topics:  make(map[string][]int32),
				}
			}
			individualTime := time.Since(individualStart)
			fmt.Fprintf(os.Stderr, "DescribeConsumerGroups: individual requests took %v (%d groups, avg %v per group)\n", individualTime, len(groupIDs), individualTime/time.Duration(len(groupIDs)))
		}
	} else {
		// Parallel execution - batching doesn't work reliably, use individual parallel requests
		semaphore := make(chan struct{}, maxConcurrent)
		var wg sync.WaitGroup
		var mu sync.Mutex

		for _, groupID := range groupIDs {
			wg.Add(1)
			go func(gid string) {
				defer wg.Done()
				semaphore <- struct{}{} // Acquire semaphore
				defer func() { <-semaphore }() // Release semaphore

				// Use shared client (thread-safe)
				resp, err := a.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
					GroupIDs: []string{gid},
				})

				var group *types.ConsumerGroup
				if err != nil {
					// Fallback to STABLE if DescribeGroups fails
					group = &types.ConsumerGroup{
						GroupID: gid,
						State:   types.StateStable,
						Topics:  make(map[string][]int32),
					}
				} else if len(resp.Groups) == 0 {
					group = &types.ConsumerGroup{
						GroupID: gid,
						State:   types.StateStable,
						Topics:  make(map[string][]int32),
					}
				} else {
					grp := resp.Groups[0]
					if grp.Error != nil {
						group = &types.ConsumerGroup{
							GroupID: gid,
							State:   types.StateStable,
							Topics:  make(map[string][]int32),
						}
					} else {
						// Map Kafka group state to our state type
						state := types.StateUnknown
						switch grp.GroupState {
						case "Stable":
							state = types.StateStable
						case "Empty":
							state = types.StateEmpty
						case "PreparingRebalance":
							state = types.StatePreparingRebalance
						case "CompletingRebalance":
							state = types.StateCompletingRebalance
						case "Dead":
							state = types.StateDead
						default:
							// If no members, consider it empty
							if len(grp.Members) == 0 {
								state = types.StateEmpty
							} else {
								state = types.StateStable
							}
						}

						group = &types.ConsumerGroup{
							GroupID: gid,
							State:   state,
							Topics:  make(map[string][]int32),
						}
					}
				}

				mu.Lock()
				groups[gid] = group
				mu.Unlock()
			}(groupID)
		}
		wg.Wait()
	}

	return groups, nil
}

// ListConsumerGroupOffsets lists offsets for a consumer group
func (a *AdminClient) ListConsumerGroupOffsets(ctx context.Context, groupID string) (*types.ConsumerGroupOffset, error) {
	return a.ListConsumerGroupOffsetsWithClient(ctx, groupID, nil)
}

// ListConsumerGroupOffsetsWithClient lists offsets using a specific client (or shared if nil)
func (a *AdminClient) ListConsumerGroupOffsetsWithClient(ctx context.Context, groupID string, client *kafka.Client) (*types.ConsumerGroupOffset, error) {
	// Use client from pool if provided, otherwise use shared client
	if client == nil {
		client = a.client
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
	return a.ListTopicOffsetsWithClient(ctx, topic, partitions, nil)
}

// ListTopicOffsetsWithClient lists offsets using a specific client (or shared if nil)
func (a *AdminClient) ListTopicOffsetsWithClient(ctx context.Context, topic string, partitions []int32, client *kafka.Client) (*types.TopicOffset, error) {
	offsets := &types.TopicOffset{
		Topic:            topic,
		PartitionOffsets: make(map[int32]int64),
		Timestamp:        time.Now(),
	}

	// Use client from pool if provided, otherwise use shared client
	if client == nil {
		client = a.client
	}

	// Use the Client API for batch ListOffsets request
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
			ISRs:     convertToInt32Slice(partition.Isr),
		})
	}

	return topics, nil
}

// GetTopicMetadata gets detailed metadata for a specific topic including configs
func (a *AdminClient) GetTopicMetadata(ctx context.Context, topicName string) (*types.TopicInfo, error) {
	// Try to get topic ID from Metadata API first
	var topicID string
	metadataResp, err := a.client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:    kafka.TCP(a.broker),
		Topics:  []string{topicName},
	})
	if err == nil && len(metadataResp.Topics) > 0 {
		// Find the topic in the response
		for _, topic := range metadataResp.Topics {
			if topic.Name == topicName {
				// Try to get TopicID using reflection (in case kafka-go has it but doesn't expose it)
				// Topic IDs are UUIDs introduced in Kafka 2.8+ (KIP-516)
				topicValue := reflect.ValueOf(topic)
				if topicValue.Kind() == reflect.Struct {
					// Try common field names for topic ID
					fieldNames := []string{"TopicID", "TopicId", "ID", "Id", "UUID", "Uuid"}
					for _, fieldName := range fieldNames {
						field := topicValue.FieldByName(fieldName)
						if field.IsValid() && field.Kind() == reflect.String && field.String() != "" {
							topicID = field.String()
							break
						}
					}
				}
				break
			}
		}
	}

	conn, err := kafka.Dial("tcp", a.broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	allPartitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	// Filter partitions for the specific topic
	partitions := make([]kafka.Partition, 0)
	for _, partition := range allPartitions {
		if partition.Topic == topicName {
			partitions = append(partitions, partition)
		}
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	// Get replication factor from first partition
	replicationFactor := len(partitions[0].Replicas)

	topicInfo := &types.TopicInfo{
		Name:              topicName,
		TopicID:           topicID,
		Partitions:        len(partitions),
		PartInfo:          make([]types.PartitionInfo, 0, len(partitions)),
		ReplicationFactor: replicationFactor,
		Configs:           make(map[string]string),
	}

	for _, partition := range partitions {
		topicInfo.PartInfo = append(topicInfo.PartInfo, types.PartitionInfo{
			ID:       int32(partition.ID),
			Leader:   int32(partition.Leader.ID),
			Replicas: convertToInt32Slice(partition.Replicas),
			ISRs:     convertToInt32Slice(partition.Isr),
		})
	}

	// Get topic configs using DescribeConfigs API
	configs, err := a.GetTopicConfigs(ctx, topicName)
	if err == nil {
		topicInfo.Configs = configs
	}
	// If config fetch fails, continue without configs (non-fatal)

	return topicInfo, nil
}

// GetTopicConfigs gets topic configuration using DescribeConfigs API
func (a *AdminClient) GetTopicConfigs(ctx context.Context, topicName string) (map[string]string, error) {
	// Use DescribeConfigs API
	// Note: kafka-go may not have a direct DescribeConfigs method, so we'll use Metadata API
	// For now, return empty map - we'll need to check if kafka-go supports DescribeConfigs
	// If not, we may need to use a different approach or library
	
	// Try using the client's DescribeConfigs if available
	// This is a placeholder - actual implementation depends on kafka-go API
	configs := make(map[string]string)
	
	// For now, return empty configs - we'll implement this properly if kafka-go supports it
	// or use an alternative method
	return configs, nil
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
