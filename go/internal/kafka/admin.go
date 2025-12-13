package kafka

import (
	"context"
	"fmt"
	"os"
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

	// Create a pool of clients for parallel operations
	// Pool size matches typical maxConcurrent values (default 10, but allow up to 50)
	poolSize := 50
	clientPool := make(chan *kafka.Client, poolSize)
	for i := 0; i < poolSize; i++ {
		clientPool <- &kafka.Client{
			Addr:    kafka.TCP(broker),
			Timeout: 10 * time.Second,
		}
	}

	return &AdminClient{
		broker: broker,
		conn:   conn,
		client: &kafka.Client{
			Addr:    kafka.TCP(broker),
			Timeout: 10 * time.Second,
		},
		clientPool: clientPool,
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

				// Get a client from the pool for this goroutine
				client := <-a.clientPool
				defer func() { a.clientPool <- client }() // Return client to pool

				resp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
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
