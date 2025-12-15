package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sivann/kafkatop/internal/types"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// AdminClient wraps Kafka admin operations
type AdminClient struct {
	broker               string
	conn                 *kafka.Conn
	client               *kafka.Client      // Shared client for API calls
	clientPool           chan *kafka.Client // Pool of clients for parallel operations
	franzClient          *kgo.Client        // franz-go client for DescribeConfigs API
	franzAdmin           *kadm.Client       // franz-go admin client
	useInitialBrokerOnly bool               // If true, force all operations to use initial broker
	debug                bool               // Enable debug output
}

// dnsMapResolver is a custom resolver that uses DNS mappings
type dnsMapResolver struct {
	dnsMap map[string]string
	debug  bool
}

func (r *dnsMapResolver) LookupBrokerIPAddr(ctx context.Context, broker kafka.Broker) ([]net.IPAddr, error) {
	// Check if we have a custom mapping for this broker's host
	if ipStr, exists := r.dnsMap[broker.Host]; exists {
		ip := net.ParseIP(ipStr)
		if ip != nil {
			if r.debug {
				fmt.Fprintf(os.Stderr, "DEBUG DNS Resolver: Mapped %s -> %s\n", broker.Host, ipStr)
			}
			return []net.IPAddr{{IP: ip}}, nil
		}
		if r.debug {
			fmt.Fprintf(os.Stderr, "DEBUG DNS Resolver: Mapping exists for %s but IP %s is invalid\n", broker.Host, ipStr)
		}
	}
	// Fall back to normal DNS resolution
	if r.debug {
		fmt.Fprintf(os.Stderr, "DEBUG DNS Resolver: No mapping for %s, using normal DNS\n", broker.Host)
	}
	resolver := &net.Resolver{}
	addrs, err := resolver.LookupIPAddr(ctx, broker.Host)
	if err != nil {
		return nil, err
	}
	return addrs, nil
}

// NewAdminClient creates a new Kafka admin client
// When useInitialBrokerOnly is true, all connections are redirected to the initial broker.
// This works well for single-node Kafka deployments accessed via port forwarding.
// For multi-node clusters, some operations (like OffsetFetch to coordinators) may fail
// because coordinators may not be accessible through the port-forwarded broker.
// When dnsMap is provided, custom DNS resolution is used to map hostnames to IPs.
// When debug is true, debug output is printed to stderr.
func NewAdminClient(broker string, useInitialBrokerOnly bool, dnsMap map[string]string, debug bool) (*AdminClient, error) {
	// Add default port if not specified
	if matched, _ := regexp.MatchString(`:\d+$`, broker); !matched {
		broker = broker + ":9092"
	}

	// Test connection
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %w", broker, err)
	}

	// Create a custom resolver if DNS mappings are provided
	var customResolver kafka.BrokerResolver
	if len(dnsMap) > 0 {
		customResolver = &dnsMapResolver{dnsMap: dnsMap, debug: debug}
		if debug {
			fmt.Fprintf(os.Stderr, "DEBUG: Using custom DNS resolver with %d mappings\n", len(dnsMap))
		}
	}

	// Create a single shared client for all operations
	// kafka.Client is thread-safe and can handle concurrent requests efficiently
	var sharedClient *kafka.Client
	if useInitialBrokerOnly {
		// Parse the initial broker address
		brokerHost := broker
		brokerPort := "9092"
		if idx := strings.LastIndex(broker, ":"); idx >= 0 {
			brokerHost = broker[:idx]
			brokerPort = broker[idx+1:]
		}

		// Custom dialer that always connects to the initial broker when useInitialBrokerOnly is true
		// When DNS mapping is provided, it's handled by the Resolver instead
		customDial := func(ctx context.Context, network, address string) (net.Conn, error) {
			// Always connect to the initial broker, ignoring the requested address
			return (&net.Dialer{
				Timeout:   3 * time.Second,
				DualStack: true,
			}).DialContext(ctx, network, brokerHost+":"+brokerPort)
		}

		// Create transport with custom dialer and resolver
		// The Transport will still create pools per broker ID, but all connections
		// will go to the same broker via the custom Dial function
		// Set MetadataTTL to 0 to disable caching and always get fresh metadata
		// Leave MetadataTopics empty to get metadata for all topics
		customTransport := &kafka.Transport{
			Dial:           customDial,
			DialTimeout:    5 * time.Second,
			IdleTimeout:    30 * time.Second,
			MetadataTTL:    0,   // Disable metadata caching to ensure fresh data
			MetadataTopics: nil, // nil/empty means get metadata for all topics
		}
		if customResolver != nil {
			customTransport.Resolver = customResolver
		}

		sharedClient = &kafka.Client{
			Addr:      kafka.TCP(broker),
			Timeout:   10 * time.Second,
			Transport: customTransport,
		}
	} else {
		transport := &kafka.Transport{}
		if customResolver != nil {
			transport.Resolver = customResolver
		}
		sharedClient = &kafka.Client{
			Addr:      kafka.TCP(broker),
			Timeout:   10 * time.Second,
			Transport: transport,
		}
	}

	// Create franz-go client for DescribeConfigs API (pure Go, no cgo)
	franzOpts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.RequestTimeoutOverhead(10 * time.Second),
	}

	// Create custom dialer for franz-go that handles DNS mapping and/or initial broker only
	if useInitialBrokerOnly || len(dnsMap) > 0 {
		brokerHost := broker
		brokerPort := "9092"
		if idx := strings.LastIndex(broker, ":"); idx >= 0 {
			brokerHost = broker[:idx]
			brokerPort = broker[idx+1:]
		}

		franzOpts = append(franzOpts, kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
			if useInitialBrokerOnly {
				// Always connect to the initial broker
				return (&net.Dialer{}).DialContext(ctx, network, brokerHost+":"+brokerPort)
			}

			// Use DNS mapping if available (only when not using initial broker only)
			if len(dnsMap) > 0 {
				// Parse address (format: host:port)
				host, port, err := net.SplitHostPort(address)
				if err == nil {
					if mappedIP, exists := dnsMap[host]; exists {
						// Use mapped IP instead
						address = net.JoinHostPort(mappedIP, port)
					}
				}
			}

			return (&net.Dialer{}).DialContext(ctx, network, address)
		}))
	}

	franzClient, err := kgo.NewClient(franzOpts...)
	if err != nil {
		// If franz-go client creation fails, continue without it (non-fatal)
		// We'll just return empty configs
		franzClient = nil
	}

	var franzAdmin *kadm.Client
	if franzClient != nil {
		franzAdmin = kadm.NewClient(franzClient)
	}

	return &AdminClient{
		broker:               broker,
		conn:                 conn,
		client:               sharedClient,
		clientPool:           nil, // No longer using a pool
		franzClient:          franzClient,
		franzAdmin:           franzAdmin,
		useInitialBrokerOnly: useInitialBrokerOnly,
		debug:                debug,
	}, nil
}

// Close closes the admin client connection
func (a *AdminClient) Close() error {
	var err error
	if a.conn != nil {
		if closeErr := a.conn.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if a.franzClient != nil {
		a.franzClient.Close()
	}
	return err
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

	// Debug: log how many groups we got
	if a.debug {
		fmt.Fprintf(os.Stderr, "DEBUG ListConsumerGroups: Got %d groups from ListGroups\n", len(response.Groups))
	}

	groups := make(map[string]*types.ConsumerGroup)

	var filterPattern, excludePattern *regexp.Regexp
	if params.KafkaGroupFilterPattern != "" {
		filterPattern = regexp.MustCompile(params.KafkaGroupFilterPattern)
	}
	if params.KafkaGroupExcludePattern != "" {
		excludePattern = regexp.MustCompile(params.KafkaGroupExcludePattern)
	}

	matchedCount := 0
	excludedCount := 0
	duplicateCount := 0
	sampleGroups := make([]string, 0, 10)

	for _, group := range response.Groups {
		groupID := group.GroupID

		// Apply filters
		if filterPattern != nil && !filterPattern.MatchString(groupID) {
			continue
		}
		if excludePattern != nil && excludePattern.MatchString(groupID) {
			excludedCount++
			continue
		}

		// Check if this group already exists (duplicate)
		if _, exists := groups[groupID]; exists {
			duplicateCount++
			continue
		}

		matchedCount++
		if len(sampleGroups) < 10 {
			sampleGroups = append(sampleGroups, groupID)
		}

		groups[groupID] = &types.ConsumerGroup{
			GroupID: groupID,
			State:   types.StateStable,
			Topics:  make(map[string][]int32),
		}
	}

	if a.debug {
		fmt.Fprintf(os.Stderr, "DEBUG ListConsumerGroups: Pattern matched %d unique groups (excluded %d, duplicates skipped %d). Sample: %v\n",
			matchedCount, excludedCount, duplicateCount, sampleGroups)
		fmt.Fprintf(os.Stderr, "DEBUG ListConsumerGroups: Actually returning %d groups from map\n", len(groups))
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
				semaphore <- struct{}{}        // Acquire semaphore
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
	// If useInitialBrokerOnly is enabled, explicitly specify the broker address
	// to ensure the request goes through the initial broker (which can forward to coordinators)
	req := &kafka.OffsetFetchRequest{
		GroupID: groupID,
	}
	if a.useInitialBrokerOnly {
		req.Addr = kafka.TCP(a.broker)
	}
	response, err := client.OffsetFetch(ctx, req)
	if err != nil {
		// Debug: log errors
		if a.debug {
			fmt.Fprintf(os.Stderr, "DEBUG OffsetFetch: Failed for group %s: %v\n", groupID, err)
		}
		return nil, fmt.Errorf("failed to fetch offsets for group %s: %w", groupID, err)
	}

	// Debug: check if response is empty
	if a.debug && len(response.Topics) == 0 {
		fmt.Fprintf(os.Stderr, "DEBUG OffsetFetch: Group %s returned empty topics\n", groupID)
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

	// Debug: log if we got incomplete results
	if a.useInitialBrokerOnly {
		if topicOffsets, ok := response.Topics[topic]; ok {
			if a.debug {
				fmt.Fprintf(os.Stderr, "DEBUG ListTopicOffsets: Topic %s requested %d partitions, got %d\n", topic, len(partitions), len(topicOffsets))
			}
		}
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
	// Use the client's Metadata API which uses the custom Transport when useInitialBrokerOnly is enabled
	// Request metadata for all topics by passing nil Topics
	metadataResp, err := a.client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   kafka.TCP(a.broker),
		Topics: nil, // nil means get metadata for all topics
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	topics := make(map[string]*types.TopicInfo)

	// Debug: log how many topics we got from metadata (only if useInitialBrokerOnly)
	if a.useInitialBrokerOnly {
		fmt.Fprintf(os.Stderr, "DEBUG ListTopics: Metadata API returned %d topics\n", len(metadataResp.Topics))
	}

	// Build topics map from metadata response
	for _, topic := range metadataResp.Topics {
		if topic.Error != nil {
			// Log but continue - some topics might have errors
			fmt.Fprintf(os.Stderr, "DEBUG: Topic %s has error: %v\n", topic.Name, topic.Error)
			continue
		}

		topicName := topic.Name
		if _, exists := topics[topicName]; !exists {
			topics[topicName] = &types.TopicInfo{
				Name:       topicName,
				Partitions: 0,
				PartInfo:   []types.PartitionInfo{},
			}
		}

		// Process partitions for this topic
		for _, partition := range topic.Partitions {
			if partition.Error != nil {
				// Skip partitions with errors
				continue
			}

			topics[topicName].Partitions++
			topics[topicName].PartInfo = append(topics[topicName].PartInfo, types.PartitionInfo{
				ID:       int32(partition.ID),
				Leader:   int32(partition.Leader.ID),
				Replicas: convertToInt32Slice(partition.Replicas),
				ISRs:     convertToInt32Slice(partition.Isr),
			})
		}
	}

	return topics, nil
}

// GetTopicMetadata gets detailed metadata for a specific topic including configs
func (a *AdminClient) GetTopicMetadata(ctx context.Context, topicName string) (*types.TopicInfo, error) {
	// Try to get topic ID from Metadata API first
	var topicID string
	metadataResp, err := a.client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   kafka.TCP(a.broker),
		Topics: []string{topicName},
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

// GetTopicConfigs gets topic configuration using DescribeConfigs API via franz-go
func (a *AdminClient) GetTopicConfigs(ctx context.Context, topicName string) (map[string]string, error) {
	configs := make(map[string]string)

	// Use franz-go admin client if available
	if a.franzAdmin == nil {
		// franz-go client not available, return empty configs
		return configs, nil
	}

	// Use franz-go's DescribeTopicConfigs API
	describeResp, err := a.franzAdmin.DescribeTopicConfigs(ctx, topicName)
	if err != nil {
		// Return empty configs on error (non-fatal)
		return configs, nil
	}

	// Extract configs from response
	for _, topicConfig := range describeResp {
		if topicConfig.Err != nil {
			continue
		}
		for _, config := range topicConfig.Configs {
			// Include all configs (can filter sensitive/default ones later if needed)
			// Use MaybeValue() which returns empty string for sensitive configs
			value := config.MaybeValue()
			if value != "" {
				configs[config.Key] = value
			}
		}
	}

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
