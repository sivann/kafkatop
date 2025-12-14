package kafka

import (
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/sivann/kafkatop/internal/types"
)

// TimingStats holds timing information for profiling
type TimingStats struct {
	ListConsumerGroups    time.Duration
	DescribeConsumerGroups time.Duration
	ListGroupOffsets      time.Duration
	BuildTopicsMap        time.Duration
	ListTopicOffsets      time.Duration
	CalculateLags         time.Duration
	Total                 time.Duration
}

// CalcLag calculates lag statistics for all consumer groups
func CalcLag(ctx context.Context, admin *AdminClient, params *types.Params) (*types.KafkaData, error) {
	startTime := time.Now()
	stats := &TimingStats{}

	kd := &types.KafkaData{
		ConsumerGroups:   make(map[string]*types.ConsumerGroup),
		GroupOffsets:     make(map[string]*types.ConsumerGroupOffset),
		TopicsWithGroups: make(map[string]*types.TopicWithGroups),
		TopicOffsets:     make(map[string]*types.TopicOffset),
		GroupLags:        make(map[string]map[string]*types.LagStats),
	}

	// List consumer groups
	t0 := time.Now()
	groups, err := admin.ListConsumerGroups(ctx, params)
	stats.ListConsumerGroups = time.Since(t0)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	if len(groups) == 0 {
		// No groups found (possibly due to filters)
		kd.GroupOffsetsTS = time.Now()
		kd.TopicOffsetsTS = time.Now()
		return kd, nil
	}

	// Get group IDs
	groupIDs := make([]string, 0, len(groups))
	for groupID := range groups {
		groupIDs = append(groupIDs, groupID)
	}

	// Determine max concurrent for parallelization
	maxConcurrent := params.KafkaMaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1 // Default to sequential if invalid value
	}

	// Describe consumer groups to get topic assignments (parallelized if configured)
	t1 := time.Now()
	describedGroups, err := admin.DescribeConsumerGroups(ctx, groupIDs, maxConcurrent)
	stats.DescribeConsumerGroups = time.Since(t1)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer groups: %w", err)
	}

	// Update groups with description info and filter by state
	for groupID, group := range describedGroups {
		if !params.KafkaShowEmptyGroups && group.State == types.StateEmpty {
			delete(groups, groupID)
			continue
		}
		groups[groupID] = group
	}

	kd.ConsumerGroups = groups

	// Get consumer group offsets (parallelized if configured)
	t2 := time.Now()
	if maxConcurrent <= 1 {
		// Sequential execution
		for groupID := range groups {
			offsets, err := admin.ListConsumerGroupOffsetsWithClient(ctx, groupID, nil)
			if err != nil {
				// Skip groups with errors
				continue
			}
			kd.GroupOffsets[groupID] = offsets
		}
	} else {
		// Parallel execution with semaphore
		semaphore := make(chan struct{}, maxConcurrent)
		var wg sync.WaitGroup
		var mu sync.Mutex

		for groupID := range groups {
			wg.Add(1)
			go func(gid string) {
				defer wg.Done()
				semaphore <- struct{}{} // Acquire semaphore
				defer func() { <-semaphore }() // Release semaphore

				// Use shared client (thread-safe)
				offsets, err := admin.ListConsumerGroupOffsetsWithClient(ctx, gid, nil)
				if err != nil {
					// Skip groups with errors
					return
				}

				mu.Lock()
				kd.GroupOffsets[gid] = offsets
				mu.Unlock()
			}(groupID)
		}
		wg.Wait()
	}
	stats.ListGroupOffsets = time.Since(t2)
	kd.GroupOffsetsTS = time.Now()

	// Build topics with groups map
	// Union partitions from all groups that consume each topic
	t3 := time.Now()
	for groupID, offsets := range kd.GroupOffsets {
		if len(offsets.TopicOffsets) == 0 {
			continue
		}

		for topic, partOffsets := range offsets.TopicOffsets {
			if _, exists := kd.TopicsWithGroups[topic]; !exists {
				// First time seeing this topic - initialize with partitions from this group
				partitions := make([]int32, 0, len(partOffsets))
				for part := range partOffsets {
					partitions = append(partitions, part)
				}
				kd.TopicsWithGroups[topic] = &types.TopicWithGroups{
					Topic:      topic,
					Groups:     []string{},
					Partitions: partitions,
				}
			} else {
				// Topic already exists - union partitions from this group
				existingParts := make(map[int32]bool)
				for _, part := range kd.TopicsWithGroups[topic].Partitions {
					existingParts[part] = true
				}
				// Add any new partitions from this group
				for part := range partOffsets {
					if !existingParts[part] {
						kd.TopicsWithGroups[topic].Partitions = append(kd.TopicsWithGroups[topic].Partitions, part)
					}
				}
			}
			kd.TopicsWithGroups[topic].Groups = append(kd.TopicsWithGroups[topic].Groups, groupID)
		}
	}
	stats.BuildTopicsMap = time.Since(t3)

	// Get topic offsets (parallelized if configured)
	t4 := time.Now()
	if maxConcurrent <= 1 {
		// Sequential execution
		for topic, topicInfo := range kd.TopicsWithGroups {
			offsets, err := admin.ListTopicOffsetsWithClient(ctx, topic, topicInfo.Partitions, nil)
			if err != nil {
				// Skip topics with errors
				continue
			}
			kd.TopicOffsets[topic] = offsets
		}
	} else {
		// Parallel execution with semaphore
		semaphore := make(chan struct{}, maxConcurrent)
		var wg sync.WaitGroup
		var mu sync.Mutex

		for topic, topicInfo := range kd.TopicsWithGroups {
			wg.Add(1)
			go func(t string, ti *types.TopicWithGroups) {
				defer wg.Done()
				semaphore <- struct{}{} // Acquire semaphore
				defer func() { <-semaphore }() // Release semaphore

				// Use shared client (thread-safe)
				offsets, err := admin.ListTopicOffsetsWithClient(ctx, t, ti.Partitions, nil)
				if err != nil {
					// Skip topics with errors
					return
				}

				mu.Lock()
				kd.TopicOffsets[t] = offsets
				mu.Unlock()
			}(topic, topicInfo)
		}
		wg.Wait()
	}
	stats.ListTopicOffsets = time.Since(t4)
	kd.TopicOffsetsTS = time.Now()

	// Calculate lag statistics
	t5 := time.Now()
	for groupID, groupOffsets := range kd.GroupOffsets {
		if len(groupOffsets.TopicOffsets) == 0 {
			continue
		}

		kd.GroupLags[groupID] = make(map[string]*types.LagStats)

		for topic, partOffsets := range groupOffsets.TopicOffsets {
			topicOffsets, exists := kd.TopicOffsets[topic]
			if !exists {
				continue
			}

			lags := make([]int64, 0)
			partLags := make(map[int32]int64)

			for partition, topicOffset := range topicOffsets.PartitionOffsets {
				groupOffset, exists := partOffsets[partition]
				if !exists {
					// Partition not consumed by this group
					continue
				}

				lag := topicOffset - groupOffset
				if lag < 0 {
					lag = 0
				}
				partLags[partition] = lag
				lags = append(lags, lag)
			}

			if len(lags) == 0 {
				continue
			}

			stats := &types.LagStats{
				Topic:         topic,
				GroupID:       groupID,
				PartitionLags: partLags,
				Min:           minInt64(lags),
				Max:           maxInt64(lags),
				Mean:          meanInt64(lags),
				Median:        medianInt64(lags),
				Sum:           sumInt64(lags),
			}

			kd.GroupLags[groupID][topic] = stats
		}
	}
	stats.CalculateLags = time.Since(t5)
	stats.Total = time.Since(startTime)

	// Print timing stats if enabled
	if params.TimingOutput != nil {
		w := params.TimingOutput.(io.Writer)
		fmt.Fprintf(w, "CalcLag timing (concurrent=%d, params.KafkaMaxConcurrent=%d):\n", maxConcurrent, params.KafkaMaxConcurrent)
		fmt.Fprintf(w, "  ListConsumerGroups:     %v\n", stats.ListConsumerGroups)
		fmt.Fprintf(w, "  DescribeConsumerGroups: %v\n", stats.DescribeConsumerGroups)
		fmt.Fprintf(w, "  ListGroupOffsets:       %v\n", stats.ListGroupOffsets)
		fmt.Fprintf(w, "  BuildTopicsMap:         %v\n", stats.BuildTopicsMap)
		fmt.Fprintf(w, "  ListTopicOffsets:       %v\n", stats.ListTopicOffsets)
		fmt.Fprintf(w, "  CalculateLags:          %v\n", stats.CalculateLags)
		fmt.Fprintf(w, "  Total:                  %v\n", stats.Total)
		fmt.Fprintf(w, "\n")
	}

	return kd, nil
}

// CalcRate calculates consumption rates between two KafkaData snapshots
func CalcRate(kd1, kd2 *types.KafkaData, params *types.Params) map[string]map[string]*types.RateStats {
	rates := make(map[string]map[string]*types.RateStats)

	for groupID, group1Offsets := range kd1.GroupOffsets {
		group2Offsets, exists := kd2.GroupOffsets[groupID]
		if !exists {
			continue
		}

		rates[groupID] = make(map[string]*types.RateStats)

		for topic, part1Offsets := range group1Offsets.TopicOffsets {
			part2Offsets, exists := group2Offsets.TopicOffsets[topic]
			if !exists {
				continue
			}

			// Calculate events consumed
			offset1Sum := int64(0)
			offset2Sum := int64(0)
			for part, offset := range part1Offsets {
				offset1Sum += offset
				if o2, exists := part2Offsets[part]; exists {
					offset2Sum += o2
				}
			}

			eventsConsumed := offset2Sum - offset1Sum
			timeDelta := kd2.GroupOffsetsTS.Sub(kd1.GroupOffsetsTS).Seconds()

			if timeDelta == 0 {
				continue
			}

			eventsConsumptionRate := float64(eventsConsumed) / timeDelta

			// Calculate per-partition consumption rates
			partitionRates := make(map[int32]float64)
			for part, offset1 := range part1Offsets {
				if offset2, exists := part2Offsets[part]; exists {
					partitionConsumed := offset2 - offset1
					partitionRates[part] = float64(partitionConsumed) / timeDelta
				}
			}

			// Calculate events arrival rate
			// Match Python behavior exactly: sum(kd2['topic_offsets'][t].values()) - sum(kd1['topic_offsets'][t].values())
			// Python sums ALL partitions present in each dict, regardless of whether they match between snapshots
			topic1Offsets, exists1 := kd1.TopicOffsets[topic]
			topic2Offsets, exists2 := kd2.TopicOffsets[topic]

			eventsArrivalRate := 0.0
			if exists1 && exists2 {
				topic1Sum := int64(0)
				topic2Sum := int64(0)
				// Sum all partitions in each snapshot (matching Python behavior)
				for _, offset := range topic1Offsets.PartitionOffsets {
					topic1Sum += offset
				}
				for _, offset := range topic2Offsets.PartitionOffsets {
					topic2Sum += offset
				}
				eventsArrived := topic2Sum - topic1Sum
				eventsArrivalRate = float64(eventsArrived) / timeDelta
			}

			// Calculate remaining time
			var remainingSec int64
			remainingHMS := "-"

			if lag, exists := kd2.GroupLags[groupID][topic]; exists {
				etaMethod := params.ETACalculationMethod
				if etaMethod == "" {
					etaMethod = "net-rate" // Default to new method
				}

				if etaMethod == "net-rate" {
					// More accurate ETA: account for incoming rate
					// If consumption > arrival, lag decreases
					// If arrival > consumption, lag increases
					netRate := eventsConsumptionRate - eventsArrivalRate
					if netRate > 0 {
						// Consuming faster than arriving - lag will decrease
						remainingSec = int64(float64(lag.Sum) / netRate)
						remainingHMS = formatDuration(time.Duration(remainingSec) * time.Second)
					} else if eventsConsumptionRate > 0 {
						// Arriving faster than consuming - lag will increase
						// ETA is negative/infinite, show as "-"
						remainingSec = -1
					} else {
						// No consumption
						remainingSec = -1
					}
				} else {
					// Simple method: only use consumption rate (original behavior)
					if eventsConsumptionRate > 0 {
						remainingSec = int64(float64(lag.Sum) / eventsConsumptionRate)
						remainingHMS = formatDuration(time.Duration(remainingSec) * time.Second)
					} else {
						remainingSec = -1
					}
				}
			}

			rates[groupID][topic] = &types.RateStats{
				GroupID:               groupID,
				Topic:                 topic,
				EventsConsumed:        eventsConsumed,
				TimeDelta:             timeDelta,
				EventsConsumptionRate: eventsConsumptionRate,
				EventsArrivalRate:     eventsArrivalRate,
				RemainingSec:          remainingSec,
				RemainingHMS:          remainingHMS,
				PartitionRates:        partitionRates,
			}
		}
	}

	return rates
}

// ApplyRuntimeFilter applies a runtime filter to consumer groups
func ApplyRuntimeFilter(groups map[string]*types.ConsumerGroup, pattern string) map[string]*types.ConsumerGroup {
	if pattern == "" {
		return groups
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return groups
	}

	filtered := make(map[string]*types.ConsumerGroup)
	for groupID, group := range groups {
		if re.MatchString(groupID) {
			filtered[groupID] = group
		}
	}

	return filtered
}

// Helper functions for statistics

func minInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

func maxInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

func sumInt64(values []int64) int64 {
	sum := int64(0)
	for _, v := range values {
		sum += v
	}
	return sum
}

func meanInt64(values []int64) float64 {
	if len(values) == 0 {
		return 0
	}
	return float64(sumInt64(values)) / float64(len(values))
}

func medianInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "-"
	}

	hours := int(d.Hours())
	minutes := int(math.Mod(d.Minutes(), 60))
	seconds := int(math.Mod(d.Seconds(), 60))

	return fmt.Sprintf("%d:%02d:%02d", hours, minutes, seconds)
}
