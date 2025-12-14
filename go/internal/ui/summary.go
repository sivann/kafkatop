package ui

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/sivann/kafkatop/internal/kafka"
	"github.com/sivann/kafkatop/internal/types"
)

// ShowSummary displays a summary of consumer groups and their lag
func ShowSummary(admin *kafka.AdminClient, params *types.Params) error {
	ctx := context.Background()

	kd, err := kafka.CalcLag(ctx, admin, params)
	if err != nil {
		return fmt.Errorf("failed to calculate lag: %w", err)
	}

	// ANSI color codes (disabled if text mode)
	cyan := "\033[36m"
	green := "\033[32m"
	red := "\033[31m"
	reset := "\033[0m"
	bold := "\033[1m"

	if params.TextMode {
		cyan = ""
		green = ""
		red = ""
		reset = ""
		bold = ""
	}

	// Print title
	fmt.Printf("\n%s%sInitial Lag Summary%s\n\n", bold, cyan, reset)

	// Column widths
	const (
		groupWidth = 45
		topicWidth = 25
		partsWidth = 25
		lagWidth   = 18
		stateWidth = 30
	)

	// Print header row 1
	fmt.Printf("%s%-*s %-*s %*s %*s %-*s%s\n",
		green,
		groupWidth, "Consumer Group",
		topicWidth, "Topic",
		partsWidth, "Partitions",
		lagWidth, "Lag (part median)",
		stateWidth, "Consumer Group State",
		reset)

	// Print header row 2
	fmt.Printf("%s%-*s %-*s %*s %*s %-*s%s\n",
		green,
		groupWidth, "",
		topicWidth, "",
		partsWidth, "(with groups/total)",
		lagWidth, "",
		stateWidth, "",
		reset)

	// Header underline
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────")

	// Helper function to hash strings for anonymization
	hashString := func(s string) uint32 {
		h := fnv.New32a()
		h.Write([]byte(s))
		return h.Sum32()
	}

	// Print lag summary for each group
	for groupID, groupLags := range kd.GroupLags {
		state := kd.ConsumerGroups[groupID].State

		// Color state based on whether it's empty
		stateColor := green
		if state == types.StateEmpty {
			stateColor = red
		}

		// Apply anonymization if requested
		displayGroupID := groupID
		if params.Anonymize {
			displayGroupID = fmt.Sprintf("group %06d", hashString(groupID)%1000000)
		}

		for topic, lag := range groupLags {
			partsWithConsumers := len(lag.PartitionLags)

			// Get total partitions for this topic
			totalParts := partsWithConsumers
			if topicInfo, exists := kd.TopicsWithGroups[topic]; exists {
				totalParts = len(topicInfo.Partitions)
			}

			// Apply anonymization if requested
			displayTopic := topic
			if params.Anonymize {
				displayTopic = fmt.Sprintf("topic %06d", hashString(topic)%1000000)
			}

			// Format state like Python: "ConsumerGroupState.EMPTY"
			stateStr := fmt.Sprintf("ConsumerGroupState.%s", state)

			fmt.Printf("%s%-*s%s %s%-*s%s %*s %*d %s%-*s%s\n",
				cyan, groupWidth, displayGroupID, reset,
				cyan, topicWidth, displayTopic, reset,
				partsWidth, fmt.Sprintf("%d/%d", partsWithConsumers, totalParts),
				lagWidth, lag.Median,
				stateColor, stateWidth, stateStr, reset)
		}
	}

	fmt.Println()
	return nil
}
