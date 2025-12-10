package ui

import (
	"context"
	"fmt"

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

	// Print header
	printSeparator()
	fmt.Printf("%-45s %-20s %-12s %-30s %-10s %-10s %-12s\n",
		"Consumer Group", "Topic", "Partitions", "State", "LAG min", "LAG max", "LAG median")
	printSeparator()

	// Print lag summary for each group
	for groupID, groupLags := range kd.GroupLags {
		state := kd.ConsumerGroups[groupID].State
		for topic, lag := range groupLags {
			partsWithConsumers := len(lag.PartitionLags)
			fmt.Printf("%-45s %-20s %-12d %-30s %-10d %-10d %-12d\n",
				groupID, topic, partsWithConsumers, state,
				lag.Min, lag.Max, lag.Median)
		}
	}

	fmt.Println()
	return nil
}
