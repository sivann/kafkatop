package ui

import (
	"context"
	"fmt"
	"time"

	"github.com/sivann/kafkatop/internal/kafka"
	"github.com/sivann/kafkatop/internal/types"
)

// ShowText displays lag information in plain text mode
func ShowText(admin *kafka.AdminClient, params *types.Params) error {
	ctx := context.Background()

	// Calculate initial lag
	kd, err := kafka.CalcLag(ctx, admin, params)
	if err != nil {
		return fmt.Errorf("failed to calculate lag: %w", err)
	}

	// Print header
	printSeparator()
	fmt.Printf("%-45s %-20s %-12s %-30s %-10s %-10s %-12s\n",
		"Group", "Topic", "Partitions", "State", "LAG min", "LAG max", "LAG median")
	printSeparator()

	// Print initial lag summary
	for groupID, groupLags := range kd.GroupLags {
		state := kd.ConsumerGroups[groupID].State
		for topic, lag := range groupLags {
			partsWithConsumers := len(lag.PartitionLags)
			fmt.Printf("%-45s %-20s %-12d %-30s %-10d %-10d %-12d\n",
				groupID, topic, partsWithConsumers, state,
				lag.Min, lag.Max, lag.Median)
		}
	}

	if params.KafkaPollIterations == 0 {
		return nil
	}

	time.Sleep(time.Duration(params.KafkaPollPeriod) * time.Second)
	kd1 := kd
	iteration := 0

	for {
		fmt.Println()
		iteration++

		// Print rate header
		printSeparator()
		fmt.Printf("%-45s %-8s %-6s %-12s %-12s %-10s\n",
			"Group", "Consumed", "Time", "Cons Rate", "Arr Rate", "Remaining")
		printSeparator()

		kd2, err := kafka.CalcLag(ctx, admin, params)
		if err != nil {
			return fmt.Errorf("failed to calculate lag: %w", err)
		}

		rates := kafka.CalcRate(kd1, kd2, params)
		kd1 = kd2

		for groupID, topicRates := range rates {
			for _, rate := range topicRates {
				fmt.Printf("%-45s %-8d %-6.1fs %-12.1f %-12.1f %-10s\n",
					groupID,
					rate.EventsConsumed,
					rate.TimeDelta,
					rate.EventsConsumptionRate,
					rate.EventsArrivalRate,
					rate.RemainingHMS)
			}
		}

		if iteration == params.KafkaPollIterations && params.KafkaPollIterations > 0 {
			break
		}

		time.Sleep(time.Duration(params.KafkaPollPeriod) * time.Second)
	}

	return nil
}

func printSeparator() {
	fmt.Println("============================================================================================================================")
}
