package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sivann/kafkatop/internal/kafka"
	"github.com/sivann/kafkatop/internal/types"
)

// ShowSummaryJSON displays summary information in JSON format
func ShowSummaryJSON(admin *kafka.AdminClient, params *types.Params) error {
	ctx := context.Background()

	kd, err := kafka.CalcLag(ctx, admin, params)
	if err != nil {
		return fmt.Errorf("failed to calculate lag: %w", err)
	}

	summary := make(map[string]interface{})

	for groupID, groupLags := range kd.GroupLags {
		state := kd.ConsumerGroups[groupID].State
		groupData := map[string]interface{}{
			"state":  string(state),
			"topics": make(map[string]interface{}),
		}

		for topic, lag := range groupLags {
			topicData := map[string]interface{}{
				"partitions": len(lag.PartitionLags),
				"lag_max":    lag.Max,
				"lag_min":    lag.Min,
			}
			groupData["topics"].(map[string]interface{})[topic] = topicData
		}

		summary[groupID] = groupData
	}

	output, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

// ShowStatus displays health status in JSON format
func ShowStatus(admin *kafka.AdminClient, params *types.Params) error {
	ctx := context.Background()

	kd1, err := kafka.CalcLag(ctx, admin, params)
	if err != nil {
		return fmt.Errorf("failed to calculate lag: %w", err)
	}

	time.Sleep(time.Duration(params.KafkaPollPeriod) * time.Second)

	kd2, err := kafka.CalcLag(ctx, admin, params)
	if err != nil {
		return fmt.Errorf("failed to calculate lag: %w", err)
	}

	rates := kafka.CalcRate(kd1, kd2, params)

	status := make(map[string]interface{})

	for groupID, topicRates := range rates {
		for topic, rate := range topicRates {
			lag := kd2.GroupLags[groupID][topic]
			healthStatus := calculateHealthStatus(groupID, lag.Sum, rate)

			key := fmt.Sprintf("%s-%s", groupID, topic)
			status[key] = healthStatus
		}
	}

	output, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

// ShowTopicInfo displays topic information in JSON format
func ShowTopicInfo(admin *kafka.AdminClient, params *types.Params) error {
	ctx := context.Background()

	clusterInfo, err := admin.GetClusterInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %w", err)
	}

	info := map[string]interface{}{
		"topics":      make(map[string]interface{}),
		"brokers":     make(map[int32]interface{}),
		"broker_name": clusterInfo.BrokerName,
		"cluster_id":  clusterInfo.ClusterID,
	}

	for topicName, topic := range clusterInfo.Topics {
		topicData := map[string]interface{}{
			"name":       topic.Name,
			"partitions": topic.Partitions,
		}

		if params.KafkaTopicInfoParts {
			partInfo := make([]map[string]interface{}, len(topic.PartInfo))
			for i, part := range topic.PartInfo {
				partInfo[i] = map[string]interface{}{
					"id":       part.ID,
					"leader":   part.Leader,
					"replicas": part.Replicas,
					"nisrs":    part.ISRs,
				}
			}
			topicData["partinfo"] = partInfo
		}

		info["topics"].(map[string]interface{})[topicName] = topicData
	}

	for brokerID, broker := range clusterInfo.Brokers {
		info["brokers"].(map[int32]interface{})[brokerID] = map[string]interface{}{
			"id":   broker.ID,
			"host": broker.Host,
			"port": broker.Port,
		}
	}

	output, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

func calculateHealthStatus(groupID string, lag int64, rate *types.RateStats) map[string]interface{} {
	status := make(map[string]interface{})

	// ETA status
	rs := rate.RemainingSec
	etaStatus := map[string]string{}

	if rs < 60 {
		etaStatus["status"] = "OK"
		etaStatus["reason"] = "ETA < 1m"
	} else if rs < 120 {
		etaStatus["status"] = "OK"
		etaStatus["reason"] = "ETA < 2m"
	} else if rs < 600 {
		etaStatus["status"] = "WARNING"
		etaStatus["reason"] = "ETA > 2m"
	} else if rs < 7200 {
		etaStatus["status"] = "ERROR"
		etaStatus["reason"] = "ETA > 10m"
	} else {
		etaStatus["status"] = "CRITICAL"
		etaStatus["reason"] = "ETA > 2h"
	}

	// Rate status
	rateStatus := map[string]string{
		"status": "OK",
		"reason": "",
	}

	if lag > 0 && rate.EventsConsumptionRate == 0 {
		rateStatus["status"] = "ERROR"
		rateStatus["reason"] = "Lag detected but no event consumption"
	} else if rate.EventsArrivalRate > 5*rate.EventsConsumptionRate {
		rateStatus["status"] = "ERROR"
		rateStatus["reason"] = "Arrival rate > 5 * consumption rate"
	} else if rate.EventsArrivalRate > 2*rate.EventsConsumptionRate {
		rateStatus["status"] = "WARNING"
		rateStatus["reason"] = "Arrival rate > 2 * consumption rate"
	}

	// Lag status
	lagStatus := map[string]string{
		"status": "OK",
		"reason": "",
	}

	status["eta"] = etaStatus
	status["rate"] = rateStatus
	status["lag"] = lagStatus

	return status
}
