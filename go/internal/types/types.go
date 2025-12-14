package types

import (
	"time"
)

// ConsumerGroupState represents the state of a consumer group
type ConsumerGroupState string

const (
	StateUnknown      ConsumerGroupState = "UNKNOWN"
	StatePreparingRebalance ConsumerGroupState = "PREPARING_REBALANCE"
	StateCompletingRebalance ConsumerGroupState = "COMPLETING_REBALANCE"
	StateStable       ConsumerGroupState = "STABLE"
	StateDead         ConsumerGroupState = "DEAD"
	StateEmpty        ConsumerGroupState = "EMPTY"
)

// ConsumerGroup represents a Kafka consumer group
type ConsumerGroup struct {
	GroupID      string
	State        ConsumerGroupState
	IsSimple     bool
	Topics       map[string][]int32 // topic -> partitions
}

// TopicPartition represents a topic and partition
type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

// ConsumerGroupOffset represents consumer group offsets
type ConsumerGroupOffset struct {
	GroupID        string
	TopicOffsets   map[string]map[int32]int64 // topic -> partition -> offset
	Timestamp      time.Time
}

// TopicOffset represents topic partition offsets
type TopicOffset struct {
	Topic          string
	PartitionOffsets map[int32]int64 // partition -> offset
	Timestamp      time.Time
}

// LagStats represents lag statistics for a consumer group on a topic
type LagStats struct {
	Topic        string
	GroupID      string
	PartitionLags map[int32]int64 // partition -> lag
	Min          int64
	Max          int64
	Mean         float64
	Median       int64
	Sum          int64
	PAR          float64 // Peak-to-Average Ratio: max lag / mean lag
	Cv           float64 // Coefficient of Variation: stdev / mean
}

// RateStats represents consumption rate statistics
type RateStats struct {
	GroupID               string
	Topic                 string
	EventsConsumed        int64
	TimeDelta             float64
	EventsConsumptionRate float64
	EventsArrivalRate     float64
	RemainingSec          int64
	RemainingHMS          string
	PartitionRates        map[int32]float64 // partition -> consumption rate (evts/sec)
}

// KafkaData holds all Kafka-related data
type KafkaData struct {
	ConsumerGroups    map[string]*ConsumerGroup
	GroupOffsets      map[string]*ConsumerGroupOffset
	GroupOffsetsTS    time.Time
	TopicsWithGroups  map[string]*TopicWithGroups
	TopicOffsets      map[string]*TopicOffset
	TopicOffsetsTS    time.Time
	GroupLags         map[string]map[string]*LagStats // groupID -> topic -> stats
}

// TopicWithGroups represents a topic and its associated consumer groups
type TopicWithGroups struct {
	Topic      string
	Groups     []string
	Partitions []int32
}

// HealthStatus represents the health status of a consumer group/topic
type HealthStatus struct {
	Status string
	Reason string
}

// HealthColors represents color styling for health status
type HealthColors struct {
	ETA  string
	Rate string
	Lag  string
}

// Params holds configuration parameters
type Params struct {
	KafkaBroker             string
	KafkaGroupExcludePattern string
	KafkaGroupFilterPattern  string
	KafkaPollPeriod         int
	KafkaPollIterations     int
	KafkaSummary            bool
	KafkaSummaryJSON        bool
	KafkaShowEmptyGroups    bool
	KafkaOnlyIssues         bool
	KafkaTopicInfo          bool
	KafkaTopicInfoParts     bool
	KafkaStatus             bool
	TextMode                bool
	Anonymize               bool
	ETACalculationMethod    string            // "simple" (old) or "net-rate" (new, accounts for incoming rate)
	KafkaMaxConcurrent      int               // Max concurrent API calls for lag calculation (0 or 1 = sequential, >1 = parallel)
	TimingOutput            interface{}       // Output for timing/profiling information (io.Writer, nil = disabled)
	UseInitialBrokerOnly    bool              // Use only initial broker address, ignore advertised addresses (for port forwarding)
	DNSMap                  map[string]string // Custom DNS mappings: hostname -> IP address
}

// ClusterInfo represents Kafka cluster information
type ClusterInfo struct {
	Topics      map[string]*TopicInfo
	Brokers     map[int32]*BrokerInfo
	BrokerName  string
	ClusterID   string
}

// TopicInfo represents information about a Kafka topic
type TopicInfo struct {
	Name              string
	TopicID           string // Topic ID (UUID) - empty if not available
	Partitions        int
	PartInfo          []PartitionInfo
	ReplicationFactor int
	Configs           map[string]string // topic configs
}

// PartitionInfo represents information about a partition
type PartitionInfo struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISRs     []int32 // ISR broker IDs (not just count)
}

// BrokerInfo represents information about a broker
type BrokerInfo struct {
	ID   int32
	Host string
	Port int32
}
