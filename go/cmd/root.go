package cmd

import (
	"fmt"
	"regexp"

	"github.com/sivann/kafkatop/internal/kafka"
	"github.com/sivann/kafkatop/internal/types"
	"github.com/sivann/kafkatop/internal/ui"
	"github.com/spf13/cobra"
)

var (
	params  types.Params
	version string
)

var rootCmd = &cobra.Command{
	Use:   "kafkatop",
	Short: "A real-time monitoring tool for Apache Kafka",
	Long: `kafkatop provides a simple, yet powerful, way to quickly view the health
of your Kafka consumers and topics. It helps you identify bottlenecks and
diagnose issues with consumer lag in real-time, directly from your terminal.`,
	RunE: run,
}

func Execute(ver string) error {
	version = ver
	return rootCmd.Execute()
}

func init() {
	rootCmd.Flags().StringVar(&params.KafkaBroker, "kafka-broker", "localhost:9092",
		"Broker address (host:port)")
	rootCmd.Flags().BoolVar(&params.TextMode, "text", false,
		"Disable rich text and color")
	rootCmd.Flags().IntVar(&params.KafkaPollPeriod, "poll-period", 5,
		"Poll interval (sec) for rate calculations")
	rootCmd.Flags().IntVar(&params.KafkaPollIterations, "poll-iterations", 15,
		"Refresh count before exiting (-1 for infinite)")
	rootCmd.Flags().StringVar(&params.KafkaGroupExcludePattern, "group-exclude-pattern", "",
		"Exclude groups matching regex")
	rootCmd.Flags().StringVar(&params.KafkaGroupFilterPattern, "group-filter-pattern", "",
		"Filter groups by regex")
	rootCmd.Flags().BoolVar(&params.KafkaStatus, "status", false,
		"Report health as JSON and exit")
	rootCmd.Flags().BoolVar(&params.KafkaSummary, "summary", false,
		"Display consumer groups, states, topics, partitions, and lags summary")
	rootCmd.Flags().BoolVar(&params.KafkaSummaryJSON, "summary-json", false,
		"Display consumer groups, states, topics, partitions, and lags summary in JSON and exit")
	rootCmd.Flags().BoolVar(&params.KafkaTopicInfo, "topicinfo", false,
		"Show topic metadata only (fast)")
	rootCmd.Flags().BoolVar(&params.KafkaTopicInfoParts, "topicinfo-parts", false,
		"Show topic and partition metadata")
	rootCmd.Flags().BoolVar(&params.KafkaOnlyIssues, "only-issues", false,
		"Show only groups with high lag/issues")
	rootCmd.Flags().BoolVar(&params.Anonymize, "anonymize", false,
		"Anonymize topic and group names")
	rootCmd.Flags().BoolVar(&params.KafkaShowEmptyGroups, "all", false,
		"Show all groups (including those with no members)")

	// Set version
	rootCmd.Version = version
	rootCmd.SetVersionTemplate("{{.Version}}\n")
}

func run(cmd *cobra.Command, args []string) error {
	// Validate regex patterns
	if params.KafkaGroupExcludePattern != "" {
		if _, err := regexp.Compile(params.KafkaGroupExcludePattern); err != nil {
			return fmt.Errorf("invalid exclude pattern: %w", err)
		}
	}
	if params.KafkaGroupFilterPattern != "" {
		if _, err := regexp.Compile(params.KafkaGroupFilterPattern); err != nil {
			return fmt.Errorf("invalid filter pattern: %w", err)
		}
	}

	// Create Kafka admin client
	admin, err := kafka.NewAdminClient(params.KafkaBroker)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	// Handle different modes
	if params.KafkaTopicInfo || params.KafkaTopicInfoParts {
		return ui.ShowTopicInfo(admin, &params)
	}

	if params.KafkaStatus {
		return ui.ShowStatus(admin, &params)
	}

	if params.KafkaSummaryJSON {
		return ui.ShowSummaryJSON(admin, &params)
	}

	if params.TextMode {
		return ui.ShowText(admin, &params)
	}

	// Default: show rich UI
	return ui.ShowRich(admin, &params)
}
