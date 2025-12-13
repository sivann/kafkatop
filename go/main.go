package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"

	"github.com/sivann/kafkatop/internal/kafka"
	"github.com/sivann/kafkatop/internal/types"
	"github.com/sivann/kafkatop/internal/ui"
)

const version = "1.16-go"

func main() {
	// Parse command line flags
	params := types.Params{}

	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.StringVar(&params.KafkaBroker, "kafka-broker", "localhost:9092", "Broker address (host:port)")
	flag.BoolVar(&params.TextMode, "text", false, "Disable rich text and color")
	flag.IntVar(&params.KafkaPollPeriod, "poll-period", 5, "Poll interval (sec) for rate calculations")
	flag.IntVar(&params.KafkaPollIterations, "poll-iterations", 15, "Refresh count before exiting (-1 for infinite)")
	flag.StringVar(&params.KafkaGroupExcludePattern, "group-exclude-pattern", "", "Exclude groups matching regex")
	flag.StringVar(&params.KafkaGroupFilterPattern, "group-filter-pattern", "", "Filter groups by regex")
	flag.BoolVar(&params.KafkaStatus, "status", false, "Report health as JSON and exit")
	flag.BoolVar(&params.KafkaSummary, "summary", false, "Display consumer groups, states, topics, partitions, and lags summary")
	flag.BoolVar(&params.KafkaSummaryJSON, "summary-json", false, "Display consumer groups, states, topics, partitions, and lags summary in JSON and exit")
	flag.BoolVar(&params.KafkaTopicInfo, "topicinfo", false, "Show topic metadata only (fast)")
	flag.BoolVar(&params.KafkaTopicInfoParts, "topicinfo-parts", false, "Show topic and partition metadata")
	flag.BoolVar(&params.KafkaOnlyIssues, "only-issues", false, "Show only groups with high lag/issues")
	flag.BoolVar(&params.Anonymize, "anonymize", false, "Anonymize topic and group names")
	flag.BoolVar(&params.KafkaShowEmptyGroups, "all", false, "Show all groups (including those with no members)")
	flag.StringVar(&params.ETACalculationMethod, "eta-method", "net-rate", "ETA calculation method: 'simple' (consumption rate only) or 'net-rate' (accounts for incoming rate)")
	flag.IntVar(&params.KafkaMaxConcurrent, "max-concurrent", 10, "Max concurrent API calls for lag calculation (0 or 1 = sequential, >1 = parallel)")
	showTiming := flag.Bool("timing", false, "Show timing/profiling information for lag calculation and exit")

	flag.Parse()

	// Enable timing output if requested
	if *showTiming {
		params.TimingOutput = os.Stderr
	}

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	// Validate regex patterns
	if params.KafkaGroupExcludePattern != "" {
		if _, err := regexp.Compile(params.KafkaGroupExcludePattern); err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid exclude pattern: %v\n", err)
			os.Exit(1)
		}
	}
	if params.KafkaGroupFilterPattern != "" {
		if _, err := regexp.Compile(params.KafkaGroupFilterPattern); err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid filter pattern: %v\n", err)
			os.Exit(1)
		}
	}

	// Create Kafka admin client
	admin, err := kafka.NewAdminClient(params.KafkaBroker)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create admin client: %v\n", err)
		os.Exit(1)
	}
	defer admin.Close()

	// Handle different modes
	if params.KafkaTopicInfo || params.KafkaTopicInfoParts {
		if err := ui.ShowTopicInfo(admin, &params); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if params.KafkaSummary {
		if err := ui.ShowSummary(admin, &params); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if params.KafkaStatus {
		if err := ui.ShowStatus(admin, &params); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if params.KafkaSummaryJSON {
		if err := ui.ShowSummaryJSON(admin, &params); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if params.TextMode {
		if err := ui.ShowText(admin, &params); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Default: show rich UI
	if err := ui.ShowRich(admin, &params); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
