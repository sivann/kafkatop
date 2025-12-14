package ui

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sivann/kafkatop/internal/kafka"
	"github.com/sivann/kafkatop/internal/types"
)

// ANSI color codes
const (
	colorReset   = "\033[0m"
	colorGreen   = "\033[32m"
	colorBrightGreen = "\033[1;32m"
	colorYellow  = "\033[33m"
	colorCyan    = "\033[36m"
	colorRed     = "\033[31m"
	colorMagenta = "\033[35m"
	colorWhite   = "\033[37m"
	colorBrightWhite = "\033[1;37m"
	colorBold    = "\033[1m"
	colorReverse = "\033[7m"
)

var (
	baseStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240"))

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("86"))

	selectedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("229")).
			Background(lipgloss.Color("57")).
			Bold(true)
)

type model struct {
	admin           *kafka.AdminClient
	params          *types.Params
	kd              *types.KafkaData
	rates           map[string]map[string]*types.RateStats
	iteration       int
	loading         bool
	loadingStatus   string
	spinner         spinner.Model
	table           table.Model
	sortKey         string
	sortReverse     bool
	filterInput     textinput.Model
	showFilter      bool
	filterPattern   string
	topicFilterInput textinput.Model
	showTopicFilter bool
	topicFilterPattern string
	searchInput     textinput.Model
	showSearch      bool
	searchPattern   string
	searchMatchIdx  int
	searchMatches   []int
	warnings        []string
	showWarnings    bool
	err             error
	quitting        bool
	ready           bool
	width           int
	height          int
	pollPeriod      time.Duration
	pollPeriods     []time.Duration
	pollPeriodIdx   int
	paused          bool
	scrollOffset    int
	selectedRowIdx  int
	showDetail      bool
	detailGroup     string
	detailTopic     string
	detailScrollOffset int
	detailPartitionSortKey string // Sort partitions: "" = by partition number, "lag" = by lag, "offset" = by offset
	detailTopicMetadata *types.TopicInfo // Cached topic metadata for detail view
	showHelp        bool
	followMode      bool
	showFullNumbers bool
	lastUpdateTime  time.Time
	startTime       time.Time // Start time for marquee animation
}

type tickMsg time.Time
type animTickMsg time.Time // Fast ticker for marquee animation
type dataMsg struct {
	kd    *types.KafkaData
	rates map[string]map[string]*types.RateStats
	err   error
}
type statusMsg string

func ShowRich(admin *kafka.AdminClient, params *types.Params) error {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	ti := textinput.New()
	ti.Placeholder = "Enter regex pattern..."
	ti.CharLimit = 156
	ti.Width = 50

	tiTopic := textinput.New()
	tiTopic.Placeholder = "Enter topic regex pattern..."
	tiTopic.CharLimit = 156
	tiTopic.Width = 50

	tiSearch := textinput.New()
	tiSearch.Placeholder = "Search..."
	tiSearch.CharLimit = 156
	tiSearch.Width = 50

	pollPeriods := []time.Duration{
		3 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}
	
	// Find initial poll period index
	pollPeriodIdx := 0
	initialPeriod := time.Duration(params.KafkaPollPeriod) * time.Second
	for i, period := range pollPeriods {
		if period == initialPeriod {
			pollPeriodIdx = i
			break
		}
	}

	m := model{
		admin:            admin,
		params:           params,
		loading:          true,
		spinner:          s,
		filterInput:      ti,
		topicFilterInput: tiTopic,
		searchInput:      tiSearch,
		warnings:         []string{},
		pollPeriod:       initialPeriod,
		pollPeriods:      pollPeriods,
		pollPeriodIdx:    pollPeriodIdx,
		lastUpdateTime:   time.Now(),
		startTime:        time.Now(), // Start time for marquee animation
		selectedRowIdx:   0, // Initialize to 0 so first row is selected by default
	}

	// Don't use AltScreen so the output remains on quit
	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		return err
	}

	return nil
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		loadData(m.admin, m.params),
		tickCmd(m.pollPeriod),
		animTickCmd(), // Start animation ticker for marquee
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.showFilter {
			return m.handleFilterInput(msg)
		}
		if m.showTopicFilter {
			return m.handleTopicFilterInput(msg)
		}
		if m.showSearch {
			return m.handleSearchInput(msg)
		}
		if m.showHelp {
			return m.handleHelpKeyPress(msg)
		}
		if m.showDetail {
			return m.handleDetailKeyPress(msg)
		}
		return m.handleKeyPress(msg)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.ready = true
		if !m.showFilter {
			m.updateTable()
		}

	case statusMsg:
		m.loadingStatus = string(msg)

	case dataMsg:
		m.loading = false
		m.loadingStatus = ""
		// Update lastUpdateTime when data arrives to reset countdown for next cycle
		// The countdown will count down from pollPeriod to 0, then tick fires
		m.lastUpdateTime = time.Now()
		if msg.err != nil {
			m.err = msg.err
			m.quitting = true
			return m, tea.Quit
		}
		m.kd = msg.kd
		m.rates = msg.rates
		m.iteration++
		m.updateTable()

		// Ensure selectedRowIdx is valid when data first loads
		if m.selectedRowIdx < 0 {
			rows := m.buildRowData()
			if len(rows) > 0 {
				m.selectedRowIdx = 0
			}
		}

		// If timing is enabled, exit after first data load
		if m.params.TimingOutput != nil {
			m.quitting = true
			return m, tea.Quit
		}

		if m.params.KafkaPollIterations > 0 && m.iteration >= m.params.KafkaPollIterations {
			m.quitting = true
			return m, tea.Quit
		}

	case tickMsg:
		if !m.quitting && !m.paused {
			cmds = append(cmds, tickCmd(m.pollPeriod))
			if !m.loading && !m.showFilter && !m.showTopicFilter && !m.showSearch {
				m.loading = true
				cmds = append(cmds, loadData(m.admin, m.params))
			}
		} else if !m.quitting {
			// Still schedule next tick even when paused, but don't load data
			cmds = append(cmds, tickCmd(m.pollPeriod))
		}

	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)

	case animTickMsg:
		// Fast animation ticker for marquee - continue animating
		cmds = append(cmds, animTickCmd())
	}

	if m.showFilter {
		m.filterInput, cmd = m.filterInput.Update(msg)
		cmds = append(cmds, cmd)
	} else if m.showTopicFilter {
		m.topicFilterInput, cmd = m.topicFilterInput.Update(msg)
		cmds = append(cmds, cmd)
	} else if m.showSearch {
		m.searchInput, cmd = m.searchInput.Update(msg)
		cmds = append(cmds, cmd)
	} else {
		m.table, cmd = m.table.Update(msg)
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	if m.quitting {
		return ""
	}

	if !m.ready {
		return fmt.Sprintf("\n  %s Initializing...\n", m.spinner.View())
	}

	if m.loading && m.kd == nil {
		status := "Calculating initial rates..."
		if m.loadingStatus != "" {
			status = m.loadingStatus
		}
		return fmt.Sprintf("\n  %s %s\n", m.spinner.View(), status)
	}

	if m.loading && m.loadingStatus != "" {
		return fmt.Sprintf("\n  %s %s\n", m.spinner.View(), m.loadingStatus)
	}

	if m.showFilter {
		return m.viewFilterDialog()
	}
	if m.showTopicFilter {
		return m.viewTopicFilterDialog()
	}
	if m.showSearch {
		return m.viewSearchDialog()
	}
	if m.showHelp {
		return m.viewHelp()
	}
	if m.showDetail {
		return m.viewDetail()
	}

	return m.viewMain()
}

func (m *model) viewFilterDialog() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(headerStyle.Render("Filter Consumer Groups"))
	b.WriteString("\n\n")

	currentFilter := m.filterPattern
	if currentFilter == "" {
		currentFilter = "(none)"
	}
	b.WriteString(fmt.Sprintf("Current filter: %s\n\n", currentFilter))

	b.WriteString(m.filterInput.View())
	b.WriteString("\n\n")
	b.WriteString("Press Enter to apply, Esc to cancel\n")

	return b.String()
}

func (m *model) viewTopicFilterDialog() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(headerStyle.Render("Filter Topics"))
	b.WriteString("\n\n")

	currentFilter := m.topicFilterPattern
	if currentFilter == "" {
		currentFilter = "(none)"
	}
	b.WriteString(fmt.Sprintf("Current topic filter: %s\n\n", currentFilter))

	b.WriteString(m.topicFilterInput.View())
	b.WriteString("\n\n")
	b.WriteString("Press Enter to apply, Esc to cancel\n")

	return b.String()
}

func (m *model) viewSearchDialog() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(headerStyle.Render("Search"))
	b.WriteString("\n\n")

	b.WriteString(m.searchInput.View())
	b.WriteString("\n\n")
	if len(m.searchMatches) > 0 {
		b.WriteString(fmt.Sprintf("Match %d of %d\n", m.searchMatchIdx+1, len(m.searchMatches)))
		b.WriteString("Press Enter to apply, n/N to navigate, Esc to cancel\n")
	} else if m.searchPattern != "" {
		b.WriteString("No matches found\n")
		b.WriteString("Press Enter to apply, Esc to cancel\n")
	} else {
		b.WriteString("Press Enter to apply, Esc to cancel\n")
	}

	return b.String()
}

func (m *model) viewHelp() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(headerStyle.Render("Help - Keyboard Shortcuts"))
	b.WriteString("\n\n")

	b.WriteString("Navigation:\n")
	b.WriteString("  ↑/↓, J/K     Scroll up/down\n")
	b.WriteString("  Space/B      Page down/up\n")
	b.WriteString("  Home/End     Jump to top/bottom\n")
	b.WriteString("\n")

	b.WriteString("Actions:\n")
	b.WriteString("  Q             Quit\n")
	b.WriteString("  P             Pause/resume updates\n")
	b.WriteString("  +/-           Adjust refresh rate\n")
	b.WriteString("  F             Filter consumer groups\n")
	b.WriteString("  X             Filter topics\n")
	b.WriteString("  /             Search\n")
	b.WriteString("  ? or H        Show this help\n")
	b.WriteString("  Enter or D    View partition details\n")
	b.WriteString("  Esc           Close dialogs/details\n")
	b.WriteString("\n")

	b.WriteString("Sorting:\n")
	b.WriteString("  G             Sort by Group\n")
	b.WriteString("  O             Sort by Topic\n")
	b.WriteString("  P             Sort by Partitions\n")
	b.WriteString("  T or t        Sort by Time Left\n")
	b.WriteString("  L             Sort by Lag\n")
	b.WriteString("  N             Sort by New topic rate\n")
	b.WriteString("  C             Sort by Consumed rate\n")
	b.WriteString("\n")

	b.WriteString("Partition Details:\n")
	b.WriteString("  S             Sort partitions by lag+offset\n")
	b.WriteString("  Space/B       Page down/up\n")
	b.WriteString("\n")

	b.WriteString("Column Descriptions:\n")
	b.WriteString("  Group         Consumer group name\n")
	b.WriteString("  Topic         Kafka topic name\n")
	b.WriteString("  Partitions    Number of partitions\n")
	b.WriteString("  Since         Time since last update (seconds)\n")
	b.WriteString("  Events        Total events consumed\n")
	b.WriteString("  New topic     Arrival rate (events/sec)\n")
	b.WriteString("  Consumed      Consumption rate (events/sec)\n")
	b.WriteString("  Time Left     Estimated time to consume all lag\n")
	b.WriteString("  Lag           Total lag across all partitions\n")
	b.WriteString("\n")

	b.WriteString("Partition Health:\n")
	b.WriteString("  PAR           Peak-to-Average ratio: worst-case burden\n")
	b.WriteString("                on the single most overloaded consumer\n")
	b.WriteString("  Cv            Coefficient of Variation (StdDev/mean).\n")
	b.WriteString("                How uniformly load is distributed:\n")
	b.WriteString("                0 = perfect, 0.5-1 = high skew,\n")
	b.WriteString("                >1 = critical skew (standard deviation > mean):\n")
	b.WriteString("                action required (e.g., key salting, change partitioner)\n")
	b.WriteString("\n")

	b.WriteString("Other:\n")
	b.WriteString("  E             Toggle human-readable/plain numbers\n")
	b.WriteString("  U             Toggle full numbers (alias for E)\n")
	b.WriteString("  Ctrl+F        Toggle follow mode\n")
	b.WriteString("\n")

	b.WriteString("Press ? or H or Esc to close\n")

	return b.String()
}

func (m *model) viewDetail() string {
	var b strings.Builder

	if m.kd == nil || m.rates == nil {
		return "No data available\n"
	}

	groupLag, exists := m.kd.GroupLags[m.detailGroup]
	if !exists {
		return fmt.Sprintf("Group %s not found\n", m.detailGroup)
	}

	lagStats, exists := groupLag[m.detailTopic]
	if !exists {
		return fmt.Sprintf("Topic %s not found for group %s\n", m.detailTopic, m.detailGroup)
	}

	rateStats, exists := m.rates[m.detailGroup][m.detailTopic]
	if !exists {
		rateStats = &types.RateStats{}
	}

	// Get partition offsets for more details
	var groupOffsets map[int32]int64
	var topicOffsets map[int32]int64
	if groupOffset, exists := m.kd.GroupOffsets[m.detailGroup]; exists {
		if topicPartOffsets, exists := groupOffset.TopicOffsets[m.detailTopic]; exists {
			groupOffsets = topicPartOffsets
		}
	}
	if topicOffset, exists := m.kd.TopicOffsets[m.detailTopic]; exists {
		topicOffsets = topicOffset.PartitionOffsets
	}

	// Format topic name (handle anonymization if needed)
	topicName := m.detailTopic
	if m.params.Anonymize {
		topicName = fmt.Sprintf("topic %06d", hashString(m.detailTopic)%1000000)
	}

	// Sort partitions for display
	partitions := make([]int32, 0, len(lagStats.PartitionLags))
	for part := range lagStats.PartitionLags {
		partitions = append(partitions, part)
	}
	
	switch m.detailPartitionSortKey {
	case "lag":
		// Sort by lag (descending - highest lag first)
		sort.Slice(partitions, func(i, j int) bool {
			lagI := lagStats.PartitionLags[partitions[i]]
			lagJ := lagStats.PartitionLags[partitions[j]]
			if lagI != lagJ {
				return lagI > lagJ // Higher lag first
			}
			// Tiebreaker: sort by partition number
			return partitions[i] < partitions[j]
		})
	case "groupoffset":
		// Sort by group offset (descending - highest offset first)
		sort.Slice(partitions, func(i, j int) bool {
			offsetI := int64(0)
			offsetJ := int64(0)
			if groupOffsets != nil {
				if o, exists := groupOffsets[partitions[i]]; exists {
					offsetI = o
				}
			}
			if groupOffsets != nil {
				if o, exists := groupOffsets[partitions[j]]; exists {
					offsetJ = o
				}
			}
			if offsetI != offsetJ {
				return offsetI > offsetJ // Higher offset first
			}
			// Tiebreaker: sort by partition number
			return partitions[i] < partitions[j]
		})
	case "topicoffset":
		// Sort by topic offset (descending - highest offset first)
		sort.Slice(partitions, func(i, j int) bool {
			offsetI := int64(0)
			offsetJ := int64(0)
			if topicOffsets != nil {
				if o, exists := topicOffsets[partitions[i]]; exists {
					offsetI = o
				}
			}
			if topicOffsets != nil {
				if o, exists := topicOffsets[partitions[j]]; exists {
					offsetJ = o
				}
			}
			if offsetI != offsetJ {
				return offsetI > offsetJ // Higher offset first
			}
			// Tiebreaker: sort by partition number
			return partitions[i] < partitions[j]
		})
	case "rate":
		// Sort by rate (descending - highest rate first)
		sort.Slice(partitions, func(i, j int) bool {
			rateI := 0.0
			rateJ := 0.0
			if rateStats.PartitionRates != nil {
				if r, exists := rateStats.PartitionRates[partitions[i]]; exists {
					rateI = r
				}
			}
			if rateStats.PartitionRates != nil {
				if r, exists := rateStats.PartitionRates[partitions[j]]; exists {
					rateJ = r
				}
			}
			if rateI != rateJ {
				return rateI > rateJ // Higher rate first
			}
			// Tiebreaker: sort by partition number
			return partitions[i] < partitions[j]
		})
	default:
		// Default: sort by partition number
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	}

	// Calculate space allocation - count actual newlines:
	// Header: title (1) + "\n\n" (2) + ReplicationFactor/Configs (1) + Total Lag (1) + Min (1) + PAR (1) + Cv (1) + Consumption Rate (1) + Arrival Rate (1) + ETA (1) + "\n" (1) + table header (1) + separator (1) = 14 lines
	// Footer: "\n" (1) + scroll info (1) + "\n" (1) + "Press Esc" (1) = 4 lines  
	// Total reserved: 18 lines
	headerLines := 14
	footerLines := 4
	maxPartitionRows := m.height - headerLines - footerLines
	if maxPartitionRows < 1 {
		maxPartitionRows = 1 // At least show 1 partition row
	}

	// Apply scroll offset
	startIdx := m.detailScrollOffset
	if startIdx >= len(partitions) {
		startIdx = len(partitions) - 1
	}
	if startIdx < 0 {
		startIdx = 0
	}
	// Handle "end" key - scroll to show last partitions
	if startIdx > len(partitions)-maxPartitionRows && len(partitions) > maxPartitionRows {
		startIdx = len(partitions) - maxPartitionRows
		if startIdx < 0 {
			startIdx = 0
		}
		m.detailScrollOffset = startIdx
	}

	endIdx := startIdx + maxPartitionRows
	if endIdx > len(partitions) {
		endIdx = len(partitions)
	}

	// Fetch topic metadata if not cached
	if m.detailTopicMetadata == nil || m.detailTopicMetadata.Name != m.detailTopic {
		ctx := context.Background()
		metadata, err := m.admin.GetTopicMetadata(ctx, m.detailTopic)
		if err == nil {
			m.detailTopicMetadata = metadata
		}
	}

	// Render header (always visible) - start at top of screen
	b.WriteString(headerStyle.Render(fmt.Sprintf("Partition Details: %s / %s", m.detailGroup, m.detailTopic)))
	b.WriteString("\n\n")

	// Display topic-level metadata and configs in a two-column layout
	// Left column: metadata, Right column: configs table
	leftColWidth := 50 // Width for left column (metadata)
	rightColWidth := m.width - leftColWidth - 5 // Width for right column (configs), with spacing
	
	if rightColWidth < 30 {
		rightColWidth = 30 // Minimum width for configs
		leftColWidth = m.width - rightColWidth - 5
	}
	
	// Helper function to format config value with .ms conversion to days
	formatConfigValue := func(key, value string) string {
		// Check if it's a .ms config and convert to days
		if strings.HasSuffix(key, ".ms") {
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
				days := float64(ms) / (1000 * 60 * 60 * 24)
				if days >= 1 {
					return fmt.Sprintf("%s (%d days)", value, int(days))
				} else {
					hours := float64(ms) / (1000 * 60 * 60)
					if hours >= 1 {
						return fmt.Sprintf("%s (%.1f hours)", value, hours)
					} else {
						minutes := float64(ms) / (1000 * 60)
						return fmt.Sprintf("%s (%.1f min)", value, minutes)
					}
				}
			}
		}
		return value
	}
	
	// Build configs table rows
	var configRows []string
	if m.detailTopicMetadata != nil && len(m.detailTopicMetadata.Configs) > 0 {
		// Sort config keys for consistent display
		configKeys := make([]string, 0, len(m.detailTopicMetadata.Configs))
		for k := range m.detailTopicMetadata.Configs {
			configKeys = append(configKeys, k)
		}
		sort.Strings(configKeys)
		
		for _, key := range configKeys {
			value := formatConfigValue(key, m.detailTopicMetadata.Configs[key])
			configRows = append(configRows, fmt.Sprintf("%-25s: %s", key, value))
		}
	}
	
	// Display metadata and configs side by side
	parStr := "-"
	if lagStats.PAR > 0 {
		parStr = fmt.Sprintf("%.2f", lagStats.PAR)
	}
	cvStr := "-"
	if lagStats.Cv > 0 {
		cvStr = fmt.Sprintf("%.2f", lagStats.Cv)
	}
	metadataLines := []string{
		fmt.Sprintf("Total Lag: %s", formatNumber(m.showFullNumbers, lagStats.Sum)),
		fmt.Sprintf("Min: %s, Max: %s, Mean: %.1f, Median: %s",
			formatNumber(m.showFullNumbers, lagStats.Min),
			formatNumber(m.showFullNumbers, lagStats.Max),
			lagStats.Mean,
			formatNumber(m.showFullNumbers, lagStats.Median)),
		fmt.Sprintf("PAR: %s", parStr),
		fmt.Sprintf("Cv: %s", cvStr),
		fmt.Sprintf("Consumption Rate: %s evts/sec", formatNumber(m.showFullNumbers, int64(rateStats.EventsConsumptionRate))),
		fmt.Sprintf("Arrival Rate: %s evts/sec", formatNumber(m.showFullNumbers, int64(rateStats.EventsArrivalRate))),
		fmt.Sprintf("ETA: %s", rateStats.RemainingHMS),
	}
	
	// Add topic metadata header
	if m.detailTopicMetadata != nil {
		headerParts := []string{}
		if m.detailTopicMetadata.TopicID != "" {
			headerParts = append(headerParts, fmt.Sprintf("TopicID: %s", m.detailTopicMetadata.TopicID))
		}
		headerParts = append(headerParts, fmt.Sprintf("ReplicationFactor: %d", m.detailTopicMetadata.ReplicationFactor))
		if len(headerParts) > 0 {
			b.WriteString(strings.Join(headerParts, "  ") + "\n")
		}
	}
	
	// Display metadata and configs side by side
	// Limit config rows to match metadata rows (5 rows) to prevent pushing table off screen
	maxConfigRows := len(metadataLines) // Show at most as many configs as metadata lines
	if len(configRows) > maxConfigRows {
		configRows = configRows[:maxConfigRows]
	}
	
	for i := 0; i < len(metadataLines); i++ {
		leftPart := metadataLines[i]
		rightPart := ""
		if i < len(configRows) {
			rightPart = configRows[i]
		}
		
		// Format left column
		leftFormatted := fmt.Sprintf("%-*s", leftColWidth, leftPart)
		if rightPart != "" {
			b.WriteString(fmt.Sprintf("%s  %s\n", leftFormatted, rightPart))
		} else {
			b.WriteString(leftFormatted + "\n")
		}
	}
	
	b.WriteString("\n")

	// Build header with highlighted hotkeys
	// Helper function to highlight hotkey in header column
	formatDetailHeaderHotkey := func(text string, hotkey string, sortKey string, width int, rightAlign bool) string {
		hotkeyLower := strings.ToLower(hotkey)
		textLower := strings.ToLower(text)
		hotkeyIdx := strings.Index(textLower, hotkeyLower)
		
		var coloredText string
		if hotkeyIdx == -1 {
			coloredText = colorBrightWhite + text + colorReset
		} else {
			before := text[:hotkeyIdx]
			hotkeyChar := text[hotkeyIdx : hotkeyIdx+1]
			after := text[hotkeyIdx+1:]
			
			if m.detailPartitionSortKey == sortKey {
				// Reverse video for sorted key
				coloredText = colorBrightWhite + before + colorReverse + colorBrightGreen + hotkeyChar + colorReset + colorBrightWhite + after + colorReset
			} else {
				// Normal green highlighting
				coloredText = colorBrightWhite + before + colorBrightGreen + hotkeyChar + colorBrightWhite + after + colorReset
			}
		}
		
		// Format with proper width - format the colored text directly
		// ANSI codes don't count toward width, so we need to pad based on the plain text length
		plainLen := len(text)
		if rightAlign {
			if plainLen < width {
				// Pad on the left
				padding := strings.Repeat(" ", width-plainLen)
				return padding + coloredText
			}
			return coloredText
		} else {
			if plainLen < width {
				// Pad on the right
				padding := strings.Repeat(" ", width-plainLen)
				return coloredText + padding
			}
			return coloredText
		}
	}
	
	// Build header with highlighted hotkeys (preserving column widths)
	// Column widths must match data row formatting exactly:
	// Partition: 9 chars (right-aligned in data)
	// Topic: 24 chars (left-aligned)
	// Group Offset: 12 chars (right-aligned in data)
	// Topic Offset: 12 chars (right-aligned in data)
	// Lag: 9 chars (left-aligned in data)
	// Rate: 16 chars (left-aligned in data)
	// Replica IDs: 20 chars (left-aligned)
	// ISR: 20 chars (left-aligned)
	// Leader: 8 chars (left-aligned)
	partitionHeader := fmt.Sprintf("%9s", "Partition") // Right-aligned to match %9d
	topicHeader := fmt.Sprintf("%-24s", "Topic")
	groupOffsetHeader := formatDetailHeaderHotkey("Group Offset", "G", "groupoffset", 12, true) // Right-aligned to match %12s
	topicOffsetHeader := formatDetailHeaderHotkey("Topic Offset", "T", "topicoffset", 12, true) // Right-aligned to match %12s
	lagHeader := formatDetailHeaderHotkey("Lag", "L", "lag", 9, false)
	rateHeader := formatDetailHeaderHotkey("Rate (evts/sec)", "R", "rate", 16, false)
	replicasHeader := fmt.Sprintf("%-20s", "Replica IDs")
	isrHeader := fmt.Sprintf("%-20s", "ISR")
	leaderHeader := fmt.Sprintf("%-8s", "Leader")
	
	headerLine := fmt.Sprintf("%s | %s | %s | %s | %s | %s | %s | %s | %s",
		partitionHeader,
		topicHeader,
		groupOffsetHeader,
		topicOffsetHeader,
		lagHeader,
		rateHeader,
		replicasHeader,
		isrHeader,
		leaderHeader)
	
	b.WriteString(headerLine + "\n")
	// Separator line must match exact column widths AND spacing format (generate programmatically to ensure alignment):
	// Format matches headerLine: "%s | %s | %s | ..." which means spaces around pipes
	// Partition: 9, Topic: 24, Group Offset: 12, Topic Offset: 12, Lag: 9, Rate: 16, Replica IDs: 20, ISR: 20, Leader: 8
	separator := strings.Repeat("-", 9) + " | " +
		strings.Repeat("-", 24) + " | " +
		strings.Repeat("-", 12) + " | " +
		strings.Repeat("-", 12) + " | " +
		strings.Repeat("-", 9) + " | " +
		strings.Repeat("-", 16) + " | " +
		strings.Repeat("-", 20) + " | " +
		strings.Repeat("-", 20) + " | " +
		strings.Repeat("-", 8) + "\n"
	b.WriteString(separator)

	// Calculate min/max for lag and rate to determine color gradients
	var minLag, maxLag int64 = 0, 0
	var minRate, maxRate float64 = 0, 0
	firstLag := true
	firstRate := true
	
	for _, part := range partitions {
		lag := lagStats.PartitionLags[part]
		if firstLag {
			minLag, maxLag = lag, lag
			firstLag = false
		} else {
			if lag < minLag {
				minLag = lag
			}
			if lag > maxLag {
				maxLag = lag
			}
		}
		
		rate := 0.0
		if rateStats.PartitionRates != nil {
			if r, exists := rateStats.PartitionRates[part]; exists {
				rate = r
			}
		}
		if firstRate {
			minRate, maxRate = rate, rate
			firstRate = false
		} else {
			if rate < minRate {
				minRate = rate
			}
			if rate > maxRate {
				maxRate = rate
			}
		}
	}
	
	// Helper function to get color based on value position in range (green -> yellow -> red)
	getRankColor := func(value, minVal, maxVal float64) string {
		if maxVal == minVal {
			return colorReset // All same value, no color
		}
		// Normalize to 0-1 range
		ratio := (value - minVal) / (maxVal - minVal)
		// Map to color: 0.0 = green, 0.5 = yellow, 1.0 = red
		if ratio < 0.33 {
			// Green for low values (bottom third)
			return colorGreen
		} else if ratio < 0.67 {
			// Yellow for middle values (middle third)
			return colorYellow
		} else {
			// Red for high values (top third)
			return colorRed
		}
	}

	// Render visible partition rows
	for i := startIdx; i < endIdx; i++ {
		part := partitions[i]
		lag := lagStats.PartitionLags[part]
		
		groupOffset := int64(0)
		if groupOffsets != nil {
			if offset, exists := groupOffsets[part]; exists {
				groupOffset = offset
			}
		}
		
		topicOffset := int64(0)
		if topicOffsets != nil {
			if offset, exists := topicOffsets[part]; exists {
				topicOffset = offset
			}
		}

		// Truncate topic name if too long (max 24 chars for display)
		displayTopic := topicName
		if len(displayTopic) > 24 {
			displayTopic = displayTopic[:21] + "..."
		}
		
		// Get per-partition consumption rate
		partitionRate := 0.0
		if rateStats.PartitionRates != nil {
			if rate, exists := rateStats.PartitionRates[part]; exists {
				partitionRate = rate
			}
		}
		
		// Get colors based on ranking
		lagColor := getRankColor(float64(lag), float64(minLag), float64(maxLag))
		rateColor := getRankColor(partitionRate, minRate, maxRate)
		
		lagStr := formatNumber(m.showFullNumbers, lag)
		rateStr := formatRate(m.showFullNumbers, partitionRate)
		
		// Get partition metadata
		var replicasStr, isrStr, leaderStr string
		if m.detailTopicMetadata != nil {
			// Find partition info
			for _, partInfo := range m.detailTopicMetadata.PartInfo {
				if partInfo.ID == part {
					// Format replicas
					replicaStrs := make([]string, len(partInfo.Replicas))
					for i, r := range partInfo.Replicas {
						replicaStrs[i] = fmt.Sprintf("%d", r)
					}
					replicasStr = fmt.Sprintf("(%s)", strings.Join(replicaStrs, ","))
					
					// Format ISR
					isrStrs := make([]string, len(partInfo.ISRs))
					for i, isr := range partInfo.ISRs {
						isrStrs[i] = fmt.Sprintf("%d", isr)
					}
					isrStr = fmt.Sprintf("(%s)", strings.Join(isrStrs, ","))
					
					// Format leader
					leaderStr = fmt.Sprintf("%d", partInfo.Leader)
					break
				}
			}
		}
		if replicasStr == "" {
			replicasStr = "-"
		}
		if isrStr == "" {
			isrStr = "-"
		}
		if leaderStr == "" {
			leaderStr = "-"
		}
		
		// Format rate with proper width (16 chars) to match header
		rateStrFormatted := fmt.Sprintf("%-16s", rateStr)
		
		b.WriteString(fmt.Sprintf("%9d | %-24s | %12s | %12s | %s%-9s%s | %s%s%s | %-20s | %-20s | %-8s\n",
			part,
			displayTopic,
			formatNumber(m.showFullNumbers, groupOffset),
			formatNumber(m.showFullNumbers, topicOffset),
			lagColor, lagStr, colorReset,
			rateColor, rateStrFormatted, colorReset,
			replicasStr,
			isrStr,
			leaderStr))
	}

	// Render footer (always visible)
	sortHint := ""
	if m.detailPartitionSortKey == "lag" {
		sortHint = " | [L]ag sorted"
	} else if m.detailPartitionSortKey == "groupoffset" {
		sortHint = " | [G]roup offset sorted"
	} else if m.detailPartitionSortKey == "topicoffset" {
		sortHint = " | [T]opic offset sorted"
	} else if m.detailPartitionSortKey == "rate" {
		sortHint = " | [R]ate sorted"
	} else {
		sortHint = " | [L]ag/[G]roup/[T]opic/[R]ate sort"
	}
	
	// Add color legend
	colorLegend := fmt.Sprintf(" | %s[green=low%s, %sred=high%s]", colorGreen, colorReset, colorRed, colorReset)
	
	if len(partitions) > maxPartitionRows {
		b.WriteString(fmt.Sprintf("\nRows %d-%d of %d partitions | Use ↑↓/JK/Space/B to scroll, Home/End to jump%s%s\n", startIdx+1, endIdx, len(partitions), sortHint, colorLegend))
	} else {
		b.WriteString(fmt.Sprintf("\n%d partitions total%s%s\n", len(partitions), sortHint, colorLegend))
	}

	b.WriteString("\nPress Esc to return\n")

	return b.String()
}

func (m *model) viewMain() string {
	var b strings.Builder

	// Warnings panel removed - warnings are no longer displayed

	// Table
	if m.kd != nil && m.rates != nil {
		b.WriteString(m.renderTable())

		// Bottom legend/header lines - split into 2 rows for narrow screens
		timeStr := time.Now().Format("15:04:05")
		updateTimeStr := m.lastUpdateTime.Format("15:04:05")
		
		// Helper function to format hotkey in legend
		formatLegendHotkey := func(text string, hotkey string, sortKey string) string {
			if m.sortKey == sortKey {
				// Reverse video for sorted key
				return strings.Replace(text, hotkey, colorReverse+colorBrightGreen+hotkey+colorReset+colorBrightWhite, 1)
			} else {
				// Normal green highlighting
				return strings.Replace(text, hotkey, colorBrightGreen+hotkey+colorBrightWhite, 1)
			}
		}
		
		// Calculate refresh countdown
		var refreshCountdown string
		if m.paused {
			refreshCountdown = "paused"
		} else if m.loading {
			// When loading, countdown should be 0s since refresh has started
			refreshCountdown = "0s"
		} else {
			elapsed := time.Since(m.lastUpdateTime)
			remaining := m.pollPeriod - elapsed
			if remaining < 0 {
				remaining = 0
			}
			// Use standard rounding: round to nearest second for accurate countdown
			remainingSec := int(remaining.Seconds() + 0.5)
			refreshCountdown = fmt.Sprintf("%ds", remainingSec)
		}
		
		// Row 1: Status and actions
		header1 := colorBrightWhite + fmt.Sprintf("%s poll: %d", timeStr, m.iteration)
		if m.paused {
			header1 += colorYellow + " [PAUSED]"
		}
		header1 += colorBrightWhite + fmt.Sprintf(" | refresh: %s", refreshCountdown)
		header1 += colorBrightWhite + " | actions: "
		header1 += colorBrightWhite + formatLegendHotkey("[Q]uit", "Q", "") + ", "
		header1 += colorBrightWhite + formatLegendHotkey("[F]ilter", "F", "") + ", "
		header1 += colorBrightWhite + formatLegendHotkey("[X]topic filter", "X", "") + ", "
		header1 += colorBrightWhite + colorBrightGreen + "[Enter]" + colorBrightWhite + "/" + colorBrightGreen + "[D]" + colorBrightWhite + "Details, "
		header1 += colorBrightWhite + formatLegendHotkey("[?]Help", "?", "")
		header1 += colorBrightWhite + " | " + colorBrightGreen + "P" + colorBrightWhite + " pause"
		header1 += colorBrightWhite + " | " + colorBrightGreen + "+/-" + colorBrightWhite + " rate"
		header1 += colorBrightWhite + " | " + colorBrightGreen + "E" + colorBrightWhite + " scale"
		
		if m.filterPattern != "" {
			header1 += colorBrightWhite + fmt.Sprintf(" | Group: %s", m.filterPattern)
		}
		if m.topicFilterPattern != "" {
			header1 += colorBrightWhite + fmt.Sprintf(" | Topic: %s", m.topicFilterPattern)
		}
		
		// Row 2: Sorting and navigation - aligned vertically
		header2 := colorBrightWhite + "sort-by: "
		header2 += colorBrightWhite + formatLegendHotkey("[G]roup", "G", "group") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("T[o]pic", "o", "topic") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("[P]artitions", "P", "partitions") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("[T]ime Left", "T", "eta") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("[L]ag", "L", "lag") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("[N]ew topic", "N", "newrate") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("[C]onsumed", "C", "rate") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("P[A]R", "A", "par") + "  "
		header2 += colorBrightWhite + formatLegendHotkey("C[V]", "V", "cv")
		header2 += colorBrightWhite + " | scroll: " + colorBrightGreen + "↑↓" + colorBrightWhite + "/" + colorBrightGreen + "JK" + colorBrightWhite + "/" + colorBrightGreen + "Space" + colorBrightWhite + "/" + colorBrightGreen + "B" + colorBrightWhite + "/PgUp/PgDn"

		if m.sortKey != "" {
			direction := "↑"
			if m.sortReverse {
				direction = "↓"
			}
			header2 += colorBrightWhite + fmt.Sprintf(" | Sorted: %s %s", m.sortKey, direction)
		}
		
		// Count totals (will be shown in status bar)
		rows := m.buildRowData()
		totalGroups := make(map[string]bool)
		totalTopics := make(map[string]bool)
		for _, row := range rows {
			totalGroups[row.sortGroup] = true
			totalTopics[row.sortGroup+"|"+row.sortTopic] = true
		}

		b.WriteString("\n" + colorBold + header1 + colorReset)
		b.WriteString("\n" + colorBold + header2 + colorReset)

		// Combined status line: viewport info + loading status + update time + counts
		var statusParts []string

		viewportInfo := m.getViewportInfo()
		if viewportInfo != "" {
			statusParts = append(statusParts, viewportInfo)
		}

		if m.loading {
			loadingText := "Refreshing data..."
			if m.loadingStatus != "" {
				loadingText = m.loadingStatus
			}
			statusParts = append(statusParts, fmt.Sprintf("%s %s", m.spinner.View(), loadingText))
		}

		// Add groups/topics count and update time to status bar (using counts calculated above)
		statusParts = append(statusParts, fmt.Sprintf("Groups: %d Topics: %d", len(totalGroups), len(totalTopics)))
		statusParts = append(statusParts, fmt.Sprintf("Updated: %s", updateTimeStr))

		if len(statusParts) > 0 {
			b.WriteString("\n" + strings.Join(statusParts, " | "))
		}
	} else {
		b.WriteString(fmt.Sprintf("\n  %s Loading data...\n", m.spinner.View()))
	}

	return b.String()
}

func (m *model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	case "p":
		// Pause/resume updates
		m.paused = !m.paused
		if m.paused {
			m.addWarning("Updates paused - press P to resume")
		} else {
			m.addWarning("Updates resumed")
		}
		return m, nil

	case " ":
		// Page down in main view
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		// Update selected row index
		if m.selectedRowIdx < len(rows)-1 {
			m.selectedRowIdx += maxRows
			if m.selectedRowIdx >= len(rows) {
				m.selectedRowIdx = len(rows) - 1
			}
			// Adjust scroll offset to keep selected row visible
			m.scrollToSelectedRow()
		}
		return m, nil

	case "b":
		// Page up in main view
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		// Update selected row index
		if m.selectedRowIdx > 0 {
			m.selectedRowIdx -= maxRows
			if m.selectedRowIdx < 0 {
				m.selectedRowIdx = 0
			}
			// Adjust scroll offset to keep selected row visible
			m.scrollToSelectedRow()
		}
		return m, nil

	case "+", "=":
		// Increase refresh rate
		if m.pollPeriodIdx < len(m.pollPeriods)-1 {
			m.pollPeriodIdx++
			m.pollPeriod = m.pollPeriods[m.pollPeriodIdx]
			m.addWarning(fmt.Sprintf("Refresh rate: %v", m.pollPeriod))
		}
		return m, nil

	case "-", "_":
		// Decrease refresh rate
		if m.pollPeriodIdx > 0 {
			m.pollPeriodIdx--
			m.pollPeriod = m.pollPeriods[m.pollPeriodIdx]
			m.addWarning(fmt.Sprintf("Refresh rate: %v", m.pollPeriod))
		}
		return m, nil

	case "f":
		m.showFilter = true
		m.filterInput.Focus()
		return m, nil

	case "x":
		// 'x' for topic filter
		m.showTopicFilter = true
		m.topicFilterInput.Focus()
		return m, nil

	case "/":
		m.showSearch = true
		m.searchInput.Focus()
		m.searchPattern = ""
		m.searchMatchIdx = 0
		m.searchMatches = []int{}
		return m, nil

	case "?", "h":
		m.showHelp = true
		return m, nil

	case "enter", "d":
		// Enter detail view (Enter or D)
		rows := m.buildRowData()
		m.sortRowData(rows)
		if m.selectedRowIdx >= 0 && m.selectedRowIdx < len(rows) {
			row := rows[m.selectedRowIdx]
			m.showDetail = true
			m.detailGroup = row.sortGroup
			m.detailTopic = row.sortTopic
			m.detailScrollOffset = 0 // Reset scroll to show header
			m.detailPartitionSortKey = "" // Reset sort to default
			m.detailTopicMetadata = nil // Clear cached metadata (will be fetched in viewDetail)
		}
		return m, nil

	case "home":
		m.scrollOffset = 0
		m.selectedRowIdx = 0
		return m, nil

	case "end":
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		if len(rows) > maxRows {
			m.scrollOffset = len(rows) - maxRows
			m.selectedRowIdx = len(rows) - 1
		} else {
			m.scrollOffset = 0
			m.selectedRowIdx = len(rows) - 1
		}
		return m, nil

	case "j", "down":
		rows := m.buildRowData()
		m.sortRowData(rows)
		if m.selectedRowIdx < len(rows)-1 {
			m.selectedRowIdx++
			// Adjust scroll offset to keep selected row visible
			m.scrollToSelectedRow()
		}
		return m, nil

	case "k", "up":
		rows := m.buildRowData()
		m.sortRowData(rows)
		if m.selectedRowIdx > 0 {
			m.selectedRowIdx--
			// Adjust scroll offset to keep selected row visible
			m.scrollToSelectedRow()
		}
		return m, nil

	case "pgdown":
		// Page down - move selection down by page size
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		// Update selected row index
		if m.selectedRowIdx < len(rows)-1 {
			m.selectedRowIdx += maxRows
			if m.selectedRowIdx >= len(rows) {
				m.selectedRowIdx = len(rows) - 1
			}
			// Adjust scroll offset to keep selected row visible
			m.scrollToSelectedRow()
		}
		return m, nil

	case "pgup":
		// Page up - move selection up by page size
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		// Update selected row index
		if m.selectedRowIdx > 0 {
			m.selectedRowIdx -= maxRows
			if m.selectedRowIdx < 0 {
				m.selectedRowIdx = 0
			}
			// Adjust scroll offset to keep selected row visible
			m.scrollToSelectedRow()
		}
		return m, nil

	case "e":
		// Toggle between human-readable and plain numbers (like top command)
		m.showFullNumbers = !m.showFullNumbers
		return m, nil

	case "u":
		// Toggle full numbers (alias for 'e')
		m.showFullNumbers = !m.showFullNumbers
		return m, nil

	case "ctrl+f":
		// Toggle follow mode
		m.followMode = !m.followMode
		if m.followMode {
			m.addWarning("Follow mode enabled")
		} else {
			m.addWarning("Follow mode disabled")
		}
		return m, nil

	case "n":
		// Navigate to next search match (if search is active), otherwise sort by newrate
		if m.searchPattern != "" && len(m.searchMatches) > 0 {
			m.searchMatchIdx = (m.searchMatchIdx + 1) % len(m.searchMatches)
			m.scrollToMatch()
			return m, nil
		}
		// Fall through to sorting
		newSortKey := "newrate"
		if m.sortKey == newSortKey {
			m.sortReverse = !m.sortReverse
		} else {
			m.sortKey = newSortKey
			m.sortReverse = false
		}
		m.scrollOffset = 0
		m.selectedRowIdx = 0
		m.updateTable()
		return m, nil

	case "N":
		// Navigate to previous search match (if search is active), otherwise sort by newrate
		if m.searchPattern != "" && len(m.searchMatches) > 0 {
			m.searchMatchIdx = (m.searchMatchIdx - 1 + len(m.searchMatches)) % len(m.searchMatches)
			m.scrollToMatch()
			return m, nil
		}
		// Fall through to sorting
		newSortKey := "newrate"
		if m.sortKey == newSortKey {
			m.sortReverse = !m.sortReverse
		} else {
			m.sortKey = newSortKey
			m.sortReverse = false
		}
		m.scrollOffset = 0
		m.selectedRowIdx = 0
		m.updateTable()
		return m, nil

	case "g", "G", "o", "O", "P", "t", "T", "l", "L", "c", "C", "a", "A", "v", "V":
		newSortKey := ""
		switch msg.String() {
		case "g", "G":
			newSortKey = "group"
		case "o", "O":
			newSortKey = "topic"
		case "P":
			newSortKey = "partitions"
		case "t", "T":
			// Both t and T sort by Time Left (ETA)
			newSortKey = "eta"
		case "l", "L":
			newSortKey = "lag"
		case "c", "C":
			newSortKey = "rate"
		case "n", "N":
			newSortKey = "newrate"
		case "a", "A":
			newSortKey = "par"
		case "v", "V":
			newSortKey = "cv"
		}

		if newSortKey != "" {
			if m.sortKey == newSortKey {
				m.sortReverse = !m.sortReverse
			} else {
				m.sortKey = newSortKey
				m.sortReverse = (newSortKey == "eta" || newSortKey == "lag" || newSortKey == "par" || newSortKey == "cv")
			}

			m.scrollOffset = 0 // Reset scroll when sorting
			m.selectedRowIdx = 0
			m.updateTable()
			return m, nil
		}
		return m, nil
	}

	return m, nil
}

func (m *model) handleFilterInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		pattern := m.filterInput.Value()
		if pattern != "" {
			m.filterPattern = pattern
			m.addWarning(fmt.Sprintf("Applied filter: %s", pattern))
		} else {
			m.filterPattern = ""
			m.addWarning("Cleared group filter")
		}
		m.showFilter = false
		m.filterInput.Blur()
		m.filterInput.SetValue("")
		return m, loadData(m.admin, m.params)

	case tea.KeyEsc:
		m.showFilter = false
		m.filterInput.Blur()
		m.filterInput.SetValue("")
		m.addWarning("Filter input cancelled")
		return m, nil
	}

	var cmd tea.Cmd
	m.filterInput, cmd = m.filterInput.Update(msg)
	return m, cmd
}

func (m *model) handleTopicFilterInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		pattern := m.topicFilterInput.Value()
		if pattern != "" {
			m.topicFilterPattern = pattern
			m.addWarning(fmt.Sprintf("Applied topic filter: %s", pattern))
		} else {
			m.topicFilterPattern = ""
			m.addWarning("Cleared topic filter")
		}
		m.showTopicFilter = false
		m.topicFilterInput.Blur()
		m.topicFilterInput.SetValue("")
		return m, nil

	case tea.KeyEsc:
		m.showTopicFilter = false
		m.topicFilterInput.Blur()
		m.topicFilterInput.SetValue("")
		m.addWarning("Topic filter input cancelled")
		return m, nil
	}

	var cmd tea.Cmd
	m.topicFilterInput, cmd = m.topicFilterInput.Update(msg)
	return m, cmd
}

func (m *model) handleSearchInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		// Close search dialog and apply search to main view
		pattern := m.searchInput.Value()
		m.searchPattern = pattern
		m.updateSearchMatches()
		if len(m.searchMatches) > 0 {
			m.searchMatchIdx = 0
			m.scrollToMatch()
		}
		m.showSearch = false
		m.searchInput.Blur()
		return m, nil

	case tea.KeyEsc:
		m.showSearch = false
		m.searchInput.Blur()
		m.searchInput.SetValue("")
		m.searchPattern = ""
		m.searchMatchIdx = 0
		m.searchMatches = []int{}
		return m, nil
	}

	// Handle n/N for navigation while searching
	switch msg.String() {
	case "n":
		if len(m.searchMatches) > 0 {
			m.searchMatchIdx = (m.searchMatchIdx + 1) % len(m.searchMatches)
			m.scrollToMatch()
		}
		return m, nil

	case "N":
		if len(m.searchMatches) > 0 {
			m.searchMatchIdx = (m.searchMatchIdx - 1 + len(m.searchMatches)) % len(m.searchMatches)
			m.scrollToMatch()
		}
		return m, nil
	}

	var cmd tea.Cmd
	m.searchInput, cmd = m.searchInput.Update(msg)
	// Update search as user types
	if m.searchInput.Value() != m.searchPattern {
		m.searchPattern = m.searchInput.Value()
		m.updateSearchMatches()
		if len(m.searchMatches) > 0 {
			m.searchMatchIdx = 0
			m.scrollToMatch()
		}
	}
	return m, cmd
}

func (m *model) handleHelpKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "?", "h", "q", "esc":
		m.showHelp = false
		return m, nil
	}
	return m, nil
}

func (m *model) handleDetailKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
		case "esc", "q":
		m.showDetail = false
		m.detailGroup = ""
		m.detailTopic = ""
		m.detailScrollOffset = 0
		m.detailPartitionSortKey = ""
		m.detailTopicMetadata = nil // Clear cached metadata
		return m, nil
	case "j", "down":
		m.detailScrollOffset++
		return m, nil
	case "k", "up":
		if m.detailScrollOffset > 0 {
			m.detailScrollOffset--
		}
		return m, nil
	case " ", "pgdown":
		// Page down in detail view
		maxRows := m.height - 16
		if maxRows < 5 {
			maxRows = 5
		}
		m.detailScrollOffset += maxRows
		return m, nil
	case "b", "pgup":
		// Page up in detail view
		maxRows := m.height - 16
		if maxRows < 5 {
			maxRows = 5
		}
		m.detailScrollOffset -= maxRows
		if m.detailScrollOffset < 0 {
			m.detailScrollOffset = 0
		}
		return m, nil
	case "l", "L":
		// Sort by lag
		if m.detailPartitionSortKey == "lag" {
			m.detailPartitionSortKey = "" // Toggle off
		} else {
			m.detailPartitionSortKey = "lag"
		}
		m.detailScrollOffset = 0 // Reset scroll when sorting changes
		return m, nil
	case "g", "G":
		// Sort by group offset
		if m.detailPartitionSortKey == "groupoffset" {
			m.detailPartitionSortKey = "" // Toggle off
		} else {
			m.detailPartitionSortKey = "groupoffset"
		}
		m.detailScrollOffset = 0 // Reset scroll when sorting changes
		return m, nil
	case "t", "T":
		// Sort by topic offset
		if m.detailPartitionSortKey == "topicoffset" {
			m.detailPartitionSortKey = "" // Toggle off
		} else {
			m.detailPartitionSortKey = "topicoffset"
		}
		m.detailScrollOffset = 0 // Reset scroll when sorting changes
		return m, nil
	case "r", "R":
		// Sort by rate
		if m.detailPartitionSortKey == "rate" {
			m.detailPartitionSortKey = "" // Toggle off
		} else {
			m.detailPartitionSortKey = "rate"
		}
		m.detailScrollOffset = 0 // Reset scroll when sorting changes
		return m, nil
	case "home":
		m.detailScrollOffset = 0
		return m, nil
	case "end":
		// Scroll to end - will be calculated in viewDetail based on partition count
		// Set a large value, viewDetail will clamp it
		m.detailScrollOffset = 999999
		return m, nil
	}
	return m, nil
}

func (m *model) updateSearchMatches() {
	if m.searchPattern == "" {
		m.searchMatches = []int{}
		return
	}

	rows := m.buildRowData()
	m.sortRowData(rows)
	m.searchMatches = []int{}

	pattern := strings.ToLower(m.searchPattern)
	for i, row := range rows {
		searchText := strings.ToLower(row.group + " " + row.topic)
		if strings.Contains(searchText, pattern) {
			m.searchMatches = append(m.searchMatches, i)
		}
	}
}

func (m *model) scrollToMatch() {
	if len(m.searchMatches) == 0 {
		return
	}

	matchIdx := m.searchMatches[m.searchMatchIdx]
	maxRows := m.height - 8
	if maxRows < 5 {
		maxRows = 5
	}

	// Scroll to show the match
	if matchIdx < m.scrollOffset {
		m.scrollOffset = matchIdx
	} else if matchIdx >= m.scrollOffset+maxRows {
		m.scrollOffset = matchIdx - maxRows + 1
	}

	m.selectedRowIdx = matchIdx
}

func (m *model) scrollToSelectedRow() {
	if m.selectedRowIdx < 0 {
		return
	}

	maxRows := m.height - 8
	if maxRows < 5 {
		maxRows = 5
	}

	// Scroll to keep selected row visible
	if m.selectedRowIdx < m.scrollOffset {
		// Selected row is above viewport - scroll up to show it
		m.scrollOffset = m.selectedRowIdx
	} else if m.selectedRowIdx >= m.scrollOffset+maxRows {
		// Selected row is below viewport - scroll down to show it
		m.scrollOffset = m.selectedRowIdx - maxRows + 1
	}
}

func (m *model) updateTable() {
	// We're now using manual rendering in viewMain, so this is just a placeholder
	// to maintain compatibility with the model structure
}

type rowData struct {
	group            string
	topic            string
	parts            int
	since            float64
	consumed         int64
	newRate          int64
	consRate         int64
	eta              string
	lag              int64
	par              float64 // Peak-to-Average Ratio
	cv               float64 // Coefficient of Variation
	hasIssues        bool
	etaColor         string
	rateColor        string
	lagColor         string
	sortGroup        string
	sortTopic        string
	remainingSec     int64
	consumptionRate  float64
}

func (m *model) renderTable() string {
	rows := m.buildRowData()
	m.sortRowData(rows)

	// Fixed column widths
	const (
		indicatorWidth = 2
		partsWidth     = 10
		sinceWidth     = 7
		consumedWidth  = 9
		newRateWidth   = 9
		consRateWidth  = 9
		etaWidth       = 13
		lagWidth       = 11
		parWidth       = 7 // PAR column width
		cvWidth        = 7 // Cv column width
	)

	// Calculate available width for group/topic columns
	// Total fixed width: indicator + parts + since + consumed + newRate + consRate + eta + lag + par + cv + spacing
	// Spaces: indicator-group, group-topic, topic-parts, parts-since, since-consumed, consumed-newRate, newRate-consRate, consRate-eta, eta-lag, lag-par, par-cv = 11 spaces
	fixedWidth := indicatorWidth + partsWidth + sinceWidth + consumedWidth + newRateWidth + consRateWidth + etaWidth + lagWidth + parWidth + cvWidth + 11 // spaces between columns
	availableWidth := m.width - fixedWidth - 2 // margin
	
	// Adaptive group/topic widths with minimums
	minGroupWidth := 10
	minTopicWidth := 10
	maxGroupWidth := 45
	maxTopicWidth := 30
	
	if availableWidth < minGroupWidth + minTopicWidth + 1 {
		availableWidth = minGroupWidth + minTopicWidth + 1 // Ensure minimum space
	}
	
	// Default widths
	groupWidth := maxGroupWidth
	topicWidth := maxTopicWidth
	
	// If terminal is narrow, shrink columns proportionally
	if availableWidth < (maxGroupWidth + maxTopicWidth) {
		// Shrink proportionally, but maintain minimums
		// Reserve space for both columns with minimums
		minTotalWidth := minGroupWidth + minTopicWidth + 1 // +1 for space between
		if availableWidth < minTotalWidth {
			// Terminal is too narrow, use minimums
			groupWidth = minGroupWidth
			topicWidth = minTopicWidth
		} else {
			// Shrink proportionally
			ratio := float64(availableWidth) / float64(maxGroupWidth + maxTopicWidth)
			groupWidth = int(float64(maxGroupWidth) * ratio)
			topicWidth = int(float64(maxTopicWidth) * ratio)
			
			// Ensure minimums
			if groupWidth < minGroupWidth {
				groupWidth = minGroupWidth
			}
			if topicWidth < minTopicWidth {
				topicWidth = minTopicWidth
			}
			
			// Adjust if total exceeds available width
			if groupWidth + topicWidth + 1 > availableWidth {
				// Prioritize group width, shrink topic if needed
				if groupWidth > minGroupWidth {
					groupWidth = availableWidth - topicWidth - 1
					if groupWidth < minGroupWidth {
						groupWidth = minGroupWidth
						topicWidth = availableWidth - groupWidth - 1
						if topicWidth < minTopicWidth {
							topicWidth = minTopicWidth
						}
					}
				} else {
					topicWidth = availableWidth - groupWidth - 1
					if topicWidth < minTopicWidth {
						topicWidth = minTopicWidth
					}
				}
			}
		}
	}

	var b strings.Builder

	// ANSI color codes for manual coloring
	brightWhite := "\033[1;37m"
	green := "\033[1;32m"
	cyan := "\033[36m"
	reverse := "\033[7m"
	reset := "\033[0m"

	// Helper function to format header column with hotkey highlighting
	// Format with plain text first to get correct width, then apply colors
	formatHeaderCol := func(text string, hotkey string, sortKey string, width int, leftAlign bool) string {
		// First format with plain text to get correct width
		var formatted string
		if leftAlign {
			formatted = fmt.Sprintf("%-*s", width, text)
		} else {
			formatted = fmt.Sprintf("%*s", width, text)
		}
		
		// Now find the text in the formatted string and replace with colored version
		hotkeyIdx := strings.Index(strings.ToLower(text), strings.ToLower(hotkey))
		
		var coloredText string
		if hotkeyIdx == -1 {
			coloredText = brightWhite + text + reset
		} else {
			before := text[:hotkeyIdx]
			hotkeyChar := text[hotkeyIdx : hotkeyIdx+1]
			after := text[hotkeyIdx+1:]
			
			if m.sortKey == sortKey {
				// Reverse video for sorted column hotkey
				coloredText = brightWhite + before + reverse + green + hotkeyChar + reset + brightWhite + after + reset
			} else {
				// Normal highlighting for hotkey
				coloredText = brightWhite + before + green + hotkeyChar + brightWhite + after + reset
			}
		}
		
		// Replace the plain text with colored version in the formatted string
		return strings.Replace(formatted, text, coloredText, 1)
	}

	// Build 2-row header matching Python version
	// Row 1 - top line (only for multi-line headers)
	consumedRateTopPlain := fmt.Sprintf("%*s", consRateWidth, "Consumed")
	var consumedRateTopCol string
	if m.sortKey == "rate" {
		consumedRateTopCol = strings.Replace(consumedRateTopPlain, "Consumed", brightWhite+reverse+green+"C"+reset+brightWhite+"onsumed"+reset, 1)
	} else {
		consumedRateTopCol = strings.Replace(consumedRateTopPlain, "Consumed", brightWhite+green+"C"+brightWhite+"onsumed"+reset, 1)
	}
	
	newRateTopPlain := fmt.Sprintf("%*s", newRateWidth, "New topic")
	var newRateTopCol string
	if m.sortKey == "newrate" {
		newRateTopCol = strings.Replace(newRateTopPlain, "New topic", brightWhite+reverse+green+"N"+reset+brightWhite+"ew topic"+reset, 1)
	} else {
		newRateTopCol = strings.Replace(newRateTopPlain, "New topic", brightWhite+green+"N"+brightWhite+"ew topic"+reset, 1)
	}
	
	row1 := fmt.Sprintf("%-*s %-*s %-*s %*s %*s %*s %s %s %*s %*s %*s %*s",
		indicatorWidth, "",
		groupWidth, "",
		topicWidth, "",
		partsWidth, "",
		sinceWidth, "Since",
		consumedWidth, "Events",
		newRateTopCol,
		consumedRateTopCol,
		etaWidth, "",
		lagWidth, "",
		parWidth, "",
		cvWidth, "")

	// Row 2 - build each column separately
	groupCol := formatHeaderCol("Group", "G", "group", groupWidth, true)
	
	// Topic header - special handling since hotkey is "o" not "T"
	var topicCol string
	topicPlain := fmt.Sprintf("%-*s", topicWidth, "Topic")
	if m.sortKey == "topic" {
		topicCol = strings.Replace(topicPlain, "Topic", brightWhite+"T"+reverse+green+"o"+reset+brightWhite+"pic"+reset, 1)
	} else {
		topicCol = strings.Replace(topicPlain, "Topic", brightWhite+"T"+green+"o"+brightWhite+"pic"+reset, 1)
	}
	
	partsCol := formatHeaderCol("Partitions", "P", "partitions", partsWidth, true)
	sincePlain := fmt.Sprintf("%*s", sinceWidth, "(sec)")
	sinceCol := strings.Replace(sincePlain, "(sec)", brightWhite+"(sec)"+reset, 1)
	consumedTotalPlain := fmt.Sprintf("%*s", consumedWidth, "Consumed")
	consumedTotalCol := strings.Replace(consumedTotalPlain, "Consumed", brightWhite+"Consumed"+reset, 1)
	newRatePlain := fmt.Sprintf("%*s", newRateWidth, "evts/sec")
	newRateCol := strings.Replace(newRatePlain, "evts/sec", brightWhite+"evts/sec"+reset, 1)
	consumedRatePlain := fmt.Sprintf("%*s", consRateWidth, "evts/sec")
	consumedRateCol := strings.Replace(consumedRatePlain, "evts/sec", brightWhite+"evts/sec"+reset, 1)
	etaCol := formatHeaderCol("Time Left", "T", "eta", etaWidth, false)
	lagCol := formatHeaderCol("Lag", "L", "lag", lagWidth, false)
	parCol := formatHeaderCol("PAR", "A", "par", parWidth, false)
	cvCol := formatHeaderCol("Cv", "V", "cv", cvWidth, false)

	indicatorCol := fmt.Sprintf("%-*s", indicatorWidth, "")
	row2 := indicatorCol + " " + groupCol + " " + topicCol + " " + partsCol + " " + sinceCol + " " + consumedTotalCol + " " + newRateCol + " " + consumedRateCol + " " + etaCol + " " + lagCol + " " + parCol + " " + cvCol

	// Print header row 1 (bright white)
	b.WriteString(brightWhite + row1 + reset + "\n")

	// Print header row 2 (white with highlighted hotkeys)
	b.WriteString(row2 + "\n")

	// Header underline
	totalWidth := indicatorWidth + groupWidth + topicWidth + partsWidth + sinceWidth + consumedWidth + newRateWidth + consRateWidth + etaWidth + lagWidth + parWidth + cvWidth + 11
	b.WriteString(strings.Repeat("─", totalWidth))
	b.WriteString("\n")

	// Calculate how many rows to show with scroll support
	maxRows := m.height - 8 // Reserve space for 2-row header
	if maxRows < 5 {
		maxRows = 5
	}

	// Apply scroll offset
	startRow := m.scrollOffset
	if startRow >= len(rows) {
		startRow = len(rows) - 1
	}
	if startRow < 0 {
		startRow = 0
	}

	endRow := startRow + maxRows
	if endRow > len(rows) {
		endRow = len(rows)
	}

	// Apply follow mode - auto-scroll to highest lag
	if m.followMode && len(rows) > 0 {
		maxLagIdx := 0
		maxLag := rows[0].lag
		for i, r := range rows {
			if r.lag > maxLag {
				maxLag = r.lag
				maxLagIdx = i
			}
		}
		if maxLagIdx >= maxRows {
			m.scrollOffset = maxLagIdx - maxRows + 1
			m.selectedRowIdx = maxLagIdx
		} else {
			m.scrollOffset = 0
			m.selectedRowIdx = maxLagIdx
		}
		startRow = m.scrollOffset
		if startRow >= len(rows) {
			startRow = len(rows) - 1
		}
		if startRow < 0 {
			startRow = 0
		}
		endRow = startRow + maxRows
		if endRow > len(rows) {
			endRow = len(rows)
		}
	}

	// Render rows with scroll
	for i := startRow; i < endRow; i++ {
		row := rows[i]

		// Marquee scrolling for long names
		// Use the actual row index in the sorted array for consistent marquee per row
		group := m.marqueeText(row.group, groupWidth, i)
		topic := m.marqueeText(row.topic, topicWidth, i)

		// Format values (plain text) and ensure they fit their column widths
		partsStr := fmt.Sprintf("%d", row.parts)
		if len(partsStr) > partsWidth {
			partsStr = partsStr[:partsWidth]
		}
		
		sinceStr := fmt.Sprintf("%.1fs", row.since)
		if len(sinceStr) > sinceWidth {
			sinceStr = sinceStr[:sinceWidth]
		}
		
		consumedStr := formatNumber(m.showFullNumbers, row.consumed)
		if len(consumedStr) > consumedWidth {
			consumedStr = consumedStr[:consumedWidth]
		}
		
		newRateStr := formatNumber(m.showFullNumbers, row.newRate)
		if len(newRateStr) > newRateWidth {
			newRateStr = newRateStr[:newRateWidth]
		}
		
		consRateStr := formatNumber(m.showFullNumbers, row.consRate)
		if len(consRateStr) > consRateWidth {
			consRateStr = consRateStr[:consRateWidth]
		}
		
		etaStr := row.eta
		if len(etaStr) > etaWidth {
			etaStr = etaStr[:etaWidth]
		}
		
		lagStr := formatNumber(m.showFullNumbers, row.lag)
		if len(lagStr) > lagWidth {
			lagStr = lagStr[:lagWidth]
		}
		
		// Format PAR (Peak-to-Average Ratio)
		parStr := "-"
		if row.par > 0 {
			parStr = fmt.Sprintf("%.2f", row.par)
		}
		if len(parStr) > parWidth {
			parStr = parStr[:parWidth]
		}
		
		// Format Cv (Coefficient of Variation)
		cvStr := "-"
		if row.cv > 0 {
			cvStr = fmt.Sprintf("%.2f", row.cv)
		}
		if len(cvStr) > cvWidth {
			cvStr = cvStr[:cvWidth]
		}
		
		// Check if this row matches search (highlight if search pattern exists, even if dialog is closed)
		isSearchMatch := false
		if m.searchPattern != "" && len(m.searchMatches) > 0 {
			for _, matchIdx := range m.searchMatches {
				if matchIdx == i {
					isSearchMatch = true
					break
				}
			}
		}
		// Check if this is the selected row (for detail view)
		isSelected := (m.selectedRowIdx >= 0 && i == m.selectedRowIdx)

		// Get color codes
		rateColorCode := getColorCode(row.rateColor)
		etaColorCode := getColorCode(row.etaColor)
		
		// Get Cv color based on thresholds: green (closer to 0), yellow (0.5-1), red (>1)
		cvColorCode := colorReset
		if row.cv > 0 {
			if row.cv < 0.5 {
				cvColorCode = "\033[32m" // Green: good (closer to 0)
			} else if row.cv <= 1.0 {
				cvColorCode = "\033[33m" // Yellow: high skew (0.5-1)
			} else {
				cvColorCode = "\033[31m" // Red: critical skew (>1)
			}
		}

		// Determine background color
		// Priority: issues (red) > search match (green)
		var bgCode string
		if row.hasIssues {
			bgCode = "\033[48;5;52m" // Dark red background for issues
		} else if isSearchMatch {
			bgCode = "\033[48;5;22m" // Dark green background for search match
		} else {
			bgCode = ""
		}
		resetCode := "\033[0m"

		// Indicator column: ">" for selected row, " " for others
		indicator := " "
		if isSelected {
			indicator = ">"
		}
		indicatorStr := fmt.Sprintf("%-*s", indicatorWidth, indicator)

		// Ensure group and topic are exactly the right width
		groupPadded := group
		if len(group) > groupWidth {
			groupPadded = group[:groupWidth]
		} else if len(group) < groupWidth {
			// Pad with spaces
			groupPadded = group + strings.Repeat(" ", groupWidth-len(group))
		}
		
		topicPadded := topic
		if len(topic) > topicWidth {
			topicPadded = topic[:topicWidth]
		} else if len(topic) < topicWidth {
			// Pad with spaces
			topicPadded = topic + strings.Repeat(" ", topicWidth-len(topic))
		}

		// Ensure all strings fit their widths before formatting
		partsStrPadded := partsStr
		if len(partsStrPadded) < partsWidth {
			partsStrPadded = strings.Repeat(" ", partsWidth-len(partsStrPadded)) + partsStrPadded
		}
		sinceStrPadded := sinceStr
		if len(sinceStrPadded) < sinceWidth {
			sinceStrPadded = strings.Repeat(" ", sinceWidth-len(sinceStrPadded)) + sinceStrPadded
		}
		consumedStrPadded := consumedStr
		if len(consumedStrPadded) < consumedWidth {
			consumedStrPadded = strings.Repeat(" ", consumedWidth-len(consumedStrPadded)) + consumedStrPadded
		}
		newRateStrPadded := newRateStr
		if len(newRateStrPadded) < newRateWidth {
			newRateStrPadded = strings.Repeat(" ", newRateWidth-len(newRateStrPadded)) + newRateStrPadded
		}
		consRateStrPadded := consRateStr
		if len(consRateStrPadded) < consRateWidth {
			consRateStrPadded = strings.Repeat(" ", consRateWidth-len(consRateStrPadded)) + consRateStrPadded
		}
		etaStrPadded := etaStr
		if len(etaStrPadded) < etaWidth {
			etaStrPadded = strings.Repeat(" ", etaWidth-len(etaStrPadded)) + etaStrPadded
		}
		lagStrPadded := lagStr
		if len(lagStrPadded) < lagWidth {
			lagStrPadded = strings.Repeat(" ", lagWidth-len(lagStrPadded)) + lagStrPadded
		}
		
		parStrPadded := parStr
		if len(parStrPadded) < parWidth {
			parStrPadded = strings.Repeat(" ", parWidth-len(parStrPadded)) + parStrPadded
		}
		
		cvStrPadded := cvStr
		if len(cvStrPadded) < cvWidth {
			cvStrPadded = strings.Repeat(" ", cvWidth-len(cvStrPadded)) + cvStrPadded
		}

		// Build row with background spanning entire width (no format specifiers, just concatenation)
		rowText := indicatorStr + " " + groupPadded + " " + topicPadded + " " + partsStrPadded + " " + sinceStrPadded + " " + consumedStrPadded + " " + newRateStrPadded + " " + consRateStrPadded + " " + etaStrPadded + " " + lagStrPadded + " " + parStrPadded + " " + cvStrPadded

		// Apply background and colors
		if bgCode != "" {
			b.WriteString(bgCode + rowText + resetCode + "\n")
		} else {
			// Normal row with colored cells
			indicatorColor := brightWhite
			if isSelected {
				indicatorColor = green // Green ">" for selected row
			}
			// Ensure group and topic are exactly the right width
			groupPadded := group
			if len(group) > groupWidth {
				groupPadded = group[:groupWidth]
			} else if len(group) < groupWidth {
				// Pad with spaces
				groupPadded = group + strings.Repeat(" ", groupWidth-len(group))
			}
			
			topicPadded := topic
			if len(topic) > topicWidth {
				topicPadded = topic[:topicWidth]
			} else if len(topic) < topicWidth {
				// Pad with spaces
				topicPadded = topic + strings.Repeat(" ", topicWidth-len(topic))
			}
			
			// Build row with colors (no format specifiers to avoid BADWIDTH errors)
			rowLine := indicatorColor + indicatorStr + reset + " " +
				cyan + groupPadded + reset + " " +
				cyan + topicPadded + reset + " " +
				partsStrPadded + " " +
				sinceStrPadded + " " +
				consumedStrPadded + " " +
				newRateStrPadded + " " +
				rateColorCode + consRateStrPadded + reset + " " +
				etaColorCode + etaStrPadded + reset + " " +
				lagStrPadded + " " +
				parStrPadded + " " +
				cvColorCode + cvStrPadded + reset + "\n"
			b.WriteString(rowLine)
		}
	}

	return b.String()
}

func (m *model) getViewportInfo() string {
	rows := m.buildRowData()
	maxRows := m.height - 8
	if maxRows < 5 {
		maxRows = 5
	}

	if len(rows) <= maxRows {
		return ""
	}

	startRow := m.scrollOffset
	if startRow >= len(rows) {
		startRow = len(rows) - 1
	}
	if startRow < 0 {
		startRow = 0
	}

	endRow := startRow + maxRows
	if endRow > len(rows) {
		endRow = len(rows)
	}

	percentage := int(float64(endRow) / float64(len(rows)) * 100)
	if percentage > 100 {
		percentage = 100
	}

	return fmt.Sprintf("Rows %d-%d of %d [%d%%]", startRow+1, endRow, len(rows), percentage)
}

// getColorCode returns ANSI color code for color name
func getColorCode(color string) string {
	switch color {
	case "green":
		return "\033[32m"
	case "yellow":
		return "\033[33m"
	case "magenta":
		return "\033[35m"
	case "red":
		return "\033[31m"
	case "cyan":
		return "\033[36m"
	case "white":
		return "\033[37m"
	default:
		return "\033[0m"
	}
}

// healthCheck determines the health status and colors for a row
func healthCheck(lag int64, rate *types.RateStats) (hasIssues bool, etaColor, rateColor, lagColor string) {
	// Default colors
	etaColor = "white"
	rateColor = "green"
	lagColor = "white"
	hasIssues = false

	// ETA (Time Left) health check
	// Python highlights rows ONLY based on ETA status, when rs >= 120 (> 2 minutes)
	rs := rate.RemainingSec
	if rs >= 0 && rs < 60 {
		// OK: ETA < 1 minute
		etaColor = "green"
	} else if rs >= 60 && rs < 120 {
		// OK: ETA < 2 minutes
		etaColor = "yellow"
	} else if rs >= 120 && rs < 600 {
		// WARNING: ETA 2-10 minutes - HIGHLIGHT ROW
		etaColor = "yellow"
		hasIssues = true
	} else if rs >= 600 && rs < 7200 {
		// ERROR: ETA 10m-2h - HIGHLIGHT ROW
		etaColor = "magenta"
		hasIssues = true
	} else if rs >= 7200 {
		// CRITICAL: ETA > 2h - HIGHLIGHT ROW
		etaColor = "red"
		hasIssues = true
	} else if rs == -1 {
		// No consumption - only highlight if there's incoming data or existing lag
		etaColor = "red"
		// Only highlight if there's a real problem (data arriving or lag exists)
		if rate.EventsArrivalRate > 1.0 || lag > 1000 {
			hasIssues = true
		}
	}

	// Rate health check - colors cells but does NOT trigger row highlighting
	if lag > 0 && rate.EventsConsumptionRate == 0 {
		// No consumption with lag - color cell red (no row highlight)
		rateColor = "red"
	} else if rate.EventsArrivalRate > 5*rate.EventsConsumptionRate {
		// ERROR: Arrival rate > 5x consumption rate
		rateColor = "red"
	} else if rate.EventsArrivalRate > 2*rate.EventsConsumptionRate {
		// WARNING: Arrival rate > 2x consumption rate
		rateColor = "yellow"
	}

	return hasIssues, etaColor, rateColor, lagColor
}

func (m *model) buildRowData() []*rowData {
	var rows []*rowData

	for groupID, topicRates := range m.rates {
		for topic, rate := range topicRates {
			lag, exists := m.kd.GroupLags[groupID][topic]
			if !exists {
				continue
			}

			// Check health status
			hasIssues, etaColor, rateColor, lagColor := healthCheck(lag.Sum, rate)

			// Apply only-issues filter
			if m.params.KafkaOnlyIssues {
				if !hasIssues {
					continue
				}
			}

			// Apply runtime filter pattern
			if m.filterPattern != "" {
				matched, _ := regexp.MatchString(m.filterPattern, groupID)
				if !matched {
					continue
				}
			}

			// Apply topic filter pattern
			if m.topicFilterPattern != "" {
				matched, _ := regexp.MatchString(m.topicFilterPattern, topic)
				if !matched {
					continue
				}
			}

			groupName := groupID
			topicName := topic

			if m.params.Anonymize {
				groupName = fmt.Sprintf("group %06d", hashString(groupID)%1000000)
				topicName = fmt.Sprintf("topic %06d", hashString(topic)%1000000)
			}

			row := &rowData{
				group:           groupName,
				topic:           topicName,
				parts:           len(lag.PartitionLags),
				since:           rate.TimeDelta,
				consumed:        rate.EventsConsumed,
				newRate:         int64(rate.EventsArrivalRate),
				consRate:        int64(rate.EventsConsumptionRate),
				eta:             rate.RemainingHMS,
				lag:             lag.Sum,
				par:             lag.PAR,
				cv:              lag.Cv,
				hasIssues:       hasIssues,
				etaColor:        etaColor,
				rateColor:       rateColor,
				lagColor:        lagColor,
				sortGroup:       groupID,
				sortTopic:       topic,
				remainingSec:    rate.RemainingSec,
				consumptionRate: rate.EventsConsumptionRate,
			}

			rows = append(rows, row)
		}
	}

	return rows
}

// formatNumber formats numbers in human-readable SI units (K, M, G, etc.)
func formatNumber(showFull bool, n int64) string {
	if showFull {
		return fmt.Sprintf("%d", n)
	}

	if n < 0 {
		return fmt.Sprintf("-%s", formatNumber(false, -n))
	}

	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	units := []string{"", "K", "M", "G", "T", "P"}
	exp := 0
	val := float64(n)

	for val >= 1000 && exp < len(units)-1 {
		val /= 1000
		exp++
	}

	if val >= 100 {
		return fmt.Sprintf("%.0f%s", val, units[exp])
	} else if val >= 10 {
		return fmt.Sprintf("%.1f%s", val, units[exp])
	}
	return fmt.Sprintf("%.2f%s", val, units[exp])
}

func formatRate(showFull bool, rate float64) string {
	if rate < 0 {
		return "-"
	}
	if rate == 0 {
		return "0"
	}
	
	if showFull {
		// Plain number format - no units
		if rate == float64(int64(rate)) {
			return fmt.Sprintf("%.0f", rate)
		}
		return fmt.Sprintf("%.1f", rate)
	}
	
	// Human-readable format with units
	// Format as integer if whole number, otherwise with 1 decimal place
	if rate < 1000 {
		if rate == float64(int64(rate)) {
			return fmt.Sprintf("%.0f", rate)
		}
		return fmt.Sprintf("%.1f", rate)
	}

	units := []string{"", "K", "M", "G", "T", "P"}
	exp := 0
	val := rate

	for val >= 1000 && exp < len(units)-1 {
		val /= 1000
		exp++
	}

	if val >= 100 {
		return fmt.Sprintf("%.0f%s", val, units[exp])
	} else if val >= 10 {
		return fmt.Sprintf("%.1f%s", val, units[exp])
	}
	return fmt.Sprintf("%.2f%s", val, units[exp])
}

func (m *model) sortRowData(rows []*rowData) {
	if m.sortKey == "" {
		// Even without explicit sort, maintain stable order by group+topic
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].sortGroup != rows[j].sortGroup {
				return rows[i].sortGroup < rows[j].sortGroup
			}
			return rows[i].sortTopic < rows[j].sortTopic
		})
		return
	}

	// Use stable sort to maintain order for equal values
	sort.SliceStable(rows, func(i, j int) bool {
		var less bool

		switch m.sortKey {
		case "group":
			less = rows[i].sortGroup < rows[j].sortGroup
			// Secondary sort by topic if groups are equal
			if rows[i].sortGroup == rows[j].sortGroup {
				less = rows[i].sortTopic < rows[j].sortTopic
			}
		case "topic":
			less = rows[i].sortTopic < rows[j].sortTopic
			// Secondary sort by group if topics are equal
			if rows[i].sortTopic == rows[j].sortTopic {
				less = rows[i].sortGroup < rows[j].sortGroup
			}
		case "partitions":
			less = rows[i].parts < rows[j].parts
			// Secondary sort by group+topic if parts are equal
			if rows[i].parts == rows[j].parts {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		case "eta":
			less = rows[i].remainingSec < rows[j].remainingSec
			if rows[i].remainingSec == rows[j].remainingSec {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		case "lag":
			less = rows[i].lag < rows[j].lag
			if rows[i].lag == rows[j].lag {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		case "rate":
			less = rows[i].consumptionRate < rows[j].consumptionRate
			if rows[i].consumptionRate == rows[j].consumptionRate {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		case "newrate":
			less = rows[i].newRate < rows[j].newRate
			if rows[i].newRate == rows[j].newRate {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		case "par":
			less = rows[i].par < rows[j].par
			if rows[i].par == rows[j].par {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		case "cv":
			less = rows[i].cv < rows[j].cv
			if rows[i].cv == rows[j].cv {
				if rows[i].sortGroup != rows[j].sortGroup {
					less = rows[i].sortGroup < rows[j].sortGroup
				} else {
					less = rows[i].sortTopic < rows[j].sortTopic
				}
			}
		default:
			return false
		}

		if m.sortReverse {
			return !less
		}
		return less
	})
}

func (m *model) addWarning(msg string) {
	m.warnings = append(m.warnings, msg)
	if len(m.warnings) > 10 {
		m.warnings = m.warnings[len(m.warnings)-10:]
	}
}

func tickCmd(pollPeriod time.Duration) tea.Cmd {
	return tea.Tick(pollPeriod, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func animTickCmd() tea.Cmd {
	// Fast ticker for marquee animation: 200ms
	return tea.Tick(200*time.Millisecond, func(t time.Time) tea.Msg {
		return animTickMsg(t)
	})
}

func loadData(admin *kafka.AdminClient, params *types.Params) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()

		// First snapshot
		kd1, err := kafka.CalcLag(ctx, admin, params)
		if err != nil {
			return dataMsg{err: err}
		}

		// Wait for poll period
		time.Sleep(time.Duration(params.KafkaPollPeriod) * time.Second)

		// Second snapshot
		kd2, err := kafka.CalcLag(ctx, admin, params)
		if err != nil {
			return dataMsg{err: err}
		}

		// Calculate rates
		rates := kafka.CalcRate(kd1, kd2, params)

		return dataMsg{
			kd:    kd2,
			rates: rates,
		}
	}
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// marqueeText creates a scrolling marquee effect for long text
// Always returns exactly 'width' characters
func (m *model) marqueeText(text string, width int, rowIndex int) string {
	if width <= 0 {
		return ""
	}
	
	if len(text) <= width {
		// Text fits, pad to exact width with spaces
		if len(text) < width {
			return text + strings.Repeat(" ", width-len(text))
		}
		return text
	}

	// Time-based marquee: scroll left to right smoothly
	// All rows start scrolling at the same time (synchronized)
	elapsed := time.Since(m.startTime)
	
	// Scroll speed: 4 characters per 300ms animation tick
	scrollSpeed := time.Millisecond * 300
	charsPerTick := 4 // Characters to scroll per animation tick
	
	// All rows start at the same time (no baseOffset for synchronization)
	// Calculate scroll position in characters (4 chars per tick)
	charsScrolled := int(elapsed / scrollSpeed) * charsPerTick
	
	// Scrollable range: from 0 to len(text) - width (so we can show full width)
	maxScrollPos := len(text) - width
	if maxScrollPos < 0 {
		// Text is shorter than width (shouldn't happen, but handle it)
		result := text
		if len(result) < width {
			return result + strings.Repeat(" ", width-len(result))
		}
		return result[:width]
	}
	
	// Add a pause at the end (show beginning for a bit before restarting)
	// Pause duration: show beginning for 2 seconds before restarting
	// Calculate pause in terms of scroll ticks
	pauseTicks := int((2 * time.Second) / scrollSpeed) // 2 seconds worth of ticks
	pauseChars := pauseTicks * charsPerTick
	totalScrollable := maxScrollPos + 1 + pauseChars
	
	// Calculate scroll position with wrap-around
	scrollPos := charsScrolled % totalScrollable
	
	// If we're in the pause phase (showing beginning)
	if scrollPos > maxScrollPos {
		// Show beginning of text
		result := text[:width]
		return result
	}
	
	// Normal scrolling - show contiguous portion of text starting from scrollPos
	// Ensure we don't go out of bounds
	if scrollPos < 0 {
		scrollPos = 0
	}
	if scrollPos > maxScrollPos {
		scrollPos = maxScrollPos
	}
	
	// Extract exactly 'width' characters starting from scrollPos
	result := text[scrollPos : scrollPos+width]
	return result
}
