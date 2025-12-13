package ui

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
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
	showHelp        bool
	followMode      bool
	showFullNumbers bool
	lastUpdateTime  time.Time
}

type tickMsg time.Time
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
		selectedRowIdx:   -1, // Initialize to -1 so no row is selected by default
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
	b.WriteString("  Home/End     Jump to top/bottom\n")
	b.WriteString("\n")

	b.WriteString("Actions:\n")
	b.WriteString("  Q             Quit\n")
	b.WriteString("  Space         Pause/resume updates\n")
	b.WriteString("  +/-           Adjust refresh rate\n")
	b.WriteString("  F             Filter consumer groups\n")
	b.WriteString("  X             Filter topics\n")
	b.WriteString("  /             Search\n")
	b.WriteString("  ? or H        Show this help\n")
	b.WriteString("  Enter         View partition details\n")
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

	b.WriteString("Other:\n")
	b.WriteString("  U             Toggle full numbers\n")
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

	b.WriteString("\n")
	b.WriteString(headerStyle.Render(fmt.Sprintf("Partition Details: %s / %s", m.detailGroup, m.detailTopic)))
	b.WriteString("\n\n")

	b.WriteString(fmt.Sprintf("Total Lag: %s\n", formatNumber(m.showFullNumbers, lagStats.Sum)))
	b.WriteString(fmt.Sprintf("Min: %s, Max: %s, Mean: %.1f, Median: %s\n",
		formatNumber(m.showFullNumbers, lagStats.Min),
		formatNumber(m.showFullNumbers, lagStats.Max),
		lagStats.Mean,
		formatNumber(m.showFullNumbers, lagStats.Median)))
	b.WriteString(fmt.Sprintf("Consumption Rate: %s evts/sec\n", formatNumber(m.showFullNumbers, int64(rateStats.EventsConsumptionRate))))
	b.WriteString(fmt.Sprintf("Arrival Rate: %s evts/sec\n", formatNumber(m.showFullNumbers, int64(rateStats.EventsArrivalRate))))
	b.WriteString(fmt.Sprintf("ETA: %s\n", rateStats.RemainingHMS))
	b.WriteString("\n")

	b.WriteString("Partition | Group Offset | Topic Offset | Lag\n")
	b.WriteString("----------|--------------|--------------|----\n")

	// Sort partitions for display
	partitions := make([]int32, 0, len(lagStats.PartitionLags))
	for part := range lagStats.PartitionLags {
		partitions = append(partitions, part)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	// Calculate how many rows to show with scroll support
	maxRows := m.height - 12 // Reserve space for header and summary
	if maxRows < 5 {
		maxRows = 5
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
	if startIdx > len(partitions)-maxRows && len(partitions) > maxRows {
		startIdx = len(partitions) - maxRows
		if startIdx < 0 {
			startIdx = 0
		}
		m.detailScrollOffset = startIdx
	}

	endIdx := startIdx + maxRows
	if endIdx > len(partitions) {
		endIdx = len(partitions)
	}

	// Render partitions with scroll
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

		b.WriteString(fmt.Sprintf("%9d | %12s | %12s | %s\n",
			part,
			formatNumber(m.showFullNumbers, groupOffset),
			formatNumber(m.showFullNumbers, topicOffset),
			formatNumber(m.showFullNumbers, lag)))
	}

	// Show scroll info
	if len(partitions) > maxRows {
		b.WriteString(fmt.Sprintf("\nRows %d-%d of %d partitions | Use ↑↓/JK to scroll, Home/End to jump\n", startIdx+1, endIdx, len(partitions)))
	} else {
		b.WriteString(fmt.Sprintf("\n%d partitions total\n", len(partitions)))
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
		
		// Row 1: Status and actions
		header1 := colorBrightWhite + fmt.Sprintf("%s poll: %d", timeStr, m.iteration)
		if m.paused {
			header1 += colorYellow + " [PAUSED]"
		}
		header1 += colorBrightWhite + fmt.Sprintf(" | refresh: %v", m.pollPeriod)
		header1 += colorBrightWhite + " | actions: "
		header1 += colorBrightWhite + formatLegendHotkey("[Q]uit", "Q", "") + ", "
		header1 += colorBrightWhite + formatLegendHotkey("[F]ilter", "F", "") + ", "
		header1 += colorBrightWhite + formatLegendHotkey("[X]topic filter", "X", "") + ", "
		header1 += colorBrightWhite + formatLegendHotkey("[?]Help", "?", "")
		
		if m.filterPattern != "" {
			header1 += colorBrightWhite + fmt.Sprintf(" | Group: %s", m.filterPattern)
		}
		if m.topicFilterPattern != "" {
			header1 += colorBrightWhite + fmt.Sprintf(" | Topic: %s", m.topicFilterPattern)
		}
		
		// Row 2: Sorting and navigation
		header2 := colorBrightWhite + "sort-by: "
		header2 += colorBrightWhite + formatLegendHotkey("[G]roup", "G", "group") + ", "
		header2 += colorBrightWhite + formatLegendHotkey("T[o]pic", "o", "topic") + ", "
		header2 += colorBrightWhite + formatLegendHotkey("[P]artitions", "P", "partitions") + ", "
		header2 += colorBrightWhite + formatLegendHotkey("[T]ime Left", "T", "eta") + " (t/T), "
		header2 += colorBrightWhite + formatLegendHotkey("[L]ag", "L", "lag") + ", "
		header2 += colorBrightWhite + formatLegendHotkey("[N]ew topic", "N", "newrate") + ", "
		header2 += colorBrightWhite + formatLegendHotkey("[C]onsumed", "C", "rate")
		header2 += colorBrightWhite + " | scroll: " + colorBrightGreen + "↑↓" + colorBrightWhite + "/" + colorBrightGreen + "JK" + colorBrightWhite + "/PgUp/PgDn"
		header2 += colorBrightWhite + " | " + colorBrightGreen + "Space" + colorBrightWhite + " pause"
		header2 += colorBrightWhite + " | " + colorBrightGreen + "+/-" + colorBrightWhite + " rate"

		if m.sortKey != "" {
			direction := "↑"
			if m.sortReverse {
				direction = "↓"
			}
			header2 += colorBrightWhite + fmt.Sprintf(" | Sorted: %s %s", m.sortKey, direction)
		}
		
		// Count totals
		rows := m.buildRowData()
		totalGroups := make(map[string]bool)
		totalTopics := make(map[string]bool)
		for _, row := range rows {
			totalGroups[row.sortGroup] = true
			totalTopics[row.sortGroup+"|"+row.sortTopic] = true
		}
		header2 += colorBrightWhite + fmt.Sprintf(" | Groups: %d Topics: %d", len(totalGroups), len(totalTopics))

		b.WriteString("\n" + colorBold + header1 + colorReset)
		b.WriteString("\n" + colorBold + header2 + colorReset)

		// Combined status line: viewport info + loading status + update time
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

		// Add update time to status bar
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

	case " ":
		// Pause/resume updates
		m.paused = !m.paused
		if m.paused {
			m.addWarning("Updates paused - press Space to resume")
		} else {
			m.addWarning("Updates resumed")
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

	case "enter":
		// Enter detail view
		rows := m.buildRowData()
		m.sortRowData(rows)
		if m.selectedRowIdx >= 0 && m.selectedRowIdx < len(rows) {
			row := rows[m.selectedRowIdx]
			m.showDetail = true
			m.detailGroup = row.sortGroup
			m.detailTopic = row.sortTopic
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
		m.scrollOffset++
		rows := m.buildRowData()
		m.sortRowData(rows)
		if m.selectedRowIdx < len(rows)-1 {
			m.selectedRowIdx++
		}
		return m, nil

	case "k", "up":
		if m.scrollOffset > 0 {
			m.scrollOffset--
		}
		if m.selectedRowIdx > 0 {
			m.selectedRowIdx--
		}
		return m, nil

	case "pgdown":
		// Page down - scroll by page size
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		m.scrollOffset += maxRows
		if m.scrollOffset >= len(rows) {
			m.scrollOffset = len(rows) - 1
			if m.scrollOffset < 0 {
				m.scrollOffset = 0
			}
		}
		// Update selected row index
		if m.selectedRowIdx < len(rows)-1 {
			m.selectedRowIdx += maxRows
			if m.selectedRowIdx >= len(rows) {
				m.selectedRowIdx = len(rows) - 1
			}
		}
		return m, nil

	case "pgup":
		// Page up - scroll by page size
		rows := m.buildRowData()
		m.sortRowData(rows)
		maxRows := m.height - 8
		if maxRows < 5 {
			maxRows = 5
		}
		m.scrollOffset -= maxRows
		if m.scrollOffset < 0 {
			m.scrollOffset = 0
		}
		// Update selected row index
		if m.selectedRowIdx > 0 {
			m.selectedRowIdx -= maxRows
			if m.selectedRowIdx < 0 {
				m.selectedRowIdx = 0
			}
		}
		return m, nil

	case "u":
		// Toggle full numbers
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

	case "g", "G", "o", "O", "p", "P", "t", "T", "l", "L", "c", "C":
		newSortKey := ""
		switch msg.String() {
		case "g", "G":
			newSortKey = "group"
		case "o", "O":
			newSortKey = "topic"
		case "p", "P":
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
		}

		if newSortKey != "" {
			if m.sortKey == newSortKey {
				m.sortReverse = !m.sortReverse
			} else {
				m.sortKey = newSortKey
				m.sortReverse = (newSortKey == "eta" || newSortKey == "lag")
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
		return m, nil
	case "j", "down":
		m.detailScrollOffset++
		return m, nil
	case "k", "up":
		if m.detailScrollOffset > 0 {
			m.detailScrollOffset--
		}
		return m, nil
	case "pgdown":
		// Page down in detail view
		maxRows := m.height - 12
		if maxRows < 5 {
			maxRows = 5
		}
		m.detailScrollOffset += maxRows
		return m, nil
	case "pgup":
		// Page up in detail view
		maxRows := m.height - 12
		if maxRows < 5 {
			maxRows = 5
		}
		m.detailScrollOffset -= maxRows
		if m.detailScrollOffset < 0 {
			m.detailScrollOffset = 0
		}
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

	// Column widths - matching Python version
	const (
		indicatorWidth = 2   // Indicator column for selected row
		groupWidth    = 45  // Increased for longer group names
		topicWidth    = 30  // Increased for longer topic names
		partsWidth    = 10
		sinceWidth    = 7
		consumedWidth = 9
		newRateWidth  = 9
		consRateWidth = 9
		etaWidth      = 13
		lagWidth      = 11
	)

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
	
	row1 := fmt.Sprintf("%-*s %-*s %-*s %*s %*s %*s %s %s %*s %*s",
		indicatorWidth, "",
		groupWidth, "",
		topicWidth, "",
		partsWidth, "",
		sinceWidth, "Since",
		consumedWidth, "Events",
		newRateTopCol,
		consumedRateTopCol,
		etaWidth, "",
		lagWidth, "")

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

	indicatorCol := fmt.Sprintf("%-*s", indicatorWidth, "")
	row2 := indicatorCol + " " + groupCol + " " + topicCol + " " + partsCol + " " + sinceCol + " " + consumedTotalCol + " " + newRateCol + " " + consumedRateCol + " " + etaCol + " " + lagCol

	// Print header row 1 (bright white)
	b.WriteString(brightWhite + row1 + reset + "\n")

	// Print header row 2 (white with highlighted hotkeys)
	b.WriteString(row2 + "\n")

	// Header underline
	totalWidth := indicatorWidth + groupWidth + topicWidth + partsWidth + sinceWidth + consumedWidth + newRateWidth + consRateWidth + etaWidth + lagWidth + 9
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

		// Truncate long names
		group := row.group
		if len(group) > groupWidth {
			group = group[:groupWidth-3] + "..."
		}
		topic := row.topic
		if len(topic) > topicWidth {
			topic = topic[:topicWidth-3] + "..."
		}

		// Format values (plain text)
		partsStr := fmt.Sprintf("%d", row.parts)
		sinceStr := fmt.Sprintf("%.1fs", row.since)
		consumedStr := formatNumber(m.showFullNumbers, row.consumed)
		newRateStr := formatNumber(m.showFullNumbers, row.newRate)
		consRateStr := formatNumber(m.showFullNumbers, row.consRate)
		etaStr := row.eta
		lagStr := formatNumber(m.showFullNumbers, row.lag)
		
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

		// Build row with background spanning entire width
		rowText := fmt.Sprintf("%s %-*s %-*s %*s %*s %*s %*s %*s %*s %*s %*s",
			indicatorStr,
			groupWidth, group,
			topicWidth, topic,
			partsWidth, partsStr,
			sinceWidth, sinceStr,
			consumedWidth, consumedStr,
			newRateWidth, newRateStr,
			consRateWidth, consRateStr,
			etaWidth, etaStr,
			lagWidth, lagStr)

		// Apply background and colors
		if bgCode != "" {
			b.WriteString(bgCode + rowText + resetCode + "\n")
		} else {
			// Normal row with colored cells
			indicatorColor := brightWhite
			if isSelected {
				indicatorColor = green // Green ">" for selected row
			}
			b.WriteString(fmt.Sprintf("%s%s%s %s%-*s%s %s%-*s%s %*s %*s %*s %*s %s%*s%s %s%*s%s %*s\n",
				indicatorColor, indicatorStr, reset,
				cyan, groupWidth, group, reset,
				cyan, topicWidth, topic, reset,
				partsWidth, partsStr,
				sinceWidth, sinceStr,
				consumedWidth, consumedStr,
				newRateWidth, newRateStr,
				rateColorCode, consRateWidth, consRateStr, reset,
				etaColorCode, etaWidth, etaStr, reset,
				lagWidth, lagStr))
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
