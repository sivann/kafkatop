package ui

import (
	"context"
	"fmt"
	"hash/fnv"
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
	warnings        []string
	showWarnings    bool
	err             error
	quitting        bool
	ready           bool
	width           int
	height          int
	pollPeriod      time.Duration
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

	m := model{
		admin:      admin,
		params:     params,
		loading:    true,
		spinner:    s,
		filterInput: ti,
		warnings:   []string{},
		pollPeriod: time.Duration(params.KafkaPollPeriod) * time.Second,
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
		if !m.quitting {
			cmds = append(cmds, tickCmd(m.pollPeriod))
			if !m.loading && !m.showFilter {
				m.loading = true
				cmds = append(cmds, loadData(m.admin, m.params))
			}
		}

	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)
	}

	if m.showFilter {
		m.filterInput, cmd = m.filterInput.Update(msg)
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

func (m *model) viewMain() string {
	var b strings.Builder

	// Header
	timeStr := time.Now().Format("15:04:05")
	header := fmt.Sprintf("%s poll: %d | actions: [Q]uit, [F]ilter, [W]arnings, sort-by: [G]roup, T[o]pic, [P]artitions, [T]ime Left, [L]ag, [C]onsumed",
		timeStr, m.iteration)

	if m.filterPattern != "" {
		header += fmt.Sprintf(" | Filter: %s", m.filterPattern)
	}

	if m.sortKey != "" {
		direction := "↑"
		if m.sortReverse {
			direction = "↓"
		}
		header += fmt.Sprintf(" | Sorted by: %s %s", m.sortKey, direction)
	}

	b.WriteString(headerStyle.Render(header))
	b.WriteString("\n\n")

	// Warnings panel
	if m.showWarnings && len(m.warnings) > 0 {
		warningText := strings.Join(m.warnings, "\n")
		warningPanel := lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("196")).
			Padding(0, 1).
			Render(warningText)
		b.WriteString(warningPanel)
		b.WriteString("\n\n")
	}

	// Table
	if m.kd != nil && m.rates != nil {
		b.WriteString(m.renderTable())

		// Show loading indicator at bottom if currently loading
		if m.loading {
			loadingText := "Refreshing data..."
			if m.loadingStatus != "" {
				loadingText = m.loadingStatus
			}
			b.WriteString(fmt.Sprintf("\n%s %s", m.spinner.View(), loadingText))
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

	case "f":
		m.showFilter = true
		m.filterInput.Focus()
		return m, nil

	case "w":
		m.showWarnings = !m.showWarnings
		return m, nil

	case "g", "o", "p", "t", "l", "c":
		newSortKey := ""
		switch msg.String() {
		case "g":
			newSortKey = "group"
		case "o":
			newSortKey = "topic"
		case "p":
			newSortKey = "partitions"
		case "t":
			newSortKey = "eta"
		case "l":
			newSortKey = "lag"
		case "c":
			newSortKey = "rate"
		}

		if m.sortKey == newSortKey {
			m.sortReverse = !m.sortReverse
		} else {
			m.sortKey = newSortKey
			m.sortReverse = (newSortKey == "eta")
		}

		m.updateTable()
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
		groupWidth    = 35
		topicWidth    = 25
		partsWidth    = 6
		sinceWidth    = 7
		consumedWidth = 9
		newRateWidth  = 9
		consRateWidth = 9
		etaWidth      = 11
		lagWidth      = 11
	)

	var b strings.Builder

	// ANSI color codes for manual coloring
	green := "\033[1;32m"
	cyan := "\033[36m"
	reset := "\033[0m"

	// Build header text (plain text for proper alignment)
	groupH := "Group"
	topicH := "Topic"
	partsH := "Parts"
	sinceH := "Since"
	consumedH := "Consumed"
	newRateH := "New/sec"
	consRateH := "Cons/sec"
	etaH := "Time Left"
	lagH := "Lag"

	// Print header with proper spacing
	b.WriteString(fmt.Sprintf("%s%-*s %-*s %*s %*s %*s %*s %*s %*s %*s%s\n",
		green,
		groupWidth, groupH,
		topicWidth, topicH,
		partsWidth, partsH,
		sinceWidth, sinceH,
		consumedWidth, consumedH,
		newRateWidth, newRateH,
		consRateWidth, consRateH,
		etaWidth, etaH,
		lagWidth, lagH,
		reset))

	// Header underline
	totalWidth := groupWidth + topicWidth + partsWidth + sinceWidth + consumedWidth + newRateWidth + consRateWidth + etaWidth + lagWidth + 8
	b.WriteString(strings.Repeat("─", totalWidth))
	b.WriteString("\n")

	// Calculate how many rows to show
	maxRows := m.height - 7
	if maxRows < 5 {
		maxRows = 5
	}
	if maxRows > len(rows) {
		maxRows = len(rows)
	}

	// Render rows
	for i := 0; i < maxRows && i < len(rows); i++ {
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
		consumedStr := formatNumber(row.consumed)
		newRateStr := formatNumber(row.newRate)
		consRateStr := formatNumber(row.consRate)
		etaStr := row.eta
		lagStr := formatNumber(row.lag)

		// Get color codes
		rateColorCode := getColorCode(row.rateColor)
		etaColorCode := getColorCode(row.etaColor)

		if row.hasIssues {
			// Dark red background for entire row
			bgCode := "\033[48;5;52m" // background color 52 (dark red)
			resetCode := "\033[0m"

			// Build row with background spanning entire width
			rowText := fmt.Sprintf("%-*s %-*s %*s %*s %*s %*s %*s %*s %*s",
				groupWidth, group,
				topicWidth, topic,
				partsWidth, partsStr,
				sinceWidth, sinceStr,
				consumedWidth, consumedStr,
				newRateWidth, newRateStr,
				consRateWidth, consRateStr,
				etaWidth, etaStr,
				lagWidth, lagStr)

			// Apply background to entire row
			b.WriteString(bgCode + rowText + resetCode + "\n")
		} else {
			// Normal row with colored cells
			b.WriteString(fmt.Sprintf("%s%-*s%s %s%-*s%s %*s %*s %*s %*s %s%*s%s %s%*s%s %*s\n",
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
func formatNumber(n int64) string {
	if n < 0 {
		return fmt.Sprintf("-%s", formatNumber(-n))
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
		rates := kafka.CalcRate(kd1, kd2)

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
