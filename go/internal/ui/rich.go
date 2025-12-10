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
	"github.com/dustin/go-humanize"
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
}

type tickMsg time.Time
type dataMsg struct {
	kd    *types.KafkaData
	rates map[string]map[string]*types.RateStats
	err   error
}

func ShowRich(admin *kafka.AdminClient, params *types.Params) error {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	ti := textinput.New()
	ti.Placeholder = "Enter regex pattern..."
	ti.CharLimit = 156
	ti.Width = 50

	m := model{
		admin:       admin,
		params:      params,
		loading:     true,
		spinner:     s,
		filterInput: ti,
		warnings:    []string{},
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return err
	}

	return nil
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		loadData(m.admin, m.params),
		tickCmd(),
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
		m.ready = true
		if !m.showFilter {
			m.updateTable()
		}

	case dataMsg:
		m.loading = false
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
			cmds = append(cmds, tickCmd())
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
		return fmt.Sprintf("\n  %s Calculating initial rates...\n", m.spinner.View())
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
		b.WriteString(baseStyle.Render(m.table.View()))
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
	if m.kd == nil || m.rates == nil {
		return
	}

	columns := []table.Column{
		{Title: "Group", Width: 30},
		{Title: "Topic", Width: 20},
		{Title: "Parts", Width: 8},
		{Title: "Since", Width: 8},
		{Title: "Consumed", Width: 10},
		{Title: "New/sec", Width: 10},
		{Title: "Cons/sec", Width: 10},
		{Title: "Time Left", Width: 12},
		{Title: "Lag", Width: 12},
	}

	rows := m.buildRows()
	m.sortRows(rows)

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(false),
		table.WithHeight(20),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	m.table = t
}

func (m *model) buildRows() []table.Row {
	var rows []table.Row

	for groupID, topicRates := range m.rates {
		for topic, rate := range topicRates {
			lag, exists := m.kd.GroupLags[groupID][topic]
			if !exists {
				continue
			}

			// Apply only-issues filter
			if m.params.KafkaOnlyIssues {
				if rate.RemainingSec < 60 {
					continue
				}
			}

			groupName := groupID
			topicName := topic

			if m.params.Anonymize {
				groupName = fmt.Sprintf("group %06d", hashString(groupID)%1000000)
				topicName = fmt.Sprintf("topic %06d", hashString(topic)%1000000)
			}

			row := table.Row{
				groupName,
				topicName,
				fmt.Sprintf("%d", len(lag.PartitionLags)),
				fmt.Sprintf("%.1fs", rate.TimeDelta),
				humanize.Comma(rate.EventsConsumed),
				humanize.Comma(int64(rate.EventsArrivalRate)),
				humanize.Comma(int64(rate.EventsConsumptionRate)),
				rate.RemainingHMS,
				humanize.Comma(lag.Sum),
			}

			rows = append(rows, row)
		}
	}

	return rows
}

func (m *model) sortRows(rows []table.Row) {
	if m.sortKey == "" {
		return
	}

	sort.Slice(rows, func(i, j int) bool {
		var less bool

		switch m.sortKey {
		case "group":
			less = rows[i][0] < rows[j][0]
		case "topic":
			less = rows[i][1] < rows[j][1]
		case "partitions":
			less = rows[i][2] < rows[j][2]
		case "eta":
			less = rows[i][7] < rows[j][7]
		case "lag":
			less = rows[i][8] < rows[j][8]
		case "rate":
			less = rows[i][6] < rows[j][6]
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

func tickCmd() tea.Cmd {
	return tea.Tick(time.Duration(5)*time.Second, func(t time.Time) tea.Msg {
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
