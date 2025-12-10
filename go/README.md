# kafkatop - Go Implementation

This is the Go rewrite of kafkatop, providing better performance and easier deployment with static binaries.

## Status

✅ **Core Features Implemented:**
- CLI argument parsing with Cobra
- Kafka connection and broker discovery
- Topic metadata retrieval
- Lag calculation logic (statistics: min, max, mean, median, sum)
- Rate calculation between snapshots
- Multiple output modes:
  - Text mode (plain terminal output)
  - Rich TUI mode with Bubble Tea (interactive, colorful)
  - JSON output modes (summary, status, topic info)
- Interactive features (keyboard shortcuts for sorting, filtering)
- Anonymization mode
- Color-coded health status

🚧 **Known Limitations:**

The current implementation uses `github.com/segmentio/kafka-go` which is a pure Go library (no CGO, perfect for static binaries), but it has some limitations:

1. **Consumer Group Listing**: The kafka-go library doesn't provide a high-level API for listing consumer groups. The Python version uses `confluent-kafka` which wraps librdkafka (a C library) that has full admin API support.

2. **Consumer Group Offset Fetching**: Similarly, fetching consumer group offsets requires low-level wire protocol implementation that kafka-go doesn't expose easily.

## Current State

The code **compiles successfully** and provides a working skeleton, but:
- Topic listing and metadata work perfectly ✅
- Consumer group operations need additional implementation ⚠️

## Next Steps (Recommended Paths Forward)

### Option 1: Use IBM Sarama (Recommended)

Switch to `github.com/IBM/sarama` which provides full Kafka admin API support:
- Full consumer group listing
- Consumer group offset fetching
- Consumer group description
- Still pure Go, no CGO required
- Battle-tested, used in production by many companies

**Pros:**
- Complete feature parity with Python version
- Pure Go (static binaries still work)
- Well-maintained and documented

**Cons:**
- Slightly more complex API
- Larger dependency

### Option 2: Implement Wire Protocol with kafka-go

Implement the missing features using kafka-go's low-level API:
- Manually construct FindCoordinator, OffsetFetch requests
- More control but more work

### Option 3: Hybrid Approach

Use kafka-go for basic operations and implement only the missing consumer group operations using the wire protocol.

## Building

```bash
# From repository root
make go-build

# Or directly
cd go
CGO_ENABLED=0 go build -ldflags="-s -w" -o kafkatop .
```

## Dependencies

- `github.com/spf13/cobra` - CLI framework
- `github.com/segmentio/kafka-go` - Kafka client (pure Go)
- `github.com/charmbracelet/bubbletea` - TUI framework
- `github.com/charmbracelet/bubbles` - TUI components (table, spinner, text input)
- `github.com/charmbracelet/lipgloss` - TUI styling
- `github.com/dustin/go-humanize` - Number formatting

## Architecture

```
go/
├── main.go                 # Entry point
├── cmd/
│   └── root.go            # Cobra CLI setup
├── internal/
│   ├── types/
│   │   └── types.go       # Data structures (KafkaData, LagStats, etc.)
│   ├── kafka/
│   │   ├── admin.go       # Kafka admin client operations
│   │   └── lag.go         # Lag and rate calculation logic
│   └── ui/
│       ├── rich.go        # Bubble Tea TUI implementation
│       ├── text.go        # Plain text output
│       └── json.go        # JSON output modes
└── go.mod
```

## Testing

```bash
cd go
go test ./...
```

## Code Quality

```bash
cd go
go vet ./...
go fmt ./...
```

## Contributing to Go Version

If you want to help complete the Go implementation:

1. **Easy wins**: Improve TUI styling, add more keyboard shortcuts
2. **Medium**: Implement missing consumer group operations with wire protocol
3. **Recommended**: Switch to Sarama and implement full consumer group support

See the main repository README for contribution guidelines.

## Comparison with Python Version

| Feature | Python | Go (Current) | Notes |
|---------|---------|--------------|-------|
| List Topics | ✅ | ✅ | Working |
| Topic Metadata | ✅ | ✅ | Working |
| List Consumer Groups | ✅ | ⚠️ | Needs implementation |
| Consumer Group Offsets | ✅ | ⚠️ | Needs implementation |
| Lag Calculation | ✅ | ✅ | Logic implemented |
| Rate Calculation | ✅ | ✅ | Logic implemented |
| Rich TUI | ✅ | ✅ | Implemented with Bubble Tea |
| Text Mode | ✅ | ✅ | Working |
| JSON Output | ✅ | ✅ | Working |
| Filtering | ✅ | ✅ | Working |
| Sorting | ✅ | ✅ | Working |
| Anonymization | ✅ | ✅ | Working |
| Static Binary | ❌ | ✅ | Go advantage |
| Startup Time | Slow (PEX) | Fast | Go advantage |
| Memory Usage | Higher | Lower | Go advantage |
| Old OS Support | Requires Python | Works on CentOS 7+ | Go advantage |

## License

MIT License (same as parent project)
