# Security Audit: Read-Only Kafka Operations

## Audit Date
2025-12-10

## Audit Scope
Verify that kafkatop Go implementation only performs READ operations and never writes/publishes to Kafka.

## Kafka Operations Used

All operations in `internal/kafka/admin.go`:

### ✅ Read-Only Operations

1. **kafka.Client.ListGroups()** - Lists consumer groups
   - Purpose: Discover what consumer groups exist
   - Permission: READ
   - Line: admin.go:55

2. **kafka.Client.OffsetFetch()** - Fetches committed offsets for a consumer group
   - Purpose: Read where a consumer group is in each topic/partition
   - Permission: READ
   - Line: admin.go:114

3. **kafka.Conn.ReadLastOffset()** - Reads the latest offset for a topic partition
   - Purpose: Find the high-water mark for lag calculation
   - Permission: READ
   - Line: admin.go:154

4. **kafka.Conn.ReadPartitions()** - Reads partition metadata for topics
   - Purpose: Discover topic structure
   - Permission: READ
   - Line: admin.go:176

5. **kafka.Conn.Brokers()** - Lists Kafka brokers
   - Purpose: Get cluster metadata
   - Permission: READ
   - Line: admin.go:213

6. **kafka.Conn.Controller()** - Gets controller broker info
   - Purpose: Cluster metadata
   - Permission: READ
   - Line: admin.go:218

7. **kafka.Dial() / kafka.DialLeader()** - Establishes connections
   - Purpose: Network connectivity only
   - Permission: N/A (connection establishment)
   - Lines: admin.go:28, 148, 170, 207

## Operations NOT Used

The following WRITE operations are **NOT present** in the codebase:

- ❌ **Producer operations**: Produce, Write, Send, Publish
- ❌ **Topic management**: CreateTopic, DeleteTopic, CreatePartitions
- ❌ **Configuration changes**: AlterConfig, IncrementalAlterConfig
- ❌ **Offset manipulation**: OffsetCommit, AlterConsumerGroupOffsets
- ❌ **Consumer group manipulation**: DeleteGroups
- ❌ **ACL operations**: CreateAcls, DeleteAcls
- ❌ **Transaction operations**: InitProducerId, AddPartitionsToTxn

## Verification Commands

```bash
# Search for write operations (should return none related to Kafka)
grep -r "Produce\|Write\|Publish\|Send\|CreateTopic\|DeleteTopic\|AlterConfig" --include="*.go" internal/kafka/

# List all Kafka operations
grep -E "client\.|conn\." internal/kafka/admin.go | grep -v "//"
```

## Code Review Summary

### Files Audited
- `internal/kafka/admin.go` (257 lines)
- `internal/kafka/lag.go` (248 lines)
- `main.go` (104 lines)
- `internal/ui/*.go` (UI rendering only)

### Findings
✅ **SAFE**: No write operations found
✅ **READ-ONLY**: All Kafka operations are read-only metadata queries
✅ **NON-DESTRUCTIVE**: Cannot modify topics, consumer groups, or data

## Kafka Permissions Required

kafkatop only needs the following Kafka ACLs:

```
# Consumer group operations (read-only)
ALLOW principal User:* ON Group:* FROM * OPERATION:Describe

# Topic operations (read-only)
ALLOW principal User:* ON Topic:* FROM * OPERATION:Describe
ALLOW principal User:* ON Topic:* FROM * OPERATION:Read

# Cluster operations (read-only)
ALLOW principal User:* ON Cluster:kafka-cluster FROM * OPERATION:Describe
```

**NOT REQUIRED**:
- Write permissions
- Create/Delete permissions
- Alter permissions
- Transaction permissions

## Comparison with Python Version

The Python version (`python/kafkatop.py`) was also audited:

✅ Python version also READ-ONLY
- Uses confluent_kafka AdminClient for metadata queries
- No Producer instances created
- No message publishing

Both implementations are safe for production monitoring.

## Conclusion

**Status**: ✅ **APPROVED FOR READ-ONLY OPERATION**

The kafkatop Go implementation is **100% read-only** and safe to use for monitoring Kafka clusters without risk of:
- Publishing unwanted messages
- Modifying consumer group offsets
- Altering topic configurations
- Deleting topics or consumer groups
- Any other destructive operations

**Recommended Use**: Safe for production monitoring with read-only Kafka credentials.

---

**Auditor**: Claude Sonnet 4.5
**Audit Method**: Static code analysis + grep verification
**Next Review**: When new Kafka operations are added
