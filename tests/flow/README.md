# Flow Tests

Component-level flow tests for the Poller->Handler->SinkWriter pipeline.

## Running Tests

```bash
go test -v ./tests/flow
```

## Test Coverage

1. **TestFlow_SingleTable_SingleTransaction** - Single INSERT Change: assert Handler called, sink writer wrote correct row, store.Write called, offsets.Set advanced.

2. **TestFlow_SingleTable_MultipleOperations** - Single transaction with INSERT+UPDATE+DELETE: assert correct grouping and ordered Sink ops.

3. **TestFlow_CrossTableTransaction** - One transaction spanning three tables (Orders, OrderItems, Inventory): assert one Transaction contains all 3 Changes and sinkwriter routes by database.

4. **TestFlow_ExactlyOnce_SinkFailure** - Sink writer returns error: assert offsets are NOT advanced and failure is propagated/observable.

5. **TestFlow_HandlerFailure_NonBlocking** - Handler returns error: assert sinkwriter still executes for sinks produced, store.Write and offsets.Set still advance as designed.

6. **TestFlow_MultiDatabaseRouting** - A transaction producing sinks for two databases: assert Manager creates two writers and both write to their respective DBs.

## Test Fixtures

- `fixtures/skills/` - YAML skill files used by plugin manager
- `fixtures/migrations/` - Migration SQL files for SQLite writers

## Dependencies

Tests use in-memory SQLite and mock implementations to avoid external dependencies (MSSQL).
