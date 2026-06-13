# deltex-go

Official Go client for [Deltex](https://deltex.dev) — edge-native SQL database.

## Install

```bash
go get github.com/trydeltex/deltex-go
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/trydeltex/deltex-go"
)

func main() {
    // Auto-reads DELTEX_API_KEY from env
    db, err := deltex.Connect(deltex.Options{})
    if err != nil { log.Fatal(err) }

    ctx := context.Background()

    // Query
    rows, err := db.Query(ctx, "SELECT id, name FROM users WHERE active = $1", true)
    if err != nil { log.Fatal(err) }
    for _, row := range rows {
        fmt.Println(row["name"])
    }

    // Single row
    user, err := db.QueryOne(ctx, "SELECT * FROM users WHERE id = $1", 42)
    if user == nil { log.Fatal("not found") }

    // Mutation
    n, err := db.Execute(ctx, "INSERT INTO events (type, ts) VALUES ($1, NOW())", "pageview")
    fmt.Printf("%d row inserted\n", n)
}
```

## API

### `deltex.Connect(opts)`

```go
db, err := deltex.Connect(deltex.Options{
    APIKey:    "",         // or set DELTEX_API_KEY env var
    Endpoint:  "",         // or set DELTEX_ENDPOINT env var (default: https://db.deltex.dev)
    WriteMode: deltex.WriteModeEdge, // WriteModeEdge|WriteModeSync|WriteModeAsync
    Timeout:   30 * time.Second,
    MaxRetries: 3,
    Tag:       "my-service",
})
```

### Methods

```go
db.Query(ctx, sql, params...)       → ([]Row, error)
db.QueryOne(ctx, sql, params...)    → (Row, error)  // nil Row if not found
db.Execute(ctx, sql, params...)     → (int, error)  // rows affected
db.ExecuteRaw(ctx, sql, params...)  → (*QueryResult, error)
db.Transaction(ctx, func(tx) error) → error
```

### Fluent modifiers (return new *Client)

```go
db.WithWriteMode(deltex.WriteModeSync)  // per-client write mode
db.Strong()                             // X-Consistency: strong
db.WithTag("tag")                       // X-Query-Tag analytics
db.WithIdempotencyKey("req-id")         // safe deduplication
```

### Transaction

```go
err = db.Transaction(ctx, func(tx *deltex.Tx) error {
    _, err := tx.Execute(ctx, "UPDATE accounts SET balance=balance-$1 WHERE id=$2", 100, 1)
    if err != nil { return err }
    _, err = tx.Execute(ctx, "UPDATE accounts SET balance=balance+$1 WHERE id=$2", 100, 2)
    return err
})
```

### QueryResult

```go
result, err := db.ExecuteRaw(ctx, "INSERT INTO payments (amount) VALUES ($1)", 99.99)
fmt.Println(result.CommitStatus)  // "edge-accepted" | "committed" | "async-queued"
fmt.Println(result.ExecutionMs)   // server-side execution time
fmt.Println(result.SchemaVersion) // for cache invalidation
```

### Error handling

```go
import "errors"

rows, err := db.Query(ctx, "SELECT * FROM bad_table")
if err != nil {
    var dErr *deltex.Error
    var rlErr *deltex.RateLimitError
    if errors.As(err, &rlErr) {
        time.Sleep(rlErr.RetryAfter) // already retried MaxRetries times
    } else if errors.As(err, &dErr) {
        log.Printf("engine error %d: %s", dErr.Status, dErr.Message)
    }
}
```

## Write Modes

| Mode | Latency | Use when |
|------|---------|----------|
| `WriteModeEdge` (default) | ~10ms | Normal writes, ASIA/AUS PoPs |
| `WriteModeSync` | ~350ms | Financial, audit logs |
| `WriteModeAsync` | ~5ms | High-volume telemetry |

## License

MIT

---

## Common Patterns

### Error handling

```go
rows, err := db.Query(ctx, "SELECT * FROM users WHERE id = $1", 42)
if err != nil {
    var rl *deltex.RateLimitError
    if errors.As(err, &rl) {
        time.Sleep(time.Duration(rl.RetryAfter) * time.Second)
        // retry...
    }
    log.Fatal(err)
}
```

### Transactions with retry

```go
for attempt := 0; attempt < 3; attempt++ {
    err = db.Transaction(ctx, func(tx *deltex.Tx) error {
        _, err := tx.Execute(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", 100, 1)
        if err != nil { return err }
        _, err = tx.Execute(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", 100, 2)
        return err
    })
    if err == nil { break }
    if strings.Contains(err.Error(), "CAS_CONFLICT") {
        time.Sleep(time.Duration(attempt*100) * time.Millisecond)
        continue
    }
    log.Fatal(err)
}
```

### Strong consistency reads

```go
db.Strong().Query(ctx, "SELECT balance FROM accounts WHERE id = $1", accountID)
```

### Edge mode writes (ASIA/AUS)

```go
db.WithWriteMode(deltex.WriteModeEdge).Execute(ctx, "INSERT INTO events ...")
```

## SDK Version

`v1.3.0` — see [CHANGELOG.md](../../CHANGELOG.md) for history.
