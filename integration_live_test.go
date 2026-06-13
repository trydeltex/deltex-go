package deltex_test

// Live integration test against a real Deltex endpoint.
// Skipped unless DELTEX_LIVE_TEST=1 and DELTEX_API_KEY are set, so the default
// `go test ./...` run stays offline (unit tests use httptest mocks).
//
//	DELTEX_LIVE_TEST=1 DELTEX_API_KEY=dtx_k_... go test -run TestLive -v

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	deltex "github.com/trydeltex/deltex-go"
)

func TestLiveEndToEnd(t *testing.T) {
	if os.Getenv("DELTEX_LIVE_TEST") != "1" || os.Getenv("DELTEX_API_KEY") == "" {
		t.Skip("set DELTEX_LIVE_TEST=1 and DELTEX_API_KEY to run live integration")
	}
	c, err := deltex.Connect(deltex.Options{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx := context.Background()

	_, _ = c.ExecuteRaw(ctx, "DROP TABLE IF EXISTS go_live_t")
	if _, err := c.ExecuteRaw(ctx, "CREATE TABLE go_live_t (id INT PRIMARY KEY, name TEXT, score FLOAT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer c.ExecuteRaw(ctx, "DROP TABLE IF EXISTS go_live_t")
	time.Sleep(1200 * time.Millisecond)

	n, err := c.Execute(ctx, "INSERT INTO go_live_t VALUES ($1,$2,$3),($4,$5,$6)",
		1, "alice", 9.5, 2, "bob", 7.25)
	if err != nil || n != 2 {
		t.Fatalf("insert: n=%d err=%v", n, err)
	}
	time.Sleep(1200 * time.Millisecond)

	rows, err := c.Strong().Query(ctx, "SELECT id, name, score FROM go_live_t ORDER BY id")
	if err != nil || len(rows) != 2 {
		t.Fatalf("select: rows=%d err=%v", len(rows), err)
	}
	if rows[0]["name"] != "alice" || rows[1]["score"] != 7.25 {
		t.Fatalf("row values wrong: %v", rows)
	}

	one, err := c.Strong().QueryOne(ctx, "SELECT COUNT(*) AS n FROM go_live_t")
	if err != nil || fmt.Sprint(one["n"]) != "2" {
		t.Fatalf("count: %v err=%v", one, err)
	}

	if n, err = c.Execute(ctx, "UPDATE go_live_t SET score = $1 WHERE id = $2", 10.0, 1); err != nil || n != 1 {
		t.Fatalf("update: n=%d err=%v", n, err)
	}

	// Serializable transaction: settle prior writes, back off on 2PC conflicts.
	time.Sleep(1500 * time.Millisecond)
	var txErr error
	for attempt := 0; attempt < 3; attempt++ {
		txErr = c.Transaction(ctx, func(tx *deltex.Tx) error {
			tx.Execute(ctx, "INSERT INTO go_live_t VALUES (3,'carol',8.8)")
			tx.Execute(ctx, "UPDATE go_live_t SET score = score + 1 WHERE id = 2")
			return nil
		})
		if txErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if txErr != nil {
		t.Fatalf("transaction: %v", txErr)
	}
	time.Sleep(1500 * time.Millisecond)

	one, err = c.Strong().QueryOne(ctx, "SELECT COUNT(*) AS n FROM go_live_t")
	if err != nil || fmt.Sprint(one["n"]) != "3" {
		t.Fatalf("post-txn count: %v err=%v", one, err)
	}

	if _, err := c.Query(ctx, "SELECT FROM WHERE"); err == nil {
		t.Fatal("parse error should surface as Go error")
	}
}
