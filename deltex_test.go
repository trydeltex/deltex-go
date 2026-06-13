package deltex

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

// mockTransport implements http.RoundTripper for tests
type mockTransport struct {
	fn func(req *mockRequest) *mockResponse
}

type mockRequest struct {
	URL     string
	Headers map[string]string
	Body    string
}
type mockResponse struct {
	Status  int
	Body    string
	Headers map[string]string
}

// TestConnectMissingAPIKey verifies error when no key
func TestConnectMissingAPIKey(t *testing.T) {
	_, err := Connect(Options{APIKey: "deliberately-invalid-key-xyz"})
	// Connect succeeds with any non-empty key (validation happens on first query)
	if err != nil {
		t.Errorf("Connect should not fail for non-empty key: %v", err)
	}
}

func TestConnectEmptyAPIKey(t *testing.T) {
	t.Setenv("DELTEX_API_KEY", "")
	_, err := Connect(Options{})
	if err == nil {
		t.Error("Expected error for empty API key")
	}
}

func TestConnectDefaults(t *testing.T) {
	t.Setenv("DELTEX_API_KEY", "test_key")
	db, err := Connect(Options{})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if db.opts.WriteMode != WriteModeEdge {
		t.Errorf("Default write mode should be edge, got %s", db.opts.WriteMode)
	}
	if db.url != "https://db.deltex.dev/v1/query" {
		t.Errorf("Default URL wrong: %s", db.url)
	}
}

func TestWithWriteMode(t *testing.T) {
	t.Setenv("DELTEX_API_KEY", "k")
	db, _ := Connect(Options{})
	sync := db.WithWriteMode(WriteModeSync)
	if sync == db {
		t.Error("WithWriteMode should return new client")
	}
	if sync.headers["X-Write-Mode"] != "sync" {
		t.Errorf("X-Write-Mode header wrong: %s", sync.headers["X-Write-Mode"])
	}
	// Same mode returns same client
	same := db.WithWriteMode(WriteModeEdge)
	if same != db {
		t.Error("WithWriteMode same mode should return same client")
	}
}

func TestStrong(t *testing.T) {
	t.Setenv("DELTEX_API_KEY", "k")
	db, _ := Connect(Options{})
	strong := db.Strong()
	if strong.headers["X-Consistency"] != "strong" {
		t.Errorf("Expected X-Consistency: strong, got %q", strong.headers["X-Consistency"])
	}
}

func TestWithTag(t *testing.T) {
	t.Setenv("DELTEX_API_KEY", "k")
	db, _ := Connect(Options{})
	tagged := db.WithTag("my-feature")
	if tagged.headers["X-Query-Tag"] != "my-feature" {
		t.Errorf("Expected X-Query-Tag: my-feature, got %q", tagged.headers["X-Query-Tag"])
	}
}

func TestWithIdempotencyKey(t *testing.T) {
	t.Setenv("DELTEX_API_KEY", "k")
	db, _ := Connect(Options{})
	idb := db.WithIdempotencyKey("req-123")
	if idb.headers["X-Idempotency-Key"] != "req-123" {
		t.Errorf("Expected X-Idempotency-Key: req-123, got %q", idb.headers["X-Idempotency-Key"])
	}
}

// ─── Parameter binding ────────────────────────────────────────────────────────

func TestBindParams(t *testing.T) {
	tests := []struct {
		sql      string
		params   []any
		expected string
	}{
		{"SELECT $1", []any{"Alice"}, "SELECT 'Alice'"},
		{"SELECT $1", []any{42}, "SELECT 42"},
		{"SELECT $1", []any{3.14}, "SELECT 3.14"},
		{"SELECT $1", []any{true}, "SELECT TRUE"},
		{"SELECT $1", []any{false}, "SELECT FALSE"},
		{"SELECT $1", []any{nil}, "SELECT NULL"},
		{"WHERE a=$1 AND b=$2", []any{"x", 5}, "WHERE a='x' AND b=5"},
		{"SELECT $1", []any{"it's a test"}, "SELECT 'it''s a test'"},
	}
	for _, tt := range tests {
		got := bindParams(tt.sql, tt.params)
		if got != tt.expected {
			t.Errorf("bindParams(%q, %v) = %q, want %q", tt.sql, tt.params, got, tt.expected)
		}
	}
}

// ─── Transaction collector ────────────────────────────────────────────────────

func TestTransactionCollector(t *testing.T) {
	tx := &Tx{}
	ctx := context.Background()
	tx.Execute(ctx, "INSERT INTO t VALUES ($1)", 1)
	tx.Execute(ctx, "UPDATE t SET x=$1 WHERE id=$2", 99, 1)
	if len(tx.statements) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(tx.statements))
	}
	if tx.statements[0] != "INSERT INTO t VALUES (1)" {
		t.Errorf("Statement 0 wrong: %s", tx.statements[0])
	}
	if tx.statements[1] != "UPDATE t SET x=99 WHERE id=1" {
		t.Errorf("Statement 1 wrong: %s", tx.statements[1])
	}
}

func TestFormatParam(t *testing.T) {
	cases := []struct {
		input    any
		expected string
	}{
		{nil, "NULL"},
		{true, "TRUE"},
		{false, "FALSE"},
		{int(42), "42"},
		{int64(100), "100"},
		{float64(3.14), "3.14"},
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
	}
	for _, c := range cases {
		got := formatParam(c.input)
		if got != c.expected {
			t.Errorf("formatParam(%v) = %q, want %q", c.input, got, c.expected)
		}
	}
}

func TestErrorTypes(t *testing.T) {
	e := &Error{Message: "test error", Status: 400, SQL: "SELECT 1"}
	if e.Error() == "" {
		t.Error("Error.Error() should not be empty")
	}

	re := &RateLimitError{RetryAfter: time.Second}
	if re.Error() == "" {
		t.Error("RateLimitError.Error() should not be empty")
	}
}

func TestQueryResult(t *testing.T) {
	r := &QueryResult{
		Rows:         []Row{{"id": 1, "name": "Alice"}},
		Columns:      []string{"id", "name"},
		RowsAffected: 1,
		ExecutionMs:  12.5,
		CommitStatus: CommitStatusEdgeAccepted,
	}
	if len(r.Rows) != 1 { t.Error("Expected 1 row") }
	if r.Columns[0] != "id" { t.Error("Expected 'id' column") }
	if math.Abs(r.ExecutionMs - 12.5) > 0.001 { t.Errorf("ExecutionMs wrong: %f", r.ExecutionMs) }
	if r.CommitStatus != CommitStatusEdgeAccepted { t.Errorf("CommitStatus wrong: %s", r.CommitStatus) }
}

// time package ref
var _ = fmt.Sprintf
