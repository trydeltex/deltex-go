// Package deltex provides an official Go client for Deltex — edge-native SQL database.
//
// # Quick start
//
//	db, err := deltex.Connect(deltex.Options{})  // reads DELTEX_API_KEY from env
//	if err != nil { log.Fatal(err) }
//
//	rows, err := db.Query(ctx, "SELECT * FROM users WHERE active = $1", true)
//	user, err := db.QueryOne(ctx, "SELECT * FROM users WHERE id = $1", 42)
//	n, err := db.Execute(ctx, "INSERT INTO events (type) VALUES ($1)", "click")
//
//	// Transaction
//	err = db.Transaction(ctx, func(tx *deltex.Tx) error {
//	    _, err := tx.Execute(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", 100, 1)
//	    return err
//	})
package deltex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Row is a single result row: column name → value.
type Row map[string]any

// Version is the current SDK version.
const Version = "1.3.1"

// WriteMode controls how writes are committed.
type WriteMode string

const (
	// WriteModeSync waits for a durable commit. Default — never loses an
	// acknowledged write. Batch writes (multi-row INSERT/transaction) to amortize.
	WriteModeSync WriteMode = "sync"
	// WriteModeEdge is CAS-protected async. Acknowledges before the durable
	// commit lands; a concurrent write from another node can clobber it. Use for
	// caches, sessions, idempotent upserts — not data that must not be lost.
	WriteModeEdge WriteMode = "edge"
	// WriteModeAsync fire-and-forget. Best for high-volume telemetry.
	WriteModeAsync WriteMode = "async"
)

// CommitStatus describes how a write was committed.
type CommitStatus string

const (
	CommitStatusCommitted    CommitStatus = "committed"
	CommitStatusEdgeAccepted CommitStatus = "edge-accepted"
	CommitStatusAsyncQueued  CommitStatus = "async-queued"
)

// QueryResult is the full result envelope from a query.
type QueryResult struct {
	Rows          []Row
	Columns       []string
	RowsAffected  int
	ExecutionMs   float64 // 0 if not available
	CommitStatus  CommitStatus
	SchemaVersion int
}

// ─── Errors ───────────────────────────────────────────────────────────────────

// Error is returned when the engine reports an error.
type Error struct {
	Message   string
	Status    int    // HTTP status code
	SQL       string // the query that caused the error
	EngineMsg string
}

func (e *Error) Error() string {
	return fmt.Sprintf("deltex: %s (HTTP %d)", e.Message, e.Status)
}

// RateLimitError is returned when the rate limit is exceeded after all retries.
type RateLimitError struct {
	RetryAfter time.Duration
	SQL        string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("deltex: rate limit exceeded, retry after %v", e.RetryAfter)
}

// ─── Options ──────────────────────────────────────────────────────────────────

// Options configures a Client.
type Options struct {
	// APIKey is the Bearer token. Defaults to DELTEX_API_KEY env var.
	APIKey string
	// Endpoint is the engine URL. Defaults to DELTEX_ENDPOINT or https://db.deltex.dev.
	Endpoint string
	// WriteMode is the default write mode. Defaults to WriteModeSync (durable).
	WriteMode WriteMode
	// Timeout is the HTTP request timeout. Defaults to 30s.
	Timeout time.Duration
	// MaxRetries controls auto-retry on 429. Defaults to 3.
	MaxRetries int
	// Tag is sent as X-Query-Tag for analytics.
	Tag string
	// HTTPClient allows injecting a custom *http.Client.
	HTTPClient *http.Client
}

// ─── Client ───────────────────────────────────────────────────────────────────

// Client is a Deltex database client.
type Client struct {
	opts    Options
	url     string
	txURL   string
	headers map[string]string
}

// Connect creates a new Client. Reads DELTEX_API_KEY and DELTEX_ENDPOINT from env
// if not set in Options.
func Connect(opts Options) (*Client, error) {
	if opts.APIKey == "" {
		opts.APIKey = os.Getenv("DELTEX_API_KEY")
	}
	if opts.APIKey == "" {
		return nil, &Error{Message: "no API key: set DELTEX_API_KEY or Options.APIKey", Status: 0}
	}
	if opts.Endpoint == "" {
		opts.Endpoint = os.Getenv("DELTEX_ENDPOINT")
	}
	if opts.Endpoint == "" {
		opts.Endpoint = "https://db.deltex.dev"
	}
	opts.Endpoint = strings.TrimRight(opts.Endpoint, "/")
	if opts.WriteMode == "" {
		opts.WriteMode = WriteModeSync
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: opts.Timeout}
	}

	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + opts.APIKey,
		"X-Write-Mode":  string(opts.WriteMode),
	}
	if opts.Tag != "" {
		headers["X-Query-Tag"] = opts.Tag
	}

	return &Client{
		opts:    opts,
		url:     opts.Endpoint + "/v1/query",
		txURL:   opts.Endpoint + "/v1/transaction",
		headers: headers,
	}, nil
}

// WithWriteMode returns a copy of the client with a different default write mode.
func (c *Client) WithWriteMode(mode WriteMode) *Client {
	if mode == c.opts.WriteMode {
		return c
	}
	nc := *c
	nc.opts.WriteMode = mode
	nc.headers = copyHeaders(c.headers)
	nc.headers["X-Write-Mode"] = string(mode)
	return &nc
}

// Strong returns a copy of the client that bypasses cache (X-Consistency: strong).
func (c *Client) Strong() *Client {
	nc := *c
	nc.headers = copyHeaders(c.headers)
	nc.headers["X-Consistency"] = "strong"
	return &nc
}

// WithTag returns a copy of the client with the given X-Query-Tag.
func (c *Client) WithTag(tag string) *Client {
	nc := *c
	nc.headers = copyHeaders(c.headers)
	nc.headers["X-Query-Tag"] = tag
	return &nc
}

// WithIdempotencyKey returns a copy of the client with X-Idempotency-Key set.
func (c *Client) WithIdempotencyKey(key string) *Client {
	nc := *c
	nc.headers = copyHeaders(c.headers)
	nc.headers["X-Idempotency-Key"] = key
	return &nc
}

// ─── Query methods ────────────────────────────────────────────────────────────

// Query executes SQL and returns all rows.
func (c *Client) Query(ctx context.Context, sql string, params ...any) ([]Row, error) {
	r, err := c.runQuery(ctx, bindParams(sql, params))
	if err != nil {
		return nil, err
	}
	return r.Rows, nil
}

// QueryOne executes SQL and returns the first row, or nil if no rows.
func (c *Client) QueryOne(ctx context.Context, sql string, params ...any) (Row, error) {
	rows, err := c.Query(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// Execute runs a mutating statement and returns rows affected.
func (c *Client) Execute(ctx context.Context, sql string, params ...any) (int, error) {
	r, err := c.runQuery(ctx, bindParams(sql, params))
	if err != nil {
		return 0, err
	}
	return r.RowsAffected, nil
}

// ExecuteRaw runs SQL and returns the full QueryResult.
func (c *Client) ExecuteRaw(ctx context.Context, sql string, params ...any) (*QueryResult, error) {
	return c.runQuery(ctx, bindParams(sql, params))
}

// ─── Transaction ──────────────────────────────────────────────────────────────

// Tx is a transaction handle — collects mutations then commits atomically.
type Tx struct {
	statements []string
}

// Execute queues a mutating SQL statement in the transaction.
func (tx *Tx) Execute(_ context.Context, sql string, params ...any) (int, error) {
	tx.statements = append(tx.statements, bindParams(sql, params))
	return 0, nil
}

// Transaction executes fn(tx) atomically via the /v1/transaction endpoint.
// All Execute() calls inside fn are collected and sent as a single atomic batch.
// Read queries (Query/QueryOne) execute immediately (live reads).
//
//	err = db.Transaction(ctx, func(tx *deltex.Tx) error {
//	    tx.Execute(ctx, "UPDATE accounts SET balance=balance-$1 WHERE id=$2", 100, 1)
//	    tx.Execute(ctx, "UPDATE accounts SET balance=balance+$1 WHERE id=$2", 100, 2)
//	    return nil
//	})
func (c *Client) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	tx := &Tx{}
	if err := fn(tx); err != nil {
		return err
	}
	if len(tx.statements) == 0 {
		return nil // no-op
	}

	body, _ := json.Marshal(map[string]any{
		"statements": tx.statements,
		"isolation":  "SERIALIZABLE",
	})

	resp, err := c.do(ctx, c.txURL, body)
	if err != nil {
		return err
	}

	if resp.Success != nil && !*resp.Success {
		return &Error{Message: resp.Message, Status: 500, SQL: strings.Join(tx.statements, "; ")}
	}
	return nil
}

// Batch atomically executes a slice of SQL statements in ONE round-trip.
//
// The fastest way to apply many writes: the engine coalesces them into a single
// durable commit, so N statements cost ~one write (O(1)) instead of N separate
// round-trips. Prefer this — or a single multi-row INSERT — over looping Execute
// for bulk work. Runs as a transaction (all-or-nothing). Returns total rows
// affected.
//
//	n, err := db.Batch(ctx, []string{
//	    "INSERT INTO t (id) VALUES (1)",
//	    "INSERT INTO t (id) VALUES (2)",
//	})
func (c *Client) Batch(ctx context.Context, statements []string) (int, error) {
	if len(statements) == 0 {
		return 0, nil
	}
	body, _ := json.Marshal(map[string]any{
		"statements": statements,
		"isolation":  "SERIALIZABLE",
	})
	resp, err := c.do(ctx, c.txURL, body)
	if err != nil {
		return 0, err
	}
	if resp.Success != nil && !*resp.Success {
		return 0, &Error{Message: resp.Message, Status: 500, SQL: strings.Join(statements, "; ")}
	}
	if resp.AffectedRows != 0 {
		return resp.AffectedRows, nil
	}
	return resp.RowsAffected, nil
}

// ─── HTTP internals ───────────────────────────────────────────────────────────

var timingRE = regexp.MustCompile(`total;dur=([\d.]+)`)

type engineResponse struct {
	Success      *bool            `json:"success"`
	Message      string           `json:"message"`
	Columns      []string         `json:"columns"`
	Rows         []map[string]any `json:"rows"`
	AffectedRows int              `json:"affected_rows"`
	RowsAffected int              `json:"rows_affected"`
}

func (c *Client) runQuery(ctx context.Context, sql string) (*QueryResult, error) {
	body, _ := json.Marshal(map[string]string{"sql": sql})

	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
		if err != nil {
			return nil, &Error{Message: err.Error(), SQL: sql}
		}
		for k, v := range c.headers {
			req.Header.Set(k, v)
		}

		resp, err := c.opts.HTTPClient.Do(req)
		if err != nil {
			return nil, &Error{Message: "network error: " + err.Error(), SQL: sql}
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 {
			retryAfterSec := 1.0
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if f, err := strconv.ParseFloat(ra, 64); err == nil {
					retryAfterSec = f
				}
			}
			d := time.Duration(retryAfterSec * float64(time.Second))
			if attempt < c.opts.MaxRetries {
				lastErr = &RateLimitError{RetryAfter: d, SQL: sql}
				time.Sleep(d)
				continue
			}
			return nil, &RateLimitError{RetryAfter: d, SQL: sql}
		}

		var eng engineResponse
		if err := json.Unmarshal(respBody, &eng); err != nil {
			return nil, &Error{Message: "invalid JSON response", Status: resp.StatusCode, SQL: sql}
		}

		if (eng.Success != nil && !*eng.Success) || (resp.StatusCode >= 400 && len(eng.Columns) == 0) {
			return nil, &Error{
				Message:   eng.Message,
				Status:    resp.StatusCode,
				SQL:       sql,
				EngineMsg: eng.Message,
			}
		}

		// Parse server-timing
		var execMs float64
		if st := resp.Header.Get("Server-Timing"); st != "" {
			if m := timingRE.FindStringSubmatch(st); len(m) > 1 {
				execMs, _ = strconv.ParseFloat(m[1], 64)
			}
		}

		// Deltex-specific headers
		var cs CommitStatus
		if raw := strings.TrimSpace(resp.Header.Get("X-Commit-Status")); raw != "" {
			cs = CommitStatus(raw)
		}

		var sv int
		if raw := strings.TrimSpace(resp.Header.Get("X-Schema-Version")); raw != "" {
			sv, _ = strconv.Atoi(raw)
		}

		rowsAffected := eng.AffectedRows
		if rowsAffected == 0 {
			rowsAffected = eng.RowsAffected
		}
		if rowsAffected == 0 {
			rowsAffected = len(eng.Rows)
		}

		return &QueryResult{
			Rows:          toRows(eng.Rows),
			Columns:       eng.Columns,
			RowsAffected:  rowsAffected,
			ExecutionMs:   execMs,
			CommitStatus:  cs,
			SchemaVersion: sv,
		}, nil
	}
	return nil, lastErr
}

func (c *Client) do(ctx context.Context, url string, body []byte) (*engineResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.opts.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var eng engineResponse
	json.Unmarshal(respBody, &eng)
	return &eng, nil
}

// ─── Parameter binding ────────────────────────────────────────────────────────

var positionalRE = regexp.MustCompile(`\$(\d+)`)

func bindParams(sql string, params []any) string {
	if len(params) == 0 {
		return sql
	}
	return positionalRE.ReplaceAllStringFunc(sql, func(m string) string {
		idx, _ := strconv.Atoi(m[1:])
		if idx < 1 || idx > len(params) {
			return m
		}
		return formatParam(params[idx-1])
	})
}

func formatParam(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	default:
		b, _ := json.Marshal(v)
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'"
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func toRows(raw []map[string]any) []Row {
	rows := make([]Row, len(raw))
	for i, r := range raw {
		rows[i] = Row(r)
	}
	return rows
}

func copyHeaders(h map[string]string) map[string]string {
	nc := make(map[string]string, len(h))
	for k, v := range h {
		nc[k] = v
	}
	return nc
}
