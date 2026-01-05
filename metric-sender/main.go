package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

// Config holds the application configuration
type Config struct {
	URL             string `toml:"url"`
	Database        string `toml:"database"`
	IntervalMinutes int    `toml:"interval_minutes"`

	// HTTP client configuration
	ConnectTimeoutSecs int `toml:"connect_timeout_secs"` // Default: 10
	ReadTimeoutSecs    int `toml:"read_timeout_secs"`    // Default: 30
	MaxRetries         int `toml:"max_retries"`          // Default: 3
	RetryDelaySecs     int `toml:"retry_delay_secs"`     // Default: 2
}

// loadConfig loads configuration from config.toml with default values
func loadConfig(configPath string) (*Config, error) {
	// Default configuration
	config := &Config{
		URL:             "http://localhost:7000",
		Database:        "_system",
		IntervalMinutes: 5,
	}

	// If config file exists, load it
	if _, err := os.Stat(configPath); err == nil {
		if _, err := toml.DecodeFile(configPath, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	} else {
		fmt.Printf("Config file not found at %s, using defaults\n", configPath)
	}

	// Set default values for HTTP client configuration if not specified
	if config.ConnectTimeoutSecs == 0 {
		config.ConnectTimeoutSecs = 10
	}
	if config.ReadTimeoutSecs == 0 {
		config.ReadTimeoutSecs = 30
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelaySecs == 0 {
		config.RetryDelaySecs = 2
	}

	return config, nil
}

// createHTTPClient creates an HTTP client with configured timeouts
func createHTTPClient(config *Config) *http.Client {
	connectTimeout := time.Duration(config.ConnectTimeoutSecs) * time.Second
	readTimeout := time.Duration(config.ReadTimeoutSecs) * time.Second

	return &http.Client{
		Timeout: readTimeout, // Overall request timeout
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: connectTimeout, // Connection timeout
			}).DialContext,
			TLSHandshakeTimeout:   connectTimeout,
			ResponseHeaderTimeout: readTimeout,
		},
	}
}

// doRequestWithRetry performs an HTTP GET request with retry logic
func doRequestWithRetry(client *http.Client, url string, maxRetries int, retryDelay time.Duration) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Retry attempt %d/%d for URL: %s", attempt, maxRetries, url)
			time.Sleep(retryDelay * time.Duration(attempt)) // Exponential backoff
		}

		resp, err := client.Get(url)

		// Success - return immediately
		if err == nil && resp.StatusCode == 200 {
			return resp, nil
		}

		// Store error for potential retry
		if err != nil {
			lastErr = err
			log.Printf("Request failed (attempt %d): %v", attempt+1, err)
		} else {
			// Non-200 status code
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			log.Printf("Request returned status %d (attempt %d)", resp.StatusCode, attempt+1)
		}

		// Don't retry on client errors (4xx)
		if resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return nil, lastErr
		}
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// SchemasResponse is the response from /databases/_system/schemas
type SchemasResponse struct {
	Count    int      `json:"count"`
	Database string   `json:"database"`
	Schemas  []string `json:"schemas"`
}

// TablesResponse is the response from /databases/_system/schemas/{schema}/tables
type TablesResponse struct {
	Count    int      `json:"count"`
	Database string   `json:"database"`
	Schema   string   `json:"schema"`
	Tables   []string `json:"tables"`
}

// SeriesMetric holds the result of a single row
type SeriesMetric struct {
	Service string `json:"service"`
	Metric  string `json:"metric"`
	Count   int64  `json:"c"`
	Schema  string // Schema name
	Table   string // Table name
}

func main() {
	// Load configuration
	config, err := loadConfig("config.toml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create ticker with configured interval
	interval := time.Duration(config.IntervalMinutes) * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Printf("Metric sender started. Running every %d minutes.\n", config.IntervalMinutes)
	fmt.Printf("URL: %s\n", config.URL)
	fmt.Printf("Database: %s\n", config.Database)
	fmt.Printf("Press Ctrl+C to stop.\n\n")

	// Run immediately on start
	runQuery(config)

	// Run on ticker
	for {
		select {
		case <-ticker.C:
			runQuery(config)
		case sig := <-sigChan:
			fmt.Printf("\n\nReceived signal %v. Shutting down gracefully...\n", sig)
			return
		}
	}
}

func runQuery(config *Config) {
	// Create HTTP client with configured timeouts (reuse for efficiency)
	client := createHTTPClient(config)

	// Get current date and time
	now := time.Now()
	currentDate := now.Format("2006-01-02")
	currentTime := now.Format("2006-01-02 15:04:05")

	fmt.Printf("=== %s ===\n", currentTime)
	fmt.Printf("Step 1: Discovering schemas from %s...\n", config.Database)

	// Step 1: Get all schemas
	schemas, err := getAllSchemas(config, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting schemas: %v\n\n", err)
		return
	}

	if len(schemas) == 0 {
		fmt.Printf("No schemas found in %s\n\n", config.Database)
		return
	}

	fmt.Printf("Found %d schemas: %v\n\n", len(schemas), schemas)

	// Step 2 & 3: For each schema, get tables and query series data
	timestamp := now.UnixNano()
	totalTables := 0
	totalMetrics := 0

	for _, schema := range schemas {
		fmt.Printf("Step 2: Getting tables for schema '%s'...\n", schema)

		// Get tables for this schema
		tables, err := getTablesForSchema(config, client, schema)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Error getting tables for schema '%s': %v\n", schema, err)
			continue
		}

		if len(tables) == 0 {
			fmt.Printf("  No tables found in schema '%s'\n", schema)
			continue
		}

		fmt.Printf("  Found %d tables: %v\n\n", len(tables), tables)
		totalTables += len(tables)

		// Step 3: Query each table
		for _, table := range tables {
			fullTableName := fmt.Sprintf("%s.%s.%s", config.Database, schema, table)
			fmt.Printf("  Step 3: Querying %s...\n", fullTableName)

			// Build SQL query
			sqlQuery := fmt.Sprintf(
				"select service, metric, count(*) as c from %s.%s.%s where date = '%s' group by service, metric",
				config.Database, schema, table, currentDate,
			)

			// Query this table
			metrics, err := queryStatsService(config, client, sqlQuery)
			if err != nil {
				fmt.Fprintf(os.Stderr, "    Error querying %s: %v\n", fullTableName, err)
				continue
			}

			// Add schema and table name to each metric
			for i := range metrics {
				metrics[i].Schema = schema
				metrics[i].Table = table
			}
			totalMetrics += len(metrics)

			fmt.Printf("    Found %d series\n", len(metrics))
			sendMetrics(metrics, table, timestamp)
		}
		fmt.Println()
	}

	fmt.Printf("# Total metrics from %d schemas, %d tables: %d\n\n", len(schemas), totalTables, totalMetrics)

	fmt.Printf("\nDone. Scraped %d series metrics from %d schemas (%d tables) for date %s, used: %s\n",
		totalMetrics, len(schemas), totalTables, currentDate, time.Since(now))
	fmt.Printf("Next run in %d minutes...\n\n", config.IntervalMinutes)
}

func getAllSchemas(config *Config, client *http.Client) ([]string, error) {
	// GET /databases/_system/schemas
	url := fmt.Sprintf("%s/databases/%s/schemas", config.URL, config.Database)

	retryDelay := time.Duration(config.RetryDelaySecs) * time.Second
	resp, err := doRequestWithRetry(client, url, config.MaxRetries, retryDelay)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse JSON response
	var response SchemasResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return response.Schemas, nil
}

func getTablesForSchema(config *Config, client *http.Client, schema string) ([]string, error) {
	// GET /databases/_system/schemas/{schema}/tables
	url := fmt.Sprintf("%s/databases/%s/schemas/%s/tables",
		config.URL, config.Database, url.PathEscape(schema))

	retryDelay := time.Duration(config.RetryDelaySecs) * time.Second
	resp, err := doRequestWithRetry(client, url, config.MaxRetries, retryDelay)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse JSON response
	var response TablesResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return response.Tables, nil
}

func queryStatsService(config *Config, client *http.Client, sqlQuery string) ([]SeriesMetric, error) {
	// URL encode the SQL query
	params := url.Values{}
	params.Add("sql", sqlQuery)

	// Build full URL: /query/_system
	fullURL := fmt.Sprintf("%s/query/%s?%s", config.URL, config.Database, params.Encode())

	// Make HTTP GET request with retry
	retryDelay := time.Duration(config.RetryDelaySecs) * time.Second
	resp, err := doRequestWithRetry(client, fullURL, config.MaxRetries, retryDelay)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse JSON response directly as array of SeriesMetric
	var metrics []SeriesMetric
	if err := json.Unmarshal(body, &metrics); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return metrics, nil
}

func sendMetrics(ms []SeriesMetric, table string, timestamp int64) {
	// Format: metrics,series_count,schema={schema},table={table},service={service},metric={metric} cnt={count} {timestamp}
	for _, m := range ms {
		fmt.Printf("metrics,series_count,schema=%s,table=%s,service=%s,metric=%s cnt=%d %d\n",
			m.Schema,
			m.Table,
			m.Service,
			m.Metric,
			m.Count,
			timestamp,
		)
	}
}
