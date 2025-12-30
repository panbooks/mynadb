package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	statsServiceURL = "http://localhost:7000"
	systemDatabase  = "_system"
)

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
	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create ticker for 5-minute intervals
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	fmt.Printf("Metric sender started. Running every 5 minutes.\n")
	fmt.Printf("Database: %s\n", systemDatabase)
	fmt.Printf("Press Ctrl+C to stop.\n\n")

	// Run immediately on start
	runQuery()

	// Run on ticker
	for {
		select {
		case <-ticker.C:
			runQuery()
		case sig := <-sigChan:
			fmt.Printf("\n\nReceived signal %v. Shutting down gracefully...\n", sig)
			return
		}
	}
}

func runQuery() {
	// Get current date and time
	now := time.Now()
	currentDate := now.Format("2006-01-02")
	currentTime := now.Format("2006-01-02 15:04:05")

	fmt.Printf("=== %s ===\n", currentTime)
	fmt.Printf("Step 1: Discovering schemas from %s...\n", systemDatabase)

	// Step 1: Get all schemas
	schemas, err := getAllSchemas()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting schemas: %v\n\n", err)
		return
	}

	if len(schemas) == 0 {
		fmt.Printf("No schemas found in %s\n\n", systemDatabase)
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
		tables, err := getTablesForSchema(schema)
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
			fullTableName := fmt.Sprintf("%s.%s.%s", systemDatabase, schema, table)
			fmt.Printf("  Step 3: Querying %s...\n", fullTableName)

			// Build SQL query
			sqlQuery := fmt.Sprintf(
				"select service, metric, count(*) as c from %s.%s.%s where date = '%s' group by service, metric",
				systemDatabase, schema, table, currentDate,
			)

			// Query this table
			metrics, err := queryStatsService(sqlQuery)
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
	fmt.Printf("Next run in 5 minutes...\n\n")
}

func getAllSchemas() ([]string, error) {
	// GET /databases/_system/schemas
	url := fmt.Sprintf("%s/databases/%s/schemas", statsServiceURL, systemDatabase)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

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

func getTablesForSchema(schema string) ([]string, error) {
	// GET /databases/_system/schemas/{schema}/tables
	url := fmt.Sprintf("%s/databases/%s/schemas/%s/tables",
		statsServiceURL, systemDatabase, url.PathEscape(schema))

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

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

func queryStatsService(sqlQuery string) ([]SeriesMetric, error) {
	// URL encode the SQL query
	params := url.Values{}
	params.Add("sql", sqlQuery)

	// Build full URL: /query/_system
	fullURL := fmt.Sprintf("%s/query/%s?%s", statsServiceURL, systemDatabase, params.Encode())

	// Make HTTP GET request
	resp, err := http.Get(fullURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

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
