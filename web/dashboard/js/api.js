/**
 * MynaDB API Client
 * Handles all communication with the MynaDB server
 */

const API = {
    // Always use relative URL - proxy server handles forwarding to MynaDB
    get baseUrl() {
        // Use empty string for relative URLs (works with proxy)
        return '';
    },
    database: 'testdb',
    schema: 'prod',
    table: 'cpu',

    /**
     * Execute a SQL query against MynaDB
     * @param {string} sql - SQL query string
     * @returns {Promise<Array>} Query results
     */
    async query(sql) {
        try {
            const response = await fetch(`${this.baseUrl}/query/${this.database}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain',
                },
                body: sql
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText);
            }

            const text = await response.text();

            // Handle empty response
            if (!text || text.trim() === '' || text === '[]') {
                return [];
            }

            const parsed = JSON.parse(text);

            // MynaDB's query endpoint wraps rows in { success, data, rows, error }.
            // Unwrap to the row array. A bare array (older/standalone builds) is
            // returned as-is for backward compatibility.
            if (Array.isArray(parsed)) {
                return parsed;
            }
            if (parsed && typeof parsed === 'object') {
                if (parsed.success === false) {
                    throw new Error(parsed.error || 'Query failed');
                }
                return Array.isArray(parsed.data) ? parsed.data : [];
            }
            return [];
        } catch (error) {
            console.error('Query error:', error);
            throw error;
        }
    },

    /**
     * Get aggregated CPU metrics grouped by host
     * @returns {Promise<Array>} Aggregated metrics
     */
    async getAggregatedMetrics() {
        const sql = `
            SELECT
                host,
                region,
                AVG("user") as avg_user,
                AVG(idle) as avg_idle,
                AVG(system) as avg_system,
                MAX("user") as max_user,
                MIN("user") as min_user
            FROM ${this.schema}.${this.table}
            GROUP BY host, region
            ORDER BY avg_user DESC
        `;
        return this.query(sql);
    },

    /**
     * Convert seconds to InfluxDB-style interval string
     * @param {number} secs - Seconds
     * @returns {string} Interval string (e.g., "30s", "5m", "1h")
     */
    formatInterval(secs) {
        if (secs >= 3600 && secs % 3600 === 0) {
            return `${secs / 3600}h`;
        } else if (secs >= 60 && secs % 60 === 0) {
            return `${secs / 60}m`;
        }
        return `${secs}s`;
    },

    /**
     * Get time series data for a specific host with time bucketing and time range
     * Uses InfluxDB-compatible GROUP BY time(interval) syntax
     * @param {string} host - Host name (optional, null for all hosts)
     * @param {number} bucketSecs - Time bucket size in seconds (default 30)
     * @param {number} rangeMinutes - Time range in minutes (default 15)
     * @returns {Promise<Array>} Time series data
     */
    async getTimeSeries(host = null, bucketSecs = 30, rangeMinutes = 15) {
        const interval = this.formatInterval(bucketSecs);

        // Build WHERE clause (host filter only - time filter has server-side issue)
        let whereClause = '';
        if (host && host !== 'all') {
            whereClause = `WHERE host = '${host}'`;
        }

        // Use InfluxDB-compatible GROUP BY time(interval) syntax
        // The transformer automatically adds ORDER BY time ASC
        // Note: Query must be single-line for GROUP BY time() transformer regex
        const sql = `SELECT host, region, AVG("user") as user_cpu, AVG(idle) as idle, AVG(system) as system FROM ${this.schema}.${this.table} ${whereClause} GROUP BY time(${interval}), host, region`;

        return this.query(sql);
    },

    /**
     * Get latest metrics for all hosts
     * @returns {Promise<Array>} Latest metrics per host
     */
    async getLatestMetrics() {
        const sql = `
            SELECT
                host,
                region,
                "user" as user_cpu,
                idle,
                system,
                time
            FROM ${this.schema}.${this.table}
            ORDER BY time DESC
            LIMIT 10
        `;
        return this.query(sql);
    },

    /**
     * Get all available hosts
     * @returns {Promise<Array>} List of unique hosts
     */
    async getHosts() {
        const sql = `
            SELECT DISTINCT host, region
            FROM ${this.schema}.${this.table}
            ORDER BY host
        `;
        return this.query(sql);
    },

    /**
     * Check server health
     * @returns {Promise<boolean>} True if server is healthy
     */
    async checkHealth() {
        try {
            const response = await fetch(`${this.baseUrl}/health`);
            return response.ok;
        } catch {
            return false;
        }
    },

    /**
     * Get cache statistics
     * @returns {Promise<Object>} Cache stats
     */
    async getCacheStats() {
        try {
            const response = await fetch(`${this.baseUrl}/stats`);
            return await response.text();
        } catch (error) {
            console.error('Stats error:', error);
            return null;
        }
    }
};

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = API;
}
