#!/usr/bin/env bash
#
# generate-data.sh - Generate realistic test data for MynaDB
#
# Writes time-series data via InfluxDB line protocol to test and explore the database.
#
# Usage:
#   ./dev/generate-data.sh                                    # Interactive mode
#   ./dev/generate-data.sh --type cpu,http                    # Specific types
#   ./dev/generate-data.sh --type all --points 500            # All types, 500 pts/source
#   ./dev/generate-data.sh --type iot --date 2025-01-15       # Historical data
#   ./dev/generate-data.sh --type cpu --endpoint http://localhost:5000  # Direct to data node
#
set -euo pipefail

# ============ Defaults ============

ENDPOINT="${MYNADB_ENDPOINT:-http://localhost:7000}"
DATABASE="${MYNADB_DATABASE:-testdb}"
POINTS=100
DATE=""
TYPES=""
BATCH_SIZE=200
QUIET=false

# ============ Colors ============

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ============ Argument Parsing ============

usage() {
    cat <<'EOF'
MynaDB Test Data Generator

Usage: ./dev/generate-data.sh [OPTIONS]

Options:
  --type TYPE       Data types to generate (comma-separated or 'all')
                    Available: cpu, memory, disk, http, iot, app
  --points N        Points per source (default: 100)
  --date YYYY-MM-DD Date for data (default: today)
  --db NAME         Database name (default: testdb)
  --endpoint URL    Write endpoint (default: http://localhost:7000)
  --batch N         Batch size for writes (default: 200)
  --quiet           Minimal output
  -h, --help        Show this help

Environment:
  MYNADB_ENDPOINT   Default write endpoint
  MYNADB_DATABASE   Default database name

Examples:
  ./dev/generate-data.sh                          # Interactive menu
  ./dev/generate-data.sh --type cpu,http          # CPU + HTTP metrics
  ./dev/generate-data.sh --type all --points 500  # Everything, 500 pts/source
  ./dev/generate-data.sh --type iot --date 2025-01-15 --db sensors
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --type)     TYPES="$2"; shift 2 ;;
        --points)   POINTS="$2"; shift 2 ;;
        --date)     DATE="$2"; shift 2 ;;
        --db)       DATABASE="$2"; shift 2 ;;
        --endpoint) ENDPOINT="$2"; shift 2 ;;
        --batch)    BATCH_SIZE="$2"; shift 2 ;;
        --quiet)    QUIET=true; shift ;;
        -h|--help)  usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ============ Helpers ============

# Random integer in [min, max]
rand_int() {
    echo $(( RANDOM % ($2 - $1 + 1) + $1 ))
}

# Random float with 1 decimal place in [min, max]
rand_float() {
    local min=$1 max=$2
    local range=$(( (max - min) * 10 ))
    if [[ $range -le 0 ]]; then
        echo "${min}.0"
        return
    fi
    local val=$(( RANDOM % (range + 1) + min * 10 ))
    echo "$(( val / 10 )).$(( val % 10 ))"
}

# Random element from arguments
rand_elem() {
    local arr=("$@")
    echo "${arr[RANDOM % ${#arr[@]}]}"
}

# Calculate timestamps for the date range
setup_timestamps() {
    if [[ -z "$DATE" ]]; then
        DATE=$(date +%Y-%m-%d)
    fi

    # Validate date format
    if ! date -d "$DATE" +%s &>/dev/null; then
        echo -e "${RED}Invalid date: $DATE${NC}"
        exit 1
    fi

    local day_start
    day_start=$(date -d "$DATE 00:00:00" +%s)
    local day_end
    day_end=$(date -d "$DATE 23:59:59" +%s)
    local now
    now=$(date +%s)

    # If today, only generate up to current time
    if [[ "$DATE" == "$(date +%Y-%m-%d)" ]] && [[ $day_end -gt $now ]]; then
        day_end=$now
    fi

    START_SEC=$day_start
    local duration=$(( day_end - day_start ))
    if [[ $POINTS -le 1 ]]; then
        INTERVAL_SEC=0
    else
        INTERVAL_SEC=$(( duration / (POINTS - 1) ))
        if [[ $INTERVAL_SEC -lt 1 ]]; then
            INTERVAL_SEC=1
        fi
    fi
}

# Get nanosecond timestamp for point index
ts_for() {
    local idx=$1
    local sec=$(( START_SEC + idx * INTERVAL_SEC ))
    echo "${sec}000000000"
}

# Write data from stdin to the database in batches
write_data() {
    local label=$1
    local tmpfile
    tmpfile=$(mktemp)
    cat > "$tmpfile"

    local total
    total=$(wc -l < "$tmpfile")
    if [[ $total -eq 0 ]]; then
        echo -e "  ${YELLOW}No data generated${NC}"
        rm -f "$tmpfile"
        return 0
    fi

    local batches=$(( (total + BATCH_SIZE - 1) / BATCH_SIZE ))
    local sent=0
    local errors=0
    local batch_num=0
    local offset=1

    while [[ $offset -le $total ]]; do
        local chunk
        chunk=$(sed -n "${offset},$(( offset + BATCH_SIZE - 1 ))p" "$tmpfile")
        ((batch_num++))

        local http_code
        http_code=$(printf '%s\n' "$chunk" | curl -s -o /dev/null -w "%{http_code}" \
            -X POST "${ENDPOINT}/write/${DATABASE}" \
            --data-binary @- 2>/dev/null) || http_code="000"

        local chunk_lines
        chunk_lines=$(echo "$chunk" | wc -l)

        if [[ "$http_code" =~ ^2 ]]; then
            sent=$(( sent + chunk_lines ))
            if [[ "$QUIET" != "true" ]]; then
                printf "\r  Writing batch %d/%d... ${GREEN}OK${NC}  " "$batch_num" "$batches"
            fi
        else
            errors=$(( errors + 1 ))
            if [[ "$QUIET" != "true" ]]; then
                printf "\r  Writing batch %d/%d... ${RED}HTTP %s${NC}  " "$batch_num" "$batches" "$http_code"
            fi
        fi

        offset=$(( offset + BATCH_SIZE ))
    done

    if [[ "$QUIET" != "true" ]]; then
        if [[ $errors -eq 0 ]]; then
            printf "\r  ${GREEN}Done${NC}: %d points written to ${BOLD}%s${NC}                    \n" "$sent" "$DATABASE"
        else
            printf "\r  ${YELLOW}Done${NC}: %d/%d points written (%d batch errors)              \n" "$sent" "$total" "$errors"
        fi
    fi

    TOTAL_WRITTEN=$(( TOTAL_WRITTEN + sent ))
    rm -f "$tmpfile"
}

# ============ Data Generators ============

generate_cpu() {
    local hosts=("web-1" "web-2" "web-3" "api-1" "db-1")
    local regions=("us-east" "us-west" "eu-west" "us-east" "us-west")

    for i in "${!hosts[@]}"; do
        local host="${hosts[$i]}"
        local region="${regions[$i]}"
        # Each host has a different base load
        local base_user=$(( 10 + i * 8 ))
        local base_sys=$(( 3 + i * 2 ))

        for p in $(seq 0 $(( POINTS - 1 ))); do
            local ts
            ts=$(ts_for "$p")
            # Add variation to base load
            local usage_user=$(( base_user + RANDOM % 20 - 5 ))
            [[ $usage_user -lt 1 ]] && usage_user=1
            [[ $usage_user -gt 99 ]] && usage_user=99
            local usage_system=$(( base_sys + RANDOM % 8 - 3 ))
            [[ $usage_system -lt 0 ]] && usage_system=0
            local usage_idle=$(( 100 - usage_user - usage_system ))
            [[ $usage_idle -lt 0 ]] && usage_idle=0
            local load_1m
            load_1m=$(rand_float 0 8)
            local load_5m
            load_5m=$(rand_float 0 6)
            local cores=8

            echo "cpu,host=${host},region=${region},service=monitoring usage_user=${usage_user}.0,usage_system=${usage_system}.0,usage_idle=${usage_idle}.0,load_1m=${load_1m},load_5m=${load_5m},cores=${cores}i ${ts}"
        done
    done
}

generate_memory() {
    local hosts=("web-1" "web-2" "web-3" "api-1" "db-1")
    local regions=("us-east" "us-west" "eu-west" "us-east" "us-west")
    local totals=(16384 16384 16384 32768 65536)

    for i in "${!hosts[@]}"; do
        local host="${hosts[$i]}"
        local region="${regions[$i]}"
        local total_mb="${totals[$i]}"
        local base_used=$(( total_mb * (50 + i * 8) / 100 ))

        for p in $(seq 0 $(( POINTS - 1 ))); do
            local ts
            ts=$(ts_for "$p")
            local used_mb=$(( base_used + RANDOM % (total_mb / 10) - total_mb / 20 ))
            [[ $used_mb -lt 512 ]] && used_mb=512
            [[ $used_mb -gt $(( total_mb - 256 )) ]] && used_mb=$(( total_mb - 256 ))
            local available_mb=$(( total_mb - used_mb ))
            local usage_pct=$(( used_mb * 100 / total_mb ))
            local swap_used=$(( RANDOM % 512 ))

            echo "memory,host=${host},region=${region},service=monitoring total_mb=${total_mb}i,used_mb=${used_mb}i,available_mb=${available_mb}i,usage_percent=${usage_pct}.0,swap_used_mb=${swap_used}i ${ts}"
        done
    done
}

generate_disk() {
    local hosts=("web-1" "web-2" "web-3" "api-1" "db-1")
    local regions=("us-east" "us-west" "eu-west" "us-east" "us-west")
    local devices=("sda" "sdb")

    for i in "${!hosts[@]}"; do
        local host="${hosts[$i]}"
        local region="${regions[$i]}"

        for device in "${devices[@]}"; do
            for p in $(seq 0 $(( POINTS - 1 ))); do
                local ts
                ts=$(ts_for "$p")
                local read_bytes=$(( RANDOM * 1024 + RANDOM ))
                local write_bytes=$(( RANDOM * 512 + RANDOM ))
                local read_ops=$(( RANDOM % 500 + 10 ))
                local write_ops=$(( RANDOM % 300 + 5 ))
                local io_util
                io_util=$(rand_float 0 95)

                echo "disk_io,host=${host},region=${region},device=${device},service=monitoring read_bytes=${read_bytes}i,write_bytes=${write_bytes}i,read_ops=${read_ops}i,write_ops=${write_ops}i,io_util=${io_util} ${ts}"
            done
        done
    done
}

generate_http() {
    local services=("frontend" "api-gateway" "auth-service")
    local methods=("GET" "POST" "PUT" "DELETE" "GET" "GET" "GET" "POST")
    local endpoints_frontend=("/index.html" "/dashboard" "/login" "/settings" "/api/data" "/search" "/profile")
    local endpoints_api=("/v1/users" "/v1/orders" "/v1/products" "/v1/search" "/v1/health" "/v1/metrics")
    local endpoints_auth=("/auth/login" "/auth/token" "/auth/refresh" "/auth/logout" "/auth/verify")
    local statuses=("200" "200" "200" "200" "200" "200" "200" "200" "201" "204" "301" "400" "401" "404" "500")

    for svc in "${services[@]}"; do
        local svc_tag="${svc}"

        for p in $(seq 0 $(( POINTS - 1 ))); do
            local ts
            ts=$(ts_for "$p")
            local method
            method=$(rand_elem "${methods[@]}")
            local endpoint
            case $svc in
                frontend)   endpoint=$(rand_elem "${endpoints_frontend[@]}") ;;
                api-gateway) endpoint=$(rand_elem "${endpoints_api[@]}") ;;
                auth-service) endpoint=$(rand_elem "${endpoints_auth[@]}") ;;
            esac
            local status
            status=$(rand_elem "${statuses[@]}")

            # Latency: most requests fast, some slow
            local latency
            if [[ $(( RANDOM % 100 )) -lt 90 ]]; then
                latency=$(rand_float 1 50)
            elif [[ $(( RANDOM % 100 )) -lt 95 ]]; then
                latency=$(rand_float 50 200)
            else
                latency=$(rand_float 200 2000)
            fi

            local req_bytes=$(( RANDOM % 4096 + 64 ))
            local resp_bytes=$(( RANDOM % 65536 + 128 ))

            echo "http_requests,service=${svc_tag},method=${method},endpoint=${endpoint},status=${status} latency_ms=${latency},request_bytes=${req_bytes}i,response_bytes=${resp_bytes}i ${ts}"
        done
    done
}

generate_iot() {
    local sensor_ids=("sensor-01" "sensor-02" "sensor-03" "sensor-04" "sensor-05" "sensor-06" "sensor-07" "sensor-08")
    local locations=("warehouse-a" "warehouse-a" "warehouse-b" "warehouse-b" "office-1" "office-1" "outdoor" "outdoor")
    local sensor_types=("environmental" "environmental" "environmental" "environmental" "environmental" "environmental" "weather" "weather")

    for i in "${!sensor_ids[@]}"; do
        local sid="${sensor_ids[$i]}"
        local loc="${locations[$i]}"
        local stype="${sensor_types[$i]}"

        # Base temperature depends on location
        local base_temp
        case $loc in
            warehouse-*) base_temp=22 ;;
            office-*)    base_temp=24 ;;
            outdoor)     base_temp=18 ;;
        esac

        local battery=100

        for p in $(seq 0 $(( POINTS - 1 ))); do
            local ts
            ts=$(ts_for "$p")

            # Temperature: daily cycle (cooler at night, warmer midday)
            local hour_frac=$(( p * 24 / POINTS ))
            local temp_offset=0
            if [[ $hour_frac -ge 6 && $hour_frac -le 14 ]]; then
                temp_offset=$(( (hour_frac - 6) ))
            elif [[ $hour_frac -gt 14 && $hour_frac -le 22 ]]; then
                temp_offset=$(( 22 - hour_frac ))
            else
                temp_offset=-2
            fi

            local temp_whole=$(( base_temp + temp_offset + RANDOM % 3 - 1 ))
            local temp_dec=$(( RANDOM % 10 ))
            local temperature="${temp_whole}.${temp_dec}"

            local humidity
            humidity=$(rand_float 30 80)
            local pressure
            pressure=$(rand_float 1010 1025)

            # Battery slowly drains
            battery=$(( 100 - p * 15 / POINTS + RANDOM % 3 - 1 ))
            [[ $battery -lt 5 ]] && battery=5
            [[ $battery -gt 100 ]] && battery=100

            echo "sensor_data,sensor_id=${sid},location=${loc},type=${stype},service=iot_gateway temperature=${temperature},humidity=${humidity},pressure=${pressure},battery_pct=${battery}.0 ${ts}"
        done
    done
}

generate_app() {
    local services=("order-service" "payment-service" "notification-service")
    local hosts_per_service=("app-order-1" "app-payment-1" "app-notify-1")
    local levels=("info" "info" "info" "info" "info" "warn" "error")

    for i in "${!services[@]}"; do
        local svc="${services[$i]}"
        local host="${hosts_per_service[$i]}"

        for p in $(seq 0 $(( POINTS - 1 ))); do
            local ts
            ts=$(ts_for "$p")
            local level
            level=$(rand_elem "${levels[@]}")

            local response_time
            if [[ "$level" == "error" ]]; then
                response_time=$(rand_float 500 5000)
            else
                response_time=$(rand_float 5 200)
            fi

            local queue_depth=$(( RANDOM % 50 ))
            local error_count=0
            [[ "$level" == "error" ]] && error_count=$(( RANDOM % 5 + 1 ))
            [[ "$level" == "warn" ]] && error_count=$(( RANDOM % 2 ))

            # Active users: higher during business hours
            local hour_frac=$(( p * 24 / POINTS ))
            local active_users
            if [[ $hour_frac -ge 9 && $hour_frac -le 17 ]]; then
                active_users=$(( RANDOM % 500 + 200 ))
            else
                active_users=$(( RANDOM % 100 + 10 ))
            fi

            echo "app_events,service=${svc},host=${host},level=${level} response_time_ms=${response_time},queue_depth=${queue_depth}i,error_count=${error_count}i,active_users=${active_users}i ${ts}"
        done
    done
}

# ============ Type Metadata ============

type_description() {
    case $1 in
        cpu)    echo "CPU usage, load averages (5 hosts)" ;;
        memory) echo "Memory & swap usage (5 hosts)" ;;
        disk)   echo "Disk I/O throughput (5 hosts x 2 devices)" ;;
        http)   echo "HTTP request metrics (3 services)" ;;
        iot)    echo "IoT sensor readings (8 sensors)" ;;
        app)    echo "Application metrics (3 services)" ;;
    esac
}

type_source_count() {
    case $1 in
        cpu)    echo 5 ;;
        memory) echo 5 ;;
        disk)   echo 10 ;;
        http)   echo 3 ;;
        iot)    echo 8 ;;
        app)    echo 3 ;;
    esac
}

ALL_TYPES=("cpu" "memory" "disk" "http" "iot" "app")

# ============ Interactive Mode ============

interactive_menu() {
    echo ""
    echo -e "${BOLD}MynaDB Test Data Generator${NC}"
    echo -e "${DIM}══════════════════════════${NC}"
    echo ""
    echo -e "  Endpoint: ${CYAN}${ENDPOINT}${NC}"
    echo -e "  Database: ${CYAN}${DATABASE}${NC}"
    echo ""
    echo -e "${BOLD}Data types:${NC}"
    echo ""
    for i in "${!ALL_TYPES[@]}"; do
        local t="${ALL_TYPES[$i]}"
        local n
        n=$(type_source_count "$t")
        local desc
        desc=$(type_description "$t")
        printf "  ${BOLD}%d)${NC} %-8s - %s\n" "$(( i + 1 ))" "$t" "$desc"
    done
    echo ""
    printf "  ${BOLD}7)${NC} %-8s - All of the above\n" "all"
    echo ""

    # Select types
    local selection
    read -rp "$(echo -e "${BOLD}Select${NC} [1-7, or comma-separated]: ")" selection

    if [[ -z "$selection" || "$selection" == "7" || "$selection" == "all" ]]; then
        TYPES="all"
    else
        local selected=()
        IFS=',' read -ra parts <<< "$selection"
        for part in "${parts[@]}"; do
            part=$(echo "$part" | tr -d ' ')
            case $part in
                1|cpu)    selected+=("cpu") ;;
                2|memory) selected+=("memory") ;;
                3|disk)   selected+=("disk") ;;
                4|http)   selected+=("http") ;;
                5|iot)    selected+=("iot") ;;
                6|app)    selected+=("app") ;;
                7|all)    TYPES="all"; return ;;
                *) echo -e "${YELLOW}Ignoring unknown: $part${NC}" ;;
            esac
        done
        TYPES=$(IFS=','; echo "${selected[*]}")
    fi

    # Points
    local pts_input
    read -rp "$(echo -e "${BOLD}Points per source${NC} [${POINTS}]: ")" pts_input
    if [[ -n "$pts_input" ]]; then
        POINTS=$pts_input
    fi

    # Date
    local date_input
    read -rp "$(echo -e "${BOLD}Date${NC} [${DATE:-today}]: ")" date_input
    if [[ -n "$date_input" ]]; then
        DATE=$date_input
    fi

    echo ""
}

# ============ Pre-flight Check ============

check_endpoint() {
    if [[ "$QUIET" != "true" ]]; then
        printf "Checking endpoint ${CYAN}%s${NC}... " "$ENDPOINT"
    fi

    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
        "${ENDPOINT}/health" 2>/dev/null) || http_code="000"

    if [[ "$http_code" == "000" ]]; then
        echo -e "${RED}unreachable${NC}"
        echo ""
        echo -e "Cannot connect to ${BOLD}${ENDPOINT}${NC}"
        echo "Make sure the cluster is running:"
        echo "  goreman -f dev/Procfile.cluster-local start"
        echo ""
        echo "Or specify a different endpoint:"
        echo "  $0 --endpoint http://localhost:5000"
        exit 1
    fi

    if [[ "$QUIET" != "true" ]]; then
        echo -e "${GREEN}OK${NC} (HTTP ${http_code})"
    fi
}

# ============ Main ============

TOTAL_WRITTEN=0

# Interactive mode if no --type specified
if [[ -z "$TYPES" ]]; then
    interactive_menu
fi

# Setup
setup_timestamps
check_endpoint

# Resolve types
declare -a selected_types
if [[ "$TYPES" == "all" ]]; then
    selected_types=("${ALL_TYPES[@]}")
else
    IFS=',' read -ra selected_types <<< "$TYPES"
fi

# Header
if [[ "$QUIET" != "true" ]]; then
    echo ""
    echo -e "${BOLD}Generating data${NC}"
    echo -e "  Date:     ${CYAN}${DATE}${NC}"
    echo -e "  Points:   ${CYAN}${POINTS}${NC} per source"
    echo -e "  Database: ${CYAN}${DATABASE}${NC}"
    echo ""
fi

# Generate and write each type
for t in "${selected_types[@]}"; do
    t=$(echo "$t" | tr -d ' ')
    if ! declare -f "generate_${t}" &>/dev/null; then
        echo -e "${YELLOW}Unknown type: $t (skipping)${NC}"
        continue
    fi

    local_count=$(type_source_count "$t")
    local_total=$(( local_count * POINTS ))
    local_desc=$(type_description "$t")

    if [[ "$QUIET" != "true" ]]; then
        echo -e "${BOLD}${t}${NC} - ${local_desc}"
        printf "  Generating %d sources x %d points = %d lines\n" "$local_count" "$POINTS" "$local_total"
    fi

    "generate_${t}" | write_data "$t"
done

# Summary
if [[ "$QUIET" != "true" ]]; then
    echo ""
    echo -e "${GREEN}${BOLD}Total: ${TOTAL_WRITTEN} points written to ${DATABASE}${NC}"
    echo ""
    echo -e "${DIM}Try these queries:${NC}"
    echo ""

    for t in "${selected_types[@]}"; do
        t=$(echo "$t" | tr -d ' ')
        case $t in
            cpu)
                echo "  # Average CPU usage by host"
                echo -e "  ${CYAN}curl '${ENDPOINT}/query/${DATABASE}' -d 'sql=SELECT host, AVG(usage_user) as avg_cpu FROM monitoring.cpu GROUP BY host'${NC}"
                ;;
            memory)
                echo "  # Memory usage across hosts"
                echo -e "  ${CYAN}curl '${ENDPOINT}/query/${DATABASE}' -d 'sql=SELECT host, MAX(usage_percent) as max_mem FROM monitoring.memory GROUP BY host'${NC}"
                ;;
            http)
                echo "  # Request latency by service"
                echo -e "  ${CYAN}curl '${ENDPOINT}/query/${DATABASE}' -d 'sql=SELECT service, AVG(latency_ms) as avg_latency, COUNT(*) as requests FROM http_requests.http_requests GROUP BY service'${NC}"
                ;;
            iot)
                echo "  # Sensor temperature readings"
                echo -e "  ${CYAN}curl '${ENDPOINT}/query/${DATABASE}' -d 'sql=SELECT sensor_id, AVG(temperature) as avg_temp FROM iot_gateway.sensor_data GROUP BY sensor_id'${NC}"
                ;;
            app)
                echo "  # Error rates by service"
                echo -e "  ${CYAN}curl '${ENDPOINT}/query/${DATABASE}' -d 'sql=SELECT service, SUM(error_count) as errors FROM app_events GROUP BY service'${NC}"
                ;;
        esac
    done

    echo ""
    echo "  # Browse in admin UI"
    echo -e "  ${CYAN}http://localhost:18080/admin/query${NC}"
    echo ""
fi
