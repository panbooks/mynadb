#!/bin/bash
# Continuous data generator for MynaDB dashboard testing

MYNADB_URL="http://localhost:7000"
DB="testdb"

# Host configurations
HOSTS=("web01" "web02" "db01")
REGIONS=("us-east" "us-west" "us-east")

# Generate random CPU value within range
random_cpu() {
    local base=$1
    local variance=$2
    echo "scale=1; $base + ($RANDOM % $variance) - ($variance / 2)" | bc
}

echo "Starting continuous data generator..."
echo "Press Ctrl+C to stop"
echo "Writing to: $MYNADB_URL/write/$DB"
echo ""

counter=0
while true; do
    timestamp=$(date +%s)000000000

    for i in "${!HOSTS[@]}"; do
        host=${HOSTS[$i]}
        region=${REGIONS[$i]}

        # Different base values for different hosts
        case $host in
            "db01")
                user=$(random_cpu 70 20)   # High CPU (60-80)
                idle=$(random_cpu 20 15)   # Low idle
                system=$(random_cpu 10 5)  # Moderate system
                ;;
            "web01")
                user=$(random_cpu 45 20)   # Medium CPU (35-55)
                idle=$(random_cpu 40 15)   # Medium idle
                system=$(random_cpu 12 5)  # Moderate system
                ;;
            "web02")
                user=$(random_cpu 25 15)   # Low CPU (17-32)
                idle=$(random_cpu 65 15)   # High idle
                system=$(random_cpu 9 4)   # Low system
                ;;
        esac

        # Ensure values are positive
        user=$(echo "$user" | awk '{if($1<1) print 1; else print $1}')
        idle=$(echo "$idle" | awk '{if($1<1) print 1; else print $1}')
        system=$(echo "$system" | awk '{if($1<1) print 1; else print $1}')

        data="cpu,host=$host,region=$region,service=prod user=$user,idle=$idle,system=$system $timestamp"

        curl -s -X POST "$MYNADB_URL/write/$DB" -d "$data" > /dev/null
    done

    counter=$((counter + 1))
    echo "[$(date '+%H:%M:%S')] Written batch $counter (3 hosts)"

    # Wait 5 seconds before next batch
    sleep 5
done
