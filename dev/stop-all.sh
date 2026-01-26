#!/bin/bash
#
# Stop all MynaDB processes
#

echo "Stopping all MynaDB processes..."

# Kill all mynadb processes
pkill -f "mynadb" 2>/dev/null || true

# Also kill any goreman if running
pkill -f "goreman" 2>/dev/null || true

echo "Done."

# Show remaining processes if any
REMAINING=$(pgrep -f "mynadb" 2>/dev/null || true)
if [ -n "$REMAINING" ]; then
    echo ""
    echo "Warning: Some processes may still be running:"
    ps -p $REMAINING -o pid,cmd 2>/dev/null || true
    echo ""
    echo "Use 'kill -9 <pid>' to force kill if needed"
fi
