#!/bin/bash
# Clean restart script for deepcoin-hft-server v16.23
set -e
cd /home/deepcoin/deepcoin-hft-server

echo "=== Step 1: Kill all old processes ==="
# Kill by pattern matching on the command line
for pid in $(ps aux | grep '[d]eepcoin-hft-server.*gunicorn' | awk '{print $2}'); do
    echo "Killing $pid"
    kill -9 $pid 2>/dev/null || true
done
sleep 3

# Double check
remaining=$(ps aux | grep '[d]eepcoin-hft-server.*gunicorn' | wc -l)
echo "Remaining gunicorn processes: $remaining"

echo "=== Step 2: Clear pycache ==="
rm -rf __pycache__ ./*/__pycache__
echo "pycache cleared"

echo "=== Step 3: Remove stale singleton lock ==="
rm -f logs/.recover_singleton.lock
echo "Lock removed"

echo "=== Step 4: Start fresh gunicorn ==="
./venv/bin/gunicorn -w 1 --threads 4 -b 0.0.0.0:5004 --timeout 300 --keep-alive 5 app:app >> logs/gunicorn_access.log 2>&1 &
echo "Gunicorn started in background"

sleep 8

echo "=== Step 5: Health check ==="
curl -s http://127.0.0.1:5004/health

echo ""
echo "=== Step 6: Check new processes ==="
ps aux | grep '[d]eepcoin-hft-server.*gunicorn'

echo "=== Done ==="
