#!/bin/bash
# Kill all deepcoin gunicorn processes
pkill -9 -f "deepcoin-hft-server.*gunicorn"
sleep 2
# Start fresh
cd /home/deepcoin/deepcoin-hft-server
/home/deepcoin/deepcoin-hft-server/venv/bin/gunicorn -w 1 --threads 4 -b 0.0.0.0:5004 --timeout 300 --keep-alive 5 app:app >> /home/deepcoin/deepcoin-hft-server/logs/gunicorn_access.log 2>&1 &
sleep 3
ps aux | grep gunicorn | grep deepcoin
echo "STARTED"
