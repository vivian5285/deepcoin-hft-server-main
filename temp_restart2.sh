#!/bin/bash
cd /home/deepcoin/deepcoin-hft-server
echo "Killing old gunicorn..."
for pid in $(pgrep -f "deepcoin-hft-server.*gunicorn"); do
  kill -9 $pid 2>/dev/null
  echo "Killed $pid"
done
sleep 2
echo "Starting new gunicorn..."
./venv/bin/gunicorn -w 1 --threads 4 -b 0.0.0.0:5004 --timeout 300 --keep-alive 5 app:app >> logs/gunicorn_access.log 2>&1 &
sleep 4
echo "New processes:"
ps aux | grep gunicorn | grep deepcoin | grep -v grep
echo "DONE"
