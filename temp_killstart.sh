#!/bin/bash
echo "Current time: $(date)"
echo "Processes to kill:"
ps aux | grep gunicorn | grep deepcoin | grep -v grep
echo "Killing..."
ps aux | grep gunicorn | grep deepcoin | grep -v grep | awk '{print $2}' | while read pid; do
  echo "Killing $pid"
  kill -9 $pid 2>/dev/null && echo "Killed $pid" || echo "Failed to kill $pid"
done
sleep 2
echo "After kill:"
ps aux | grep gunicorn | grep deepcoin | grep -v grep | wc -l
echo "Starting fresh gunicorn..."
cd /home/deepcoin/deepcoin-hft-server
rm -rf __pycache__ ./*/__pycache__
./venv/bin/gunicorn -w 1 --threads 4 -b 0.0.0.0:5004 --timeout 300 --keep-alive 5 app:app >> logs/gunicorn_access.log 2>&1 &
sleep 5
echo "Health check:"
curl -s http://127.0.0.1:5004/health
echo ""
echo "New processes:"
ps aux | grep gunicorn | grep deepcoin | grep -v grep
