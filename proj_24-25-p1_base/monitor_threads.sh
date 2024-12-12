#!/bin/bash
PID=$1  # Pass your program's PID as the first argument
MAX_THREADS=0

while kill -0 $PID 2>/dev/null; do
    THREAD_COUNT=$(ls /proc/$PID/task | wc -l)
    if (( THREAD_COUNT > MAX_THREADS )); then
        MAX_THREADS=$THREAD_COUNT
    fi
    sleep 0.1  # Poll every 100ms
done

echo "Maximum concurrent threads: $MAX_THREADS"
