# Linux write workload generator - deepak verma
#!/bin/bash
LOG="/opt/zerto/disk_workload.log"
PIDFILE="/opt/zerto/disk_workload.pid"
DATA_FILE="/opt/zerto/disk_workload.dat"
FILE_SIZE_GB=5
FILE_SIZE=$((FILE_SIZE_GB * 1024 * 1024 * 1024))
DAILY_WRITE_PERCENT=100

# Block sizes
BLOCK_SMALL=$((4 * 1024))
BLOCK_LARGE=$((128 * 1024))

# Calculate target rate
BYTES_PER_DAY=$(echo "$FILE_SIZE * $DAILY_WRITE_PERCENT" | bc)
BYTES_PER_SECOND=$(echo "$BYTES_PER_DAY / 86400" | bc -l)

# How often to check rate (every N writes)
CHECK_INTERVAL=50

log() {
    echo "[$(date '+%F %T')] $1" >> "$LOG"
}

# Prevent duplicates
if [[ -f "$PIDFILE" ]]; then
    pid=$(cat "$PIDFILE")
    if ps -p $pid >/dev/null 2>&1; then
        log "Workload already running with PID $pid"
        exit 0
    else
        rm -f "$PIDFILE"
    fi
fi

create_file() {
    if [[ -f "$DATA_FILE" ]]; then
        size=$(stat -c%s "$DATA_FILE")
        log "Using existing file $DATA_FILE ($((size/1024/1024/1024)) GB)"
        return
    fi
    log "Creating $FILE_SIZE_GB GB file..."
    # Create with actual data, not sparse
    dd if=/dev/zero of="$DATA_FILE" bs=1M count=$((FILE_SIZE_GB * 1024)) status=progress 2>&1 | grep -v records >> "$LOG"
    log "Created workload file"
}

generate_load() {
    local target_rate=$(printf "%.0f" "$BYTES_PER_SECOND")
    log "Starting workload: Target rate ~${target_rate} B/s (~$((target_rate/1024/1024)) MB/s)"
    
    # Pre-generate random data buffers to avoid /dev/urandom bottleneck
    local tmp_small="/tmp/workload_small_$$.dat"
    local tmp_large="/tmp/workload_large_$$.dat"
    dd if=/dev/urandom of="$tmp_small" bs=$BLOCK_SMALL count=1 status=none 2>/dev/null
    dd if=/dev/urandom of="$tmp_large" bs=$BLOCK_LARGE count=1 status=none 2>/dev/null
    
    written=0
    write_count=0
    start=$(date +%s.%N)
    last_log=0
    
    while true; do
        # Choose block size and source
        if (( RANDOM % 2 )); then
            bs=$BLOCK_SMALL
            src="$tmp_small"
        else
            bs=$BLOCK_LARGE
            src="$tmp_large"
        fi
        
        # Random offset within file bounds (must be aligned to block size for dd)
        max_blocks=$((FILE_SIZE / bs - 1))
        seek_blocks=$((RANDOM % max_blocks))
        
        # Write the block using dd with proper seek
        dd if="$src" of="$DATA_FILE" bs=$bs seek=$seek_blocks count=1 conv=notrunc oflag=dsync status=none 2>/dev/null
        
        written=$((written + bs))
        write_count=$((write_count + 1))
        
        # Regenerate random data occasionally to vary the writes
        if (( write_count % 1000 == 0 )); then
            dd if=/dev/urandom of="$tmp_small" bs=$BLOCK_SMALL count=1 status=none 2>/dev/null &
            dd if=/dev/urandom of="$tmp_large" bs=$BLOCK_LARGE count=1 status=none 2>/dev/null &
        fi
        
        # Only check rate periodically to reduce overhead
        if (( write_count % CHECK_INTERVAL == 0 )); then
            now=$(date +%s.%N)
            elapsed=$(echo "$now - $start" | bc -l)
            
            # Avoid division by zero
            if (( $(echo "$elapsed < 0.01" | bc -l) )); then
                continue
            fi
            
            expected=$(echo "$BYTES_PER_SECOND * $elapsed" | bc -l)
            
            if (( $(echo "$written > $expected" | bc -l) )); then
                # We're ahead, sleep to throttle
                sleep_time=$(echo "($written - $expected) / $BYTES_PER_SECOND" | bc -l)
                if (( $(echo "$sleep_time > 0.01" | bc -l) )); then
                    sleep $sleep_time 2>/dev/null
                fi
            fi
            
            # Log progress every 100 MB
            if (( written - last_log >= 100*1024*1024 )); then
                mb=$((written / 1024 / 1024))
                hrs=$(printf "%.2f" "$(echo "$elapsed/3600" | bc -l)")
                actual_rate=$(echo "$written / $elapsed / 1024 / 1024" | bc -l)
                log "Written ${mb} MB over ${hrs}h (rate: $(printf "%.2f" "$actual_rate") MB/s)"
                last_log=$written
            fi
        fi
    done
    
    # Cleanup temp files
    rm -f "$tmp_small" "$tmp_large"
}

# Run workload in background
(
    create_file
    generate_load
) &

echo $! > "$PIDFILE"
log "Started workload with PID $(cat $PIDFILE)"

# Background log rotation
(
    while sleep 3600; do
        if [[ -f "$LOG" ]]; then
            tail -100 "$LOG" > "${LOG}.tmp"
            mv "${LOG}.tmp" "$LOG"
        fi
    done
) &
