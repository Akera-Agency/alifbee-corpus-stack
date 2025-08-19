#!/bin/bash

# Parallel batch runner for scraper.py
# - Runs the scraper over a large number of sitemaps in fixed-size batches
# - Launches processes in background for maximum parallelism
# - Stores logs in /tmp/scraper_batch_*

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "SCRIPT_DIR: $SCRIPT_DIR"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Maximum number of concurrent processes
MAX_CONCURRENT="${MAX_CONCURRENT:-40}"

# Function to check status of background processes
check_status() {
  local batch_dir="$1"
  local running=0
  local completed=0
  local failed=0
  
  if [[ ! -d "$batch_dir" ]]; then
    echo "Batch directory not found: $batch_dir"
    return 1
  fi
  
  for exit_file in "$batch_dir"/*.exit; do
    if [[ -f "$exit_file" ]]; then
      exit_code=$(cat "$exit_file")
      if [[ "$exit_code" == "0" ]]; then
        completed=$((completed + 1))
      else
        failed=$((failed + 1))
      fi
    fi
  done
  
  for pid_file in "$batch_dir"/*.pid; do
    if [[ -f "$pid_file" ]]; then
      pid=$(cat "$pid_file")
      if kill -0 "$pid" 2>/dev/null; then
        running=$((running + 1))
      fi
    fi
  done
  
  echo "Status: $running running, $completed completed, $failed failed"
  
  # Return success if all processes are done
  [[ $running -eq 0 ]]
}

# Defaults
TOTAL_SITEMAPS="${TOTAL_SITEMAPS:-9117}"
BATCH_SIZE="${BATCH_SIZE:-500}"
START_OFFSET="${START_OFFSET:-0}"

# Parse command line arguments
if [[ $# -ge 1 ]]; then TOTAL_SITEMAPS="$1"; fi
if [[ $# -ge 2 ]]; then BATCH_SIZE="$2"; fi
if [[ $# -ge 3 ]]; then START_OFFSET="$3"; fi

# Show help if requested
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
  echo "Usage: $0 [TOTAL_SITEMAPS] [BATCH_SIZE] [START_OFFSET]"
  echo ""
  echo "Options:"
  echo "  --status          # Check status of all running processes"
  echo "  --help, -h        # Show this help"
  echo ""
  echo "Examples:"
  echo "  $0                # Process 9000 sitemaps in batches of 500"
  echo "  $0 9000 500 0     # Same as above"
  echo "  $0 9000 500 120   # Start from sitemap 120"
  echo "  $0 --status       # Check status of running processes"
  echo ""
  exit 0
fi

# Check status of all running processes
if [[ "$1" == "--status" ]]; then
  echo "Checking status of all running scraper processes..."
  
  # Find all batch directories
  batch_dirs=$(find /tmp -maxdepth 1 -type d -name "scraper_batch_*_*" 2>/dev/null)
  
  if [[ -z "$batch_dirs" ]]; then
    echo "No running batches found."
    exit 0
  fi
  
  total_running=0
  total_completed=0
  total_failed=0
  
  for dir in $batch_dirs; do
    batch_id=$(basename "$dir" | sed 's/scraper_batch_\([0-9]*\)_.*/\1/')
    echo "Batch #$batch_id:"
    
    # Count processes
    running=0
    completed=0
    failed=0
    
    for exit_file in "$dir"/*.exit; do
      if [[ -f "$exit_file" ]]; then
        exit_code=$(cat "$exit_file")
        if [[ "$exit_code" == "0" ]]; then
          completed=$((completed + 1))
        else
          failed=$((failed + 1))
        fi
      fi
    done
    
    for pid_file in "$dir"/*.pid; do
      if [[ -f "$pid_file" ]]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
          running=$((running + 1))
        fi
      fi
    done
    
    echo "  Running: $running, Completed: $completed, Failed: $failed"
    
    total_running=$((total_running + running))
    total_completed=$((total_completed + completed))
    total_failed=$((total_failed + failed))
  done
  
  echo ""
  echo "Total status:"
  echo "  Running: $total_running"
  echo "  Completed: $total_completed"
  echo "  Failed: $total_failed"
  
  exit 0
fi

echo "=== Al Jazeera Scraper Batch Runner ==="
echo "Total sitemaps: $TOTAL_SITEMAPS"
echo "Batch size:     $BATCH_SIZE"
echo "Starting from:  $START_OFFSET"
echo "======================================="

start=$START_OFFSET
end=$TOTAL_SITEMAPS
batch_num=1
completed_sitemaps=0
start_time=$(date +%s)

while [[ $start -lt $end ]]; do
  # Calculate batch size
  remaining=$(( end - start ))
  limit=$BATCH_SIZE
  if [[ $remaining -lt $BATCH_SIZE ]]; then
    limit=$remaining
  fi

  # Calculate batch end
  batch_end=$(( start + limit - 1 ))
  
  # Display batch header
  echo ""
  echo "============================================================"
  echo "BATCH #$batch_num: Processing sitemaps $start to $batch_end (one by one)"
  echo "============================================================"
  
  # Process each sitemap individually within the batch but in background
  echo ""
  echo "Starting background processes for batch $batch_num..."
  
  # Create a temp directory for this batch's logs and PIDs
  batch_dir="/tmp/scraper_batch_${batch_num}_$$"
  mkdir -p "$batch_dir"
  
  # Launch each scraper in background with throttling
  for sitemap_index in $(seq $start $batch_end); do
    log_file="$batch_dir/sitemap_${sitemap_index}.log"
    pid_file="$batch_dir/sitemap_${sitemap_index}.pid"
    
    # Throttle: Wait until we're below MAX_CONCURRENT processes
    while true; do
      # Count running processes
      running_count=$(pgrep -f "$SCRIPT_DIR/scraper.py" | wc -l)
      
      if [[ $running_count -lt $MAX_CONCURRENT ]]; then
        break
      fi
      
      echo "Throttling: $running_count/$MAX_CONCURRENT processes running, waiting..."
      sleep 2
    done
    
    echo "Launching sitemap #$sitemap_index (batch $batch_num, $(( sitemap_index - start + 1 ))/$limit)"
    
    # Run scraper with limit=1 for a single sitemap in background
    # scraper.py expects: [limit_sitemaps] [start_sitemap]
    (
      "$PYTHON_BIN" "$SCRIPT_DIR/scraper.py" "1" "$sitemap_index" > "$log_file" 2>&1
      echo $? > "${log_file}.exit"
    ) &
    
    # Store PID
    echo $! > "$pid_file"
    
    # Small delay to avoid overwhelming the system
    sleep 0.2
  done
  
  echo "All $limit processes launched for batch $batch_num"
  echo "Waiting for processes to finish (they continue in background)..."
  
  # Update the completed count immediately
  completed_sitemaps=$((completed_sitemaps + limit))
  
  # Move to next batch immediately
  start=$(( start + limit ))
  batch_num=$(( batch_num + 1 ))
  
  # Show progress update
  elapsed_time=$(($(date +%s) - start_time))
  rate=$(echo "scale=2; $completed_sitemaps / ($elapsed_time + 0.01)" | bc)
  remaining_time=$(echo "scale=0; ($TOTAL_SITEMAPS - $START_OFFSET - $completed_sitemaps) / ($rate + 0.0001)" | bc)
  
  echo "Batch started in background. Progress: $completed_sitemaps of $(($TOTAL_SITEMAPS - $START_OFFSET))"
  echo "Rate: $rate sitemaps/sec, Est. remaining: $(($remaining_time / 60)) min $(($remaining_time % 60)) sec"
  
  # Display batch footer
  echo ""
  echo "------------------------------------------------------------"
  echo "LAUNCHED BATCH #$((batch_num-1)): sitemaps $(($start-$limit)) to $(($start-1))"
  echo "------------------------------------------------------------"
done

echo ""
echo "=== All batches complete! ==="
total_elapsed=$(($(date +%s) - start_time))
hours=$((total_elapsed / 3600))
minutes=$(((total_elapsed % 3600) / 60))
seconds=$((total_elapsed % 60))
avg_rate=$(echo "scale=2; $completed_sitemaps / ($total_elapsed + 0.01)" | bc)

echo "Processed $completed_sitemaps sitemaps in $((batch_num-1)) batches"
echo "Total time: ${hours}h ${minutes}m ${seconds}s"
echo "Average rate: $avg_rate sitemaps/second"


