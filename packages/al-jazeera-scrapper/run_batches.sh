#!/bin/bash

set -euo pipefail

# Batch runner for scraper.py
# - Runs the scraper over a large number of sitemaps in fixed-size batches
# - Resumes safely using .progress markers
# - Logs each batch to logs/batches/batch_START_END.log

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs/batches"
PROGRESS_DIR="$SCRIPT_DIR/.progress"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$LOG_DIR" "$PROGRESS_DIR"

# Defaults (override via CLI args or env)
TOTAL_SITEMAPS="${TOTAL_SITEMAPS:-9000}"
BATCH_SIZE="${BATCH_SIZE:-500}"
START_OFFSET="${START_OFFSET:-0}"
MAX_RETRIES="${MAX_RETRIES:-0}"

# CLI: run_batches.sh [total] [batch_size] [start_offset]
if [[ $# -ge 1 ]]; then TOTAL_SITEMAPS="$1"; fi
if [[ $# -ge 2 ]]; then BATCH_SIZE="$2"; fi
if [[ $# -ge 3 ]]; then START_OFFSET="$3"; fi

echo "Running batches"
echo "- Total sitemaps: $TOTAL_SITEMAPS"
echo "- Batch size:     $BATCH_SIZE"
echo "- Start offset:   $START_OFFSET"
echo "- Max retries:    $MAX_RETRIES"
echo "Logs:             $LOG_DIR"
echo "Progress:         $PROGRESS_DIR"

start=$START_OFFSET
end=$TOTAL_SITEMAPS

while [[ $start -lt $end ]]; do
  remaining=$(( end - start ))
  limit=$BATCH_SIZE
  if [[ $remaining -lt $BATCH_SIZE ]]; then
    limit=$remaining
  fi

  batch_end=$(( start + limit - 1 ))
  marker="$PROGRESS_DIR/batch_${start}_${batch_end}.done"
  log_file="$LOG_DIR/batch_${start}_${batch_end}.log"

  if [[ -f "$marker" ]]; then
    echo "[SKIP] Batch $start..$batch_end already completed"
    start=$(( start + limit ))
    continue
  fi

  echo "[RUN ] Batch $start..$batch_end (limit=$limit)"

  attempt=1
  success=0
  while [[ $attempt -le $MAX_RETRIES ]]; do
    echo "  Attempt $attempt/$MAX_RETRIES..."
    set +e
    # scraper.py expects: [limit_sitemaps] [start_sitemap]
    "$PYTHON_BIN" "$SCRIPT_DIR/scraper.py" "$limit" "$start" | tee "$log_file"
    exit_code=${PIPESTATUS[0]}
    set -e

    if [[ $exit_code -eq 0 ]]; then
      success=1
      break
    else
      echo "  Attempt $attempt failed with exit code $exit_code"
      sleep $(( attempt * 5 ))
      attempt=$(( attempt + 1 ))
    fi
  done

  if [[ $success -eq 1 ]]; then
    touch "$marker"
    echo "[DONE] Batch $start..$batch_end"
  else
    echo "[FAIL] Batch $start..$batch_end after $MAX_RETRIES attempts"
    echo "See logs: $log_file"
    exit 1
  fi

  start=$(( start + limit ))
done

echo "All batches complete."


