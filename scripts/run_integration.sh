#!/usr/bin/env bash
set -euo pipefail

# This script runs real CLI checks against the docker-compose stack, with verbose logging enabled,
# capturing both stdout and stderr to a timestamped log file under logs/.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/integration-$STAMP.log"

echo "Writing integration run to: $LOG_FILE"

# Ensure config is set (default to provided sample)
export ICECTL_CONFIG="${ICECTL_CONFIG:-$ROOT_DIR/infra/icectl.config.yaml}"
echo "ICECTL_CONFIG=$ICECTL_CONFIG" | tee -a "$LOG_FILE"

# Quick readiness loop: try 'catalogs list' until success or timeout
echo "Waiting for Lakekeeper to be ready..." | tee -a "$LOG_FILE"
ATTEMPTS=30
for i in $(seq 1 $ATTEMPTS); do
  if uv run icectl --verbose --json-output catalogs list >/dev/null 2>>"$LOG_FILE"; then
    echo "Lakekeeper ready after $i attempts" | tee -a "$LOG_FILE"
    break
  fi
  sleep 2
  if [[ $i -eq $ATTEMPTS ]]; then
    echo "Timed out waiting for Lakekeeper; see log: $LOG_FILE" | tee -a "$LOG_FILE"
    exit 1
  fi
done

{
  echo "=== icectl catalogs list (JSON) ==="
  uv run icectl --verbose --json-output catalogs list

  echo
  echo "=== icectl catalogs show (table) ==="
  uv run icectl --verbose catalogs show

  echo
  echo "=== icectl db list (table) ==="
  uv run icectl --verbose db list

  echo
  echo "=== icectl tables list --db analytics (table) ==="
  uv run icectl --verbose tables list --db analytics
} 2>&1 | tee -a "$LOG_FILE"

echo "Integration checks complete. Log: $LOG_FILE"

