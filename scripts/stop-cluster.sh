#!/usr/bin/env bash
# Stop nodes started by start-cluster.sh. Usage: ./scripts/stop-cluster.sh [node-id ...]  (default: all)
# Set WIPE=1 to also delete the data directories.
set -euo pipefail
cd "$(dirname "$0")/.."
NODES=("$@"); [ ${#NODES[@]} -eq 0 ] && NODES=(n1 n2 n3)
for id in "${NODES[@]}"; do
  if [ -f ".run/$id.pid" ]; then
    kill "$(cat ".run/$id.pid")" 2>/dev/null && echo "stopped $id" || echo "$id was not running"
    rm -f ".run/$id.pid"
  fi
  [ "${WIPE:-0}" = "1" ] && rm -rf "data/$id"
done
exit 0
