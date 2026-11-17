#!/usr/bin/env bash
# Start a local 3-node cluster (3 JVMs on 127.0.0.1). Raft ports 7001-7003, HTTP ports 8001-8003.
# Usage: ./scripts/start-cluster.sh [node-id ...]   (default: n1 n2 n3; pass ids to (re)start only those nodes)
set -euo pipefail
cd "$(dirname "$0")/.."

JAR="${JAR:-target/raft-kv-1.0.0.jar}"
RUN_DIR=".run"
PEERS="n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003"
NODES=("$@"); [ ${#NODES[@]} -eq 0 ] && NODES=(n1 n2 n3)

if [ ! -f "$JAR" ]; then
  echo "error: $JAR not found. Build it first:  mvn -q package -DskipTests" >&2
  exit 1
fi
mkdir -p "$RUN_DIR"

for id in "${NODES[@]}"; do
  if [ -f "$RUN_DIR/$id.pid" ] && kill -0 "$(cat "$RUN_DIR/$id.pid")" 2>/dev/null; then
    echo "$id already running (pid $(cat "$RUN_DIR/$id.pid"))"
    continue
  fi
  nohup java -jar "$JAR" --id "$id" --peers "$PEERS" --data-dir "data/$id" \
      >> "$RUN_DIR/$id.log" 2>&1 &
  echo $! > "$RUN_DIR/$id.pid"
  echo "started $id (pid $!), log: $RUN_DIR/$id.log"
done

echo "waiting for a leader..."
for _ in $(seq 1 50); do
  for port in 8001 8002 8003; do
    if curl -s --noproxy '*' "http://127.0.0.1:$port/status" 2>/dev/null | grep -q '"role":"LEADER"'; then
      echo "leader is up:"; curl -s --noproxy '*' "http://127.0.0.1:$port/status"; exit 0
    fi
  done
  sleep 0.2
done
echo "no leader after 10s; check $RUN_DIR/*.log" >&2
exit 1
