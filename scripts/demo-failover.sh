#!/usr/bin/env bash
# End-to-end demo against a real 3-node cluster: write data, kill the leader, keep writing,
# bring the dead node back and watch it catch up. Requires: start-cluster.sh already run.
set -euo pipefail
cd "$(dirname "$0")/.."
# -L follows the 307 redirect a follower sends to point at the leader
c() { curl -s --noproxy '*' -L "$@"; }
port_of() { echo $((8000 + ${1#n})); }
leader_id() {
  for id in n1 n2 n3; do
    s=$(curl -s --noproxy '*' --max-time 1 "http://127.0.0.1:$(port_of $id)/status" 2>/dev/null || true)
    if echo "$s" | grep -q '"role":"LEADER"'; then echo "$id"; return; fi
  done
}
live_port() { for id in n1 n2 n3; do
    if curl -s --noproxy '*' --max-time 1 "http://127.0.0.1:$(port_of $id)/health" >/dev/null 2>&1; then port_of $id; return; fi
  done; }

echo "== 1. write 5 keys (through any node; followers redirect to the leader)"
for i in 1 2 3 4 5; do c -X PUT --data-binary "value-$i" "http://127.0.0.1:8001/kv/key$i" >/dev/null; done
echo "   key3 -> $(c http://127.0.0.1:8002/kv/key3)"

LEADER=$(leader_id)
echo "== 2. current leader: $LEADER. Killing it (kill -9)..."
kill -9 "$(cat .run/$LEADER.pid)"; rm -f ".run/$LEADER.pid"

echo "== 3. waiting for a new leader"
NEW=""
for _ in $(seq 1 60); do NEW=$(leader_id || true); [ -n "$NEW" ] && [ "$NEW" != "$LEADER" ] && break; sleep 0.2; done
echo "   new leader: $NEW"

P=$(live_port)
echo "== 4. cluster keeps serving: old data readable, new writes accepted"
echo "   key3 -> $(c http://127.0.0.1:$P/kv/key3)"
c -X PUT --data-binary "written-during-outage" "http://127.0.0.1:$P/kv/after-failover" >/dev/null
echo "   after-failover -> $(c http://127.0.0.1:$P/kv/after-failover)"

echo "== 5. restarting $LEADER; it recovers from disk and catches up from the new leader"
./scripts/start-cluster.sh "$LEADER" >/dev/null
sleep 1.5
echo "   $LEADER local copy of 'after-failover' -> $(curl -s --noproxy '*' "http://127.0.0.1:$(port_of $LEADER)/kv/after-failover?stale=true")"
echo "   $LEADER status: $(curl -s --noproxy '*' "http://127.0.0.1:$(port_of $LEADER)/status")"
